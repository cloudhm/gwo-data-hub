/**
 * 亚马逊 VC 报表服务：增量拉取 SP-API Reports API 中的 Vendor 报表，
 * 按天存储，支持 createReport 后轮询 getReport 或入队延时重试。
 */
import { createRequire } from 'module';
import prisma from '../../config/database.js';
import { getAmazonMarketplace, getAmazonRequestIdFromError } from '../../utils/amazon.js';
import { VC_REPORT_TYPES } from './vcReportTypes.js';

const require = createRequire(import.meta.url);
const { SellingPartner } = require('amazon-sp-api');

const DEBUG_SP_API = process.env.DEBUG_AMAZON_SP_API === 'true' || process.env.DEBUG_AMAZON_SP_API === '1';

/** 轮询 getReport 间隔（毫秒） */
const POLL_INTERVAL_MS = 15 * 1000;
/** 最大轮询时长（毫秒），超时则入队 */
const MAX_POLL_WAIT_MS = 5 * 60 * 1000;
/** 入队后延迟分钟数 */
const QUEUE_RETRY_AFTER_MINUTES = 10;
/** 队列最大重试次数 */
const QUEUE_MAX_ATTEMPTS = 5;
/** 默认回看天数（无 sync state 时） */
const DEFAULT_LOOKBACK_DAYS = 90;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** 获取所有已授权的 VC 店铺 */
async function getVcStores() {
  return prisma.amazonStore.findMany({
    where: { accountType: 'vc', isAuthorized: true, archived: false },
    orderBy: { createdAt: 'asc' }
  });
}

/** 创建 SP-API 客户端（VC 店铺） */
function createSpClient(store) {
  const marketplace = getAmazonMarketplace(store.countryCode);
  const region = marketplace?.region || 'na';
  const clientId = process.env.AMAZON_CLIENT_ID || process.env.SELLING_PARTNER_APP_CLIENT_ID;
  const clientSecret = process.env.AMAZON_CLIENT_SECRET || process.env.SELLING_PARTNER_APP_CLIENT_SECRET;
  if (!store.refreshToken || !clientId || !clientSecret) {
    throw new Error(`VC 店铺 ${store.id} 缺少 refreshToken 或环境变量 AMAZON_CLIENT_ID/AMAZON_CLIENT_SECRET`);
  }
  return new SellingPartner({
    region,
    refresh_token: store.refreshToken,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: clientId,
      SELLING_PARTNER_APP_CLIENT_SECRET: clientSecret
    },
    options: { debug_log: DEBUG_SP_API }
  });
}

/** 当天 0 点 UTC 的 Date（用于 dataDate 唯一键） */
function toDataDate(d) {
  const x = new Date(d);
  return new Date(Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate()));
}

/**
 * 创建报表请求，返回 reportId
 * @param {object} spClient - SellingPartner 实例
 * @param {object} body - { reportType, marketplaceIds, dataStartTime?, dataEndTime?, reportOptions? }
 */
async function createReport(spClient, body) {
  const res = await spClient.callAPI({
    operation: 'reports.createReport',
    body
  });
  return res.reportId;
}

/**
 * 获取报表状态
 * @returns {{ processingStatus, reportDocumentId? }}
 */
async function getReport(spClient, reportId) {
  return spClient.callAPI({
    operation: 'reports.getReport',
    path: { reportId }
  });
}

/**
 * 轮询 getReport 直到终态或超时；超时则入队并返回 null
 * @returns {{ documentId: string } | null} documentId 为 reportDocumentId；null 表示已入队
 */
async function waitForReport(spClient, reportId, options = {}) {
  const {
    pollIntervalMs = POLL_INTERVAL_MS,
    maxWaitMs = MAX_POLL_WAIT_MS,
    enqueueRetryAfterMinutes = QUEUE_RETRY_AFTER_MINUTES,
    amazonStoreId,
    reportType,
    dataDate,
    marketplaceId
  } = options;

  const start = Date.now();
  let attempts = 0;

  while (Date.now() - start < maxWaitMs) {
    attempts++;
    const res = await getReport(spClient, reportId);

    if (res.processingStatus === 'DONE') {
      return { documentId: res.reportDocumentId };
    }
    if (res.processingStatus === 'CANCELLED' || res.processingStatus === 'FATAL') {
      throw new Error(`Report ${reportId} ${res.processingStatus}`);
    }

    if (DEBUG_SP_API) {
      console.log(`[VC Report] ${reportType} reportId=${reportId} status=${res.processingStatus} attempt=${attempts}`);
    }
    await sleep(pollIntervalMs);
  }

  // 超时：入队延时重试
  if (amazonStoreId && reportType && marketplaceId !== undefined) {
    const retryAt = new Date(Date.now() + enqueueRetryAfterMinutes * 60 * 1000);
    await prisma.amazonVcReportPendingQueue.create({
      data: {
        amazonStoreId,
        reportId,
        reportType,
        dataDate: dataDate || null,
        marketplaceId: marketplaceId ?? 'ALL',
        retryAt,
        attempts: 0
      }
    });
    if (DEBUG_SP_API) {
      console.log(`[VC Report] Enqueued reportId=${reportId} retryAt=${retryAt.toISOString()}`);
    }
  }
  return null;
}

/**
 * 获取报表文档详情（url + compressionAlgorithm）
 */
async function getReportDocument(spClient, reportDocumentId) {
  return spClient.callAPI({
    operation: 'reports.getReportDocument',
    path: { reportDocumentId }
  });
}

/**
 * 下载并解析报表（JSON 或 tab/CSV -> 行数组）
 * @returns {Array<object>} 行数组
 */
async function downloadAndParseReport(spClient, documentDetails) {
  const raw = await spClient.download(documentDetails, { json: true, unzip: true });
  if (Array.isArray(raw)) return raw;
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
    const keys = Object.keys(raw);
    if (keys.length === 1 && Array.isArray(raw[keys[0]])) return raw[keys[0]];
    return [raw];
  }
  return [];
}

/**
 * 若行数据中有 marketplaceId 字段，则按店铺 marketplaceId 过滤
 */
function filterRowsByMarketplaceId(rows, marketplaceId) {
  if (!marketplaceId || marketplaceId === 'ALL' || !Array.isArray(rows)) return rows;
  const key = 'marketplaceId';
  const hasKey = rows.some((r) => r && r[key] !== undefined);
  if (!hasKey) return rows;
  return rows.filter((r) => r && r[key] === marketplaceId);
}

/**
 * 保存报表数据（按天 upsert）
 */
async function saveReportData(amazonStoreId, reportType, dataDate, marketplaceId, rows) {
  const dateOnly = toDataDate(dataDate);
  const data = filterRowsByMarketplaceId(rows, marketplaceId === 'ALL' ? null : marketplaceId);
  await prisma.amazonVcReportData.upsert({
    where: {
      amazonStoreId_reportType_dataDate_marketplaceId: {
        amazonStoreId,
        reportType,
        dataDate: dateOnly,
        marketplaceId: marketplaceId || 'ALL'
      }
    },
    create: {
      amazonStoreId,
      reportType,
      dataDate: dateOnly,
      marketplaceId: marketplaceId || 'ALL',
      data: data
    },
    update: { data, updatedAt: new Date() }
  });
  await prisma.amazonVcReportSyncState.upsert({
    where: {
      amazonStoreId_reportType_marketplaceId: {
        amazonStoreId,
        reportType,
        marketplaceId: marketplaceId || 'ALL'
      }
    },
    create: {
      amazonStoreId,
      reportType,
      marketplaceId: marketplaceId || 'ALL',
      lastDataEndAt: dateOnly,
      recordCount: data.length
    },
    update: {
      lastDataEndAt: dateOnly,
      recordCount: data.length
    }
  });
}

/**
 * 获取下次应拉取的日期范围（按天/按 maxSpanDays 分段）
 * @returns {{ startDate: Date, endDate: Date }[]}
 */
async function getNextDateRangesForReport(amazonStoreId, reportType, marketplaceId, config, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS } = options;
  const state = await prisma.amazonVcReportSyncState.findUnique({
    where: {
      amazonStoreId_reportType_marketplaceId: { amazonStoreId, reportType, marketplaceId: marketplaceId || 'ALL' }
    }
  });

  const now = new Date();
  const end = endDate ? new Date(endDate) : new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
  let start;
  if (state?.lastDataEndAt) {
    const last = new Date(state.lastDataEndAt);
    start = new Date(last.getFullYear(), last.getMonth(), last.getDate() + 1);
  } else {
    const lookback = config.lookbackDays ?? defaultLookbackDays;
    start = new Date(end.getTime() - lookback * 24 * 60 * 60 * 1000);
  }
  start.setHours(0, 0, 0, 0);
  end.setHours(23, 59, 59, 999);

  if (start > end) return [];

  const ranges = [];
  const cur = new Date(start);
  while (cur <= end) {
    const dayStart = new Date(cur);
    dayStart.setHours(0, 0, 0, 0);
    const dayEnd = new Date(cur);
    dayEnd.setHours(23, 59, 59, 999);
    ranges.push({ startDate: dayStart, endDate: dayEnd });
    cur.setDate(cur.getDate() + 1);
  }
  return ranges;
}

/**
 * 单个报表类型、单个店铺的增量同步（按天）
 */
async function incrementalSyncReportForStore(store, reportTypeConfig, options = {}) {
  const { reportType, supportsDateRange, reportOptions } = reportTypeConfig;
  const marketplace = getAmazonMarketplace(store.countryCode);
  const marketplaceId = marketplace?.marketplaceId || 'ALL';

  const spClient = createSpClient(store);

  if (!supportsDateRange) {
    const reportId = await createReport(spClient, {
      reportType,
      marketplaceIds: [marketplaceId],
      reportOptions: reportOptions || undefined
    });
    const result = await waitForReport(spClient, reportId, {
      ...options,
      amazonStoreId: store.id,
      reportType,
      dataDate: null,
      marketplaceId
    });
    if (!result) return { reportId, enqueued: true };
    const doc = await getReportDocument(spClient, result.documentId);
    const rows = await downloadAndParseReport(spClient, doc);
    await saveReportData(store.id, reportType, new Date(), marketplaceId, rows);
    return { reportId, rows: rows.length };
  }

  const ranges = await getNextDateRangesForReport(
    store.id,
    reportType,
    marketplaceId,
    reportTypeConfig,
    options
  );
  const results = [];
  for (const { startDate, endDate } of ranges) {
    const reportId = await createReport(spClient, {
      reportType,
      marketplaceIds: [marketplaceId],
      dataStartTime: startDate.toISOString(),
      dataEndTime: endDate.toISOString(),
      reportOptions: reportOptions || undefined
    });
    const dataDate = toDataDate(startDate);
    const result = await waitForReport(spClient, reportId, {
      ...options,
      amazonStoreId: store.id,
      reportType,
      dataDate,
      marketplaceId
    });
    if (!result) {
      results.push({ reportId, enqueued: true });
      continue;
    }
    const doc = await getReportDocument(spClient, result.documentId);
    const rows = await downloadAndParseReport(spClient, doc);
    await saveReportData(store.id, reportType, dataDate, marketplaceId, rows);
    results.push({ reportId, rows: rows.length });
    await sleep(500);
  }
  return results;
}

/**
 * 单个报表类型、所有 VC 店铺的按天增量同步
 * @param {string} reportType - 报表类型，如 'GET_VENDOR_SALES_REPORT'
 * @param {object} options - { endDate?, defaultLookbackDays?, logErrors? }
 * @returns {Promise<Array<{ storeId, result?: any, error?: string }>>}
 */
async function incrementalSyncReportTypeForAllStores(reportType, options = {}) {
  const config = VC_REPORT_TYPES.find((c) => c.reportType === reportType);
  if (!config) {
    throw new Error(`未知的 VC 报表类型: ${reportType}，支持: ${VC_REPORT_TYPES.map((c) => c.reportType).join(', ')}`);
  }
  const stores = await getVcStores();
  const results = [];
  for (const store of stores) {
    try {
      const r = await incrementalSyncReportForStore(store, config, options);
      results.push({ storeId: store.id, reportType, result: r });
    } catch (e) {
      const apiRequestId = getAmazonRequestIdFromError(e);
      if (options.logErrors !== false) {
        const ts = new Date().toISOString();
        const responseDate = e?.response?.headers?.date;
        console.error(
          `[VC Report] ${store.id} ${reportType}`,
          e?.message || e,
          apiRequestId ? `apiRequestId=${apiRequestId}` : '',
          `timestamp=${ts}`,
          responseDate ? `responseDate=${responseDate}` : ''
        );
      }
      results.push({ storeId: store.id, reportType, error: e?.message || String(e), ...(apiRequestId && { amazonRequestId: apiRequestId }) });
    }
    await sleep(300);
  }
  return results;
}

/**
 * 全量 VC 报表增量同步：所有 VC 店铺、所有配置的报表类型
 */
async function incrementalSyncAllVcReports(options = {}) {
  const stores = await getVcStores();
  const results = [];
  for (const store of stores) {
    for (const config of VC_REPORT_TYPES) {
      try {
        const r = await incrementalSyncReportForStore(store, config, options);
        results.push({ storeId: store.id, reportType: config.reportType, result: r });
      } catch (e) {
        const apiRequestId = getAmazonRequestIdFromError(e);
        if (options.logErrors !== false) {
          const ts = new Date().toISOString();
          const responseDate = e?.response?.headers?.date;
          console.error(
            `[VC Report] ${store.id} ${config.reportType}`,
            e?.message || e,
            apiRequestId ? `apiRequestId=${apiRequestId}` : '',
            `timestamp=${ts}`,
            responseDate ? `responseDate=${responseDate}` : ''
          );
        }
        results.push({ storeId: store.id, reportType: config.reportType, error: e?.message || String(e), ...(apiRequestId && { amazonRequestId: apiRequestId }) });
      }
      await sleep(300);
    }
  }
  return results;
}

/**
 * 处理待处理队列：对 retryAt <= now 的项执行 getReport，DONE 则下载保存并删除队列项；否则更新 retryAt 与 attempts
 */
async function processReportPendingQueue(options = {}) {
  const { maxItems = 50 } = options;
  const items = await prisma.amazonVcReportPendingQueue.findMany({
    where: { retryAt: { lte: new Date() } },
    take: maxItems,
    orderBy: { retryAt: 'asc' },
    include: { amazonStore: true }
  });
  const results = [];
  for (const item of items) {
    try {
      const store = item.amazonStore;
      const spClient = createSpClient(store);
      const res = await getReport(spClient, item.reportId);

      if (res.processingStatus === 'DONE') {
        const doc = await getReportDocument(spClient, res.reportDocumentId);
        const rows = await downloadAndParseReport(spClient, doc);
        const dataDate = item.dataDate ? new Date(item.dataDate) : new Date();
        await saveReportData(store.id, item.reportType, dataDate, item.marketplaceId, rows);
        await prisma.amazonVcReportPendingQueue.delete({ where: { id: item.id } });
        results.push({ id: item.id, reportId: item.reportId, status: 'saved', rows: rows.length });
        continue;
      }
      if (res.processingStatus === 'CANCELLED' || res.processingStatus === 'FATAL') {
        await prisma.amazonVcReportPendingQueue.delete({ where: { id: item.id } });
        results.push({ id: item.id, reportId: item.reportId, status: res.processingStatus });
        continue;
      }

      const attempts = item.attempts + 1;
      const retryAt = new Date(Date.now() + QUEUE_RETRY_AFTER_MINUTES * 60 * 1000);
      if (attempts >= QUEUE_MAX_ATTEMPTS) {
        await prisma.amazonVcReportPendingQueue.delete({ where: { id: item.id } });
        results.push({ id: item.id, reportId: item.reportId, status: 'max_attempts_exceeded' });
      } else {
        await prisma.amazonVcReportPendingQueue.update({
          where: { id: item.id },
          data: { retryAt, attempts }
        });
        results.push({ id: item.id, reportId: item.reportId, status: 'requeued', attempts });
      }
    } catch (e) {
      const attempts = item.attempts + 1;
      const retryAt = new Date(Date.now() + QUEUE_RETRY_AFTER_MINUTES * 60 * 1000);
      if (attempts >= QUEUE_MAX_ATTEMPTS) {
        await prisma.amazonVcReportPendingQueue.delete({ where: { id: item.id } }).catch(() => {});
        results.push({ id: item.id, reportId: item.reportId, status: 'error_deleted', error: e?.message });
      } else {
        await prisma.amazonVcReportPendingQueue
          .update({ where: { id: item.id }, data: { retryAt, attempts } })
          .catch(() => {});
        results.push({ id: item.id, reportId: item.reportId, status: 'error_requeued', error: e?.message });
      }
      if (options.logErrors !== false) {
        const apiRequestId = getAmazonRequestIdFromError(e);
        const ts = new Date().toISOString();
        const responseDate = e?.response?.headers?.date;
        console.error(
          `[VC Report Queue] ${item.reportId}`,
          e?.message || e,
          apiRequestId ? `apiRequestId=${apiRequestId}` : '',
          `timestamp=${ts}`,
          responseDate ? `responseDate=${responseDate}` : ''
        );
      }
    }
    await sleep(500);
  }
  return results;
}

export default {
  VC_REPORT_TYPES,
  getVcStores,
  createReport,
  getReport,
  waitForReport,
  getReportDocument,
  downloadAndParseReport,
  saveReportData,
  getNextDateRangesForReport,
  incrementalSyncReportForStore,
  incrementalSyncReportTypeForAllStores,
  incrementalSyncAllVcReports,
  processReportPendingQueue
};
