/**
 * 亚马逊 VC（Vendor Central）PO/DF 服务
 * 独立于领星，直接调用 Amazon SP-API：
 * - Vendor Orders API：PO 单
 * - Vendor Direct Fulfillment Orders API：DF 单
 */
import { createRequire } from 'module';
import prisma from '../../config/database.js';
import { getAmazonMarketplace, generateAmazonRequestId, getAmazonRequestIdFromError } from '../../utils/amazon.js';

const require = createRequire(import.meta.url);
const { SellingPartner } = require('amazon-sp-api');

const MAX_DAYS_PER_REQUEST = 7; // API 限制单次查询不超过 7 天
const DEFAULT_LOOKBACK_DAYS = 365;
/** 每次 getPurchaseOrders 调用后等待（毫秒），避免超过 10 req/s 限流 */
const RESTORE_DELAY_MS = 150;
/** 限流重试：最大次数、初始等待 ms */
const THROTTLE_RETRY_MAX = 5;
const THROTTLE_RETRY_INITIAL_MS = 1000;
/** 入队后延迟分钟数、队列内最大重试次数 */
const QUEUE_RETRY_AFTER_MINUTES = 10;
const QUEUE_MAX_ATTEMPTS = 3;

/** 是否打印 SP-API 请求/返回（可通过环境变量 DEBUG_AMAZON_SP_API=true 开启） */
const DEBUG_SP_API = process.env.DEBUG_AMAZON_SP_API === 'true' || process.env.DEBUG_AMAZON_SP_API === '1';

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * 判断是否为限流/配额类错误（需重试）
 */
function isThrottleError(e) {
  if (!e) return false;
  const code = (e.code || '').toString();
  const msg = (e.message || '').toString().toLowerCase();
  return (
    code === 'QuotaExceeded' ||
    code === '429' ||
    msg.includes('throttl') ||
    msg.includes('rate limit') ||
    msg.includes('429') ||
    msg.includes('too many requests')
  );
}

/**
 * 获取所有已授权的 VC 店铺（accountType=vc）
 */
async function getVcStores() {
  return prisma.amazonStore.findMany({
    where: { accountType: 'vc', isAuthorized: true, archived: false },
    orderBy: { createdAt: 'asc' }
  });
}

/**
 * 根据 refresh_token 创建 SP-API 客户端（仅用于 VC 店铺）
 */
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

/**
 * 获取本次应拉取的日期范围
 */
async function getNextDateRange(amazonStoreId, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS } = options;
  const state = await prisma.amazonVcSyncState.findUnique({
    where: { amazonStoreId_syncType: { amazonStoreId, syncType: 'po' } }
  });
  const now = new Date();
  const end = endDate ? new Date(endDate) : new Date(now.getTime() - 24 * 60 * 60 * 1000);
  let start;
  if (state?.lastEndAt) {
    start = new Date(Number(state.lastEndAt));
  } else {
    start = new Date(end.getTime() - defaultLookbackDays * 24 * 60 * 60 * 1000);
  }
  return { start, end, state };
}

/** DF：获取本次应拉取的日期范围 */
async function getNextDateRangeDf(amazonStoreId, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS } = options;
  const state = await prisma.amazonVcSyncState.findUnique({
    where: { amazonStoreId_syncType: { amazonStoreId, syncType: 'df' } }
  });
  const now = new Date();
  const end = endDate ? new Date(endDate) : new Date(now.getTime() - 24 * 60 * 60 * 1000);
  let start;
  if (state?.lastEndAt) {
    start = new Date(Number(state.lastEndAt));
  } else {
    start = new Date(end.getTime() - defaultLookbackDays * 24 * 60 * 60 * 1000);
  }
  return { start, end, state };
}

/** Shipments：获取本次应拉取的日期范围（默认最近 60 天） */
const DEFAULT_LOOKBACK_DAYS_SHIPMENTS = 60;
async function getNextDateRangeShipments(amazonStoreId, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS_SHIPMENTS } = options;
  const state = await prisma.amazonVcSyncState.findUnique({
    where: { amazonStoreId_syncType: { amazonStoreId, syncType: 'shipments' } }
  });
  const now = new Date();
  const end = endDate ? new Date(endDate) : new Date(now.getTime() - 24 * 60 * 60 * 1000);
  let start;
  if (state?.lastEndAt) {
    start = new Date(Number(state.lastEndAt));
  } else {
    start = new Date(end.getTime() - defaultLookbackDays * 24 * 60 * 60 * 1000);
  }
  return { start, end, state };
}

/** 单次调用 getPurchaseOrders（成功时 SDK 返回 payload 对象，失败抛异常）
 * 限流（429/QuotaExceeded）时按指数退避重试，最多 THROTTLE_RETRY_MAX 次
 */
async function fetchPurchaseOrdersPage(spClient, query) {
  const requestId = generateAmazonRequestId();
  const q = {
    createdAfter: query.createdAfter,
    createdBefore: query.createdBefore,
    includeDetails: 'true',
    limit: Math.min(query.limit || 100, 100)
  };
  if (query.nextToken) q.nextToken = query.nextToken;

  if (DEBUG_SP_API) {
    console.log(`[SP-API] [${requestId}] 请求 getPurchaseOrders`, {
      operation: 'getPurchaseOrders',
      endpoint: 'vendorOrders',
      query: { ...q }
    });
  }

  let lastError;
  for (let attempt = 0; attempt <= THROTTLE_RETRY_MAX; attempt++) {
    try {
      const res = await spClient.callAPI({
        operation: 'getPurchaseOrders',
        endpoint: 'vendorOrders',
        query: q
      });
      if (DEBUG_SP_API) {
        const orders = res?.purchaseOrders || [];
        console.log(`[SP-API] [${requestId}] 返回 getPurchaseOrders`, {
          purchaseOrdersCount: orders.length,
          nextToken: res?.pagination?.nextToken ?? null,
          payloadKeys: Object.keys(res || {})
        });
        if (orders.length > 0 && orders.length <= 3) {
          console.log(`[SP-API] [${requestId}] 返回 body 摘要 (purchaseOrders):`, JSON.stringify(orders, null, 2));
        } else if (orders.length > 3) {
          console.log(`[SP-API] [${requestId}] 返回 body 摘要 (前 2 条):`, JSON.stringify(orders.slice(0, 2), null, 2));
        }
      }
      return {
        purchaseOrders: res?.purchaseOrders || [],
        nextToken: res?.pagination?.nextToken || null
      };
    } catch (e) {
      lastError = e;
      if (attempt < THROTTLE_RETRY_MAX && isThrottleError(e)) {
        const waitMs = THROTTLE_RETRY_INITIAL_MS * Math.pow(2, attempt);
        console.warn(
          `[VC PO] [${requestId}] 限流重试 ${attempt + 1}/${THROTTLE_RETRY_MAX}，${waitMs}ms 后重试:`,
          e?.message || e?.code
        );
        await sleep(waitMs);
      } else {
        console.error(`[VC PO] [${requestId}] getPurchaseOrders API 请求失败:`, e?.code ?? e?.name ?? 'Error', e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
        throw e;
      }
    }
  }
  console.error(`[VC PO] [${requestId}] getPurchaseOrders 重试用尽仍失败:`, lastError?.code, lastError?.message, 'apiRequestId=', getAmazonRequestIdFromError(lastError));
  throw lastError;
}

/** DF：单次调用 getOrders（Vendor Direct Fulfillment Orders API），限流重试同 PO */
async function fetchDfOrdersPage(spClient, query) {
  const requestId = generateAmazonRequestId();
  const q = {
    createdAfter: query.createdAfter,
    createdBefore: query.createdBefore,
    includeDetails: 'true'
  };
  const token = query.nextToken ?? query.paginationToken;
  if (token) {
    q.nextToken = token;
    q.paginationToken = token;
  }

  if (DEBUG_SP_API) {
    console.log(`[SP-API] [${requestId}] 请求 getOrders (DF)`, { endpoint: 'vendorDirectFulfillmentOrders', query: { ...q } });
  }

  let lastError;
  for (let attempt = 0; attempt <= THROTTLE_RETRY_MAX; attempt++) {
    try {
      const res = await spClient.callAPI({
        operation: 'getOrders',
        endpoint: 'vendorDirectFulfillmentOrders',
        query: q,
        options: { version: '2021-12-28' }
      });
      const orders = res?.orders || res?.payload?.orders || [];
      const nextToken = res?.pagination?.nextToken ?? res?.nextToken ?? null;
      if (DEBUG_SP_API) {
        console.log(`[SP-API] [${requestId}] 返回 getOrders (DF)`, { ordersCount: orders.length, nextToken });
      }
      return { orders, nextToken };
    } catch (e) {
      lastError = e;
      if (attempt < THROTTLE_RETRY_MAX && isThrottleError(e)) {
        const waitMs = THROTTLE_RETRY_INITIAL_MS * Math.pow(2, attempt);
        console.warn(`[VC DF] [${requestId}] 限流重试 ${attempt + 1}/${THROTTLE_RETRY_MAX}，${waitMs}ms 后重试:`, e?.message || e?.code, 'apiRequestId=', getAmazonRequestIdFromError(e));
        await sleep(waitMs);
      } else {
        console.error(`[VC DF] [${requestId}] getOrders API 请求失败:`, e?.code ?? e?.name ?? 'Error', e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
        throw e;
      }
    }
  }
  console.error(`[VC DF] [${requestId}] getOrders 重试用尽仍失败:`, lastError?.code, lastError?.message, 'apiRequestId=', getAmazonRequestIdFromError(lastError));
  throw lastError;
}

/** Shipments：单次调用 GetShipmentDetails（Vendor Shipments API v1），限流重试同 PO */
async function fetchShipmentDetailsPage(spClient, query) {
  const requestId = generateAmazonRequestId();
  const q = {
    shipAfter: query.shipAfter,
    shipBefore: query.shipBefore,
    limit: Math.min(query.limit || 100, 100)
  };
  if (query.nextToken) q.nextToken = query.nextToken;

  if (DEBUG_SP_API) {
    console.log(`[SP-API] [${requestId}] 请求 GetShipmentDetails`, { endpoint: 'vendorShipments', query: { ...q } });
  }

  let lastError;
  for (let attempt = 0; attempt <= THROTTLE_RETRY_MAX; attempt++) {
    try {
      const res = await spClient.callAPI({
        operation: 'GetShipmentDetails',
        endpoint: 'vendorShipments',
        query: q
      });
      const shipments = res?.shipments || res?.payload?.shipments || [];
      const nextToken = res?.pagination?.nextToken ?? res?.nextToken ?? null;
      if (DEBUG_SP_API) {
        console.log(`[SP-API] [${requestId}] 返回 GetShipmentDetails`, { shipmentsCount: shipments.length, nextToken });
      }
      return { shipments, nextToken };
    } catch (e) {
      lastError = e;
      if (attempt < THROTTLE_RETRY_MAX && isThrottleError(e)) {
        const waitMs = THROTTLE_RETRY_INITIAL_MS * Math.pow(2, attempt);
        console.warn(`[VC Shipments] [${requestId}] 限流重试 ${attempt + 1}/${THROTTLE_RETRY_MAX}，${waitMs}ms 后重试:`, e?.message || e?.code, 'apiRequestId=', getAmazonRequestIdFromError(e));
        await sleep(waitMs);
      } else {
        console.error(`[VC Shipments] [${requestId}] GetShipmentDetails API 请求失败:`, e?.code ?? e?.name ?? 'Error', e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
        throw e;
      }
    }
  }
  console.error(`[VC Shipments] [${requestId}] GetShipmentDetails 重试用尽仍失败:`, lastError?.code, lastError?.message, 'apiRequestId=', getAmazonRequestIdFromError(lastError));
  throw lastError;
}

/** Shipments：保存一条货件明细 */
async function saveShipmentDetail(amazonStoreId, payload) {
  const arn = payload?.amazonReferenceNumber ?? payload?.amazonReferenceId ?? payload?.shipmentId ?? payload?.referenceNumber;
  const ref = arn != null ? String(arn) : `ship-${payload?.shipmentId ?? Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const shipmentId = payload?.shipmentId ?? null;

  return prisma.amazonVcShipmentDetail.upsert({
    where: { amazonStoreId_amazonReferenceNumber: { amazonStoreId, amazonReferenceNumber: ref } },
    update: { shipmentId, data: payload, updatedAt: new Date() },
    create: { amazonStoreId, amazonReferenceNumber: ref, shipmentId, data: payload }
  });
}

/**
async function enqueueSegmentForRetry(amazonStoreId, createdAfter, createdBefore) {
  const retryAt = new Date(Date.now() + QUEUE_RETRY_AFTER_MINUTES * 60 * 1000);
  await prisma.amazonVcPoSyncRetryQueue.create({
    data: { amazonStoreId, createdAfter, createdBefore, retryAt }
  });
  console.warn(
    `[VC PO] [${generateAmazonRequestId()}] 时段已入队，${QUEUE_RETRY_AFTER_MINUTES} 分钟后重试: store=${amazonStoreId} ${createdAfter}~${createdBefore}`
  );
}

/**
 * 处理队列中已到点的任务（retryAt <= now）
 * 成功则更新 sync state 并删除队列项；再次限流则延后 10 分钟、attempts+1，超过 QUEUE_MAX_ATTEMPTS 则删除
 */
async function processRetryQueue() {
  const now = new Date();
  const items = await prisma.amazonVcPoSyncRetryQueue.findMany({
    where: { retryAt: { lte: now } },
    include: { amazonStore: true },
    orderBy: { retryAt: 'asc' }
  });
  if (items.length === 0) return { processed: 0, success: 0, failed: 0, deferred: 0 };

  let success = 0;
  let failed = 0;
  let deferred = 0;
  for (const item of items) {
    const store = item.amazonStore;
    if (!store || !store.refreshToken) {
      await prisma.amazonVcPoSyncRetryQueue.delete({ where: { id: item.id } });
      failed++;
      continue;
    }
    try {
      const spClient = createSpClient(store);
      let totalRecords = 0;
      let nextToken = null;
      do {
        const q = {
          createdAfter: item.createdAfter,
          createdBefore: item.createdBefore,
          includeDetails: 'true',
          limit: 100
        };
        if (nextToken) q.nextToken = nextToken;
        const page = await fetchPurchaseOrdersPage(spClient, q);
        for (const po of page.purchaseOrders) {
          await savePurchaseOrderWithItems(store.id, po);
          totalRecords++;
        }
        nextToken = page.nextToken;
        if (nextToken) await sleep(200);
        else await sleep(RESTORE_DELAY_MS);
      } while (nextToken);

      const segmentEnd = new Date(item.createdBefore);
      const state = await prisma.amazonVcSyncState.findUnique({
        where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'po' } }
      });
      const shouldUpdate =
        !state || Number(state.lastEndAt) < segmentEnd.getTime();
      if (shouldUpdate) {
        await prisma.amazonVcSyncState.upsert({
          where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'po' } },
          update: { lastEndAt: segmentEnd.getTime(), lastSyncAt: new Date(), recordCount: totalRecords },
          create: { amazonStoreId: store.id, syncType: 'po', lastEndAt: segmentEnd.getTime(), recordCount: totalRecords }
        });
      }
      await prisma.amazonVcPoSyncRetryQueue.delete({ where: { id: item.id } });
      success++;
      console.log(`[VC PO] [${generateAmazonRequestId()}] 队列任务成功: store=${store.id} ${item.createdAfter}~${item.createdBefore} 拉取 ${totalRecords} 条`);
    } catch (e) {
      if (isThrottleError(e) && item.attempts + 1 < QUEUE_MAX_ATTEMPTS) {
        const nextRetryAt = new Date(Date.now() + QUEUE_RETRY_AFTER_MINUTES * 60 * 1000);
        await prisma.amazonVcPoSyncRetryQueue.update({
          where: { id: item.id },
          data: { retryAt: nextRetryAt, attempts: item.attempts + 1 }
        });
        deferred++;
        console.warn(`[VC PO] [${generateAmazonRequestId()}] 队列任务再次限流，${QUEUE_RETRY_AFTER_MINUTES} 分钟后重试 (attempts=${item.attempts + 1}):`, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e));
      } else {
        await prisma.amazonVcPoSyncRetryQueue.delete({ where: { id: item.id } });
        failed++;
        console.error(`[VC PO] [${generateAmazonRequestId()}] 队列任务失败或超过最大重试次数，已移除:`, item.id, e?.code ?? e?.name, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
      }
    }
  }
  return { processed: items.length, success, failed, deferred };
}

/**
 * 保存一条 PO 及其明细
 */
async function savePurchaseOrderWithItems(amazonStoreId, poPayload) {
  const purchaseOrderNumber = poPayload.purchaseOrderNumber;
  if (!purchaseOrderNumber) return null;

  const orderDate = poPayload.purchaseOrderDate ? new Date(poPayload.purchaseOrderDate) : null;
  const sellingPartyPartyId = poPayload.sellingParty?.partyId ?? null;
  const shipToPartyPartyId = poPayload.shipToParty?.partyId ?? null;
  const orderState = poPayload.orderState ?? null;

  const order = await prisma.amazonVcPurchaseOrder.upsert({
    where: {
      amazonStoreId_purchaseOrderNumber: { amazonStoreId, purchaseOrderNumber }
    },
    update: {
      orderDate,
      orderState,
      sellingPartyPartyId,
      shipToPartyPartyId,
      data: poPayload,
      updatedAt: new Date()
    },
    create: {
      amazonStoreId,
      purchaseOrderNumber,
      orderDate,
      orderState,
      sellingPartyPartyId,
      shipToPartyPartyId,
      data: poPayload
    }
  });

  await prisma.amazonVcPurchaseOrderItem.deleteMany({ where: { purchaseOrderId: order.id } });
  const items = poPayload.items || [];
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const lineNumber = it.itemSequenceNumber ?? it.lineNumber ?? String(i + 1);
    const buyerProductId = it.buyerProductIdentifier ?? null;
    const vendorProductId = it.vendorProductIdentifier ?? null;
    const quantity = it.orderedQuantity?.orderedQuantity?.amount ?? it.orderedQuantity ?? null;
    let unitPrice = null;
    if (it.netCost?.amount !== undefined) unitPrice = it.netCost.amount;
    else if (it.listPrice?.amount !== undefined) unitPrice = it.listPrice.amount;

    await prisma.amazonVcPurchaseOrderItem.create({
      data: {
        purchaseOrderId: order.id,
        amazonStoreId,
        lineNumber: String(lineNumber),
        buyerProductId,
        vendorProductId,
        quantity: quantity != null ? Number(quantity) : null,
        unitPrice,
        data: it
      }
    });
  }
  return order;
}

/** DF：将限流失败的时段入队 */
async function enqueueDfSegmentForRetry(amazonStoreId, createdAfter, createdBefore) {
  const retryAt = new Date(Date.now() + QUEUE_RETRY_AFTER_MINUTES * 60 * 1000);
  await prisma.amazonVcDfSyncRetryQueue.create({
    data: { amazonStoreId, createdAfter, createdBefore, retryAt }
  });
  console.warn(`[VC DF] [${generateAmazonRequestId()}] 时段已入队，${QUEUE_RETRY_AFTER_MINUTES} 分钟后重试: store=${amazonStoreId} ${createdAfter}~${createdBefore}`);
}

/** DF：保存一条订单及其明细（结构兼容 API 返回） */
async function saveDfOrderWithItems(amazonStoreId, orderPayload) {
  const purchaseOrderNumber = orderPayload.purchaseOrderNumber ?? orderPayload.purchaseOrderId ?? null;
  if (!purchaseOrderNumber) return null;

  const orderDate = orderPayload.orderDate ? new Date(orderPayload.orderDate) : null;
  const orderStatus = orderPayload.orderStatus ?? orderPayload.orderStatus ?? null;

  const order = await prisma.amazonVcDfOrder.upsert({
    where: {
      amazonStoreId_purchaseOrderNumber: { amazonStoreId, purchaseOrderNumber }
    },
    update: { orderDate, orderStatus, data: orderPayload, updatedAt: new Date() },
    create: { amazonStoreId, purchaseOrderNumber, orderDate, orderStatus, data: orderPayload }
  });

  await prisma.amazonVcDfOrderItem.deleteMany({ where: { orderId: order.id } });
  const items = orderPayload.items || orderPayload.orderItems || [];
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const lineNumber = it.itemSequenceNumber ?? it.lineNumber ?? String(i + 1);
    const buyerProductId = it.buyerProductIdentifier ?? it.buyerProductId ?? null;
    const vendorProductId = it.vendorProductIdentifier ?? it.vendorProductId ?? null;
    const quantity = it.orderedQuantity?.quantity ?? it.orderedQuantity ?? it.quantity ?? null;
    await prisma.amazonVcDfOrderItem.create({
      data: {
        orderId: order.id,
        amazonStoreId,
        lineNumber: String(lineNumber),
        buyerProductId,
        vendorProductId,
        quantity: quantity != null ? Math.round(Number(quantity)) : null,
        data: it
      }
    });
  }
  return order;
}

/**
 * 对单个 VC 店铺做增量拉取
 * 若某时段 5 次重试后仍限流，则入队 10 分钟后重试，本店当次同步结束并返回 queuedSegments
 */
async function incrementalSyncVcPoForStore(store, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS } = options;
  const { start, end } = await getNextDateRange(store.id, { endDate, defaultLookbackDays });
  if (start >= end) {
    return { storeId: store.id, success: true, recordCount: 0, message: '无新时间范围需拉取' };
  }

  const spClient = createSpClient(store);
  let totalRecords = 0;
  let queuedSegments = 0;
  let currentStart = new Date(start);
  const endTime = end.getTime();

  while (currentStart.getTime() < endTime) {
    const segmentEnd = new Date(Math.min(currentStart.getTime() + MAX_DAYS_PER_REQUEST * 24 * 60 * 60 * 1000, endTime));
    const createdAfter = currentStart.toISOString();
    const createdBefore = segmentEnd.toISOString();

    let nextToken = null;
    let segmentDone = false;
    try {
      do {
        const q = { createdAfter, createdBefore, includeDetails: 'true', limit: 100 };
        if (nextToken) q.nextToken = nextToken;
        const page = await fetchPurchaseOrdersPage(spClient, q);
        for (const po of page.purchaseOrders) {
          await savePurchaseOrderWithItems(store.id, po);
          totalRecords++;
        }
        nextToken = page.nextToken;
        if (nextToken) await sleep(200);
        else await sleep(RESTORE_DELAY_MS);
      } while (nextToken);
      segmentDone = true;
    } catch (e) {
      if (isThrottleError(e)) {
        await enqueueSegmentForRetry(store.id, createdAfter, createdBefore);
        queuedSegments++;
        return {
          storeId: store.id,
          success: true,
          recordCount: totalRecords,
          queuedSegments,
          message: `部分时段因限流已入队，约 ${QUEUE_RETRY_AFTER_MINUTES} 分钟后重试`
        };
      }
      console.error(`[VC PO] [${generateAmazonRequestId()}] 店铺增量同步失败 storeId=%s:`, store.id, e?.code ?? e?.name, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
      throw e;
    }

    if (segmentDone) {
      await prisma.amazonVcSyncState.upsert({
        where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'po' } },
        update: { lastEndAt: segmentEnd.getTime(), lastSyncAt: new Date(), recordCount: totalRecords },
        create: { amazonStoreId: store.id, syncType: 'po', lastEndAt: segmentEnd.getTime(), recordCount: totalRecords }
      });
    }

    currentStart = new Date(segmentEnd.getTime() + 1);
    if (currentStart.getTime() < endTime) await sleep(500);
  }

  return { storeId: store.id, success: true, recordCount: totalRecords, queuedSegments };
}

/**
 * 对所有 VC 店铺增量拉取 PO
 * 每次先处理队列中已到点的限流重试任务，再按店铺做正常增量
 */
async function incrementalSyncAllVcPo(options = {}) {
  const queueResult = await processRetryQueue();

  const stores = await getVcStores();
  if (stores.length === 0) {
    return {
      success: true,
      storeCount: 0,
      results: [],
      queue: queueResult,
      message: queueResult.processed > 0 ? `已处理 ${queueResult.processed} 个队列任务，无 VC 店铺` : '没有已授权的 VC 店铺'
    };
  }
  const results = [];
  for (const store of stores) {
    try {
      const r = await incrementalSyncVcPoForStore(store, options);
      results.push(r);
    } catch (e) {
      console.error(`[VC PO] [${generateAmazonRequestId()}] 店铺同步异常 storeId=%s:`, store.id, e?.message, e?.stack, 'apiRequestId=', getAmazonRequestIdFromError(e));
      results.push({ storeId: store.id, success: false, error: e?.message || String(e) });
    }
  }
  const successCount = results.filter((x) => x.success).length;
  const totalRecords = results.reduce((s, x) => s + (x.recordCount ?? 0), 0);
  const queuedSegments = results.reduce((s, x) => s + (x.queuedSegments ?? 0), 0);
  return {
    success: successCount === stores.length && queuedSegments === 0,
    storeCount: stores.length,
    successCount,
    totalRecords,
    queuedSegments,
    queue: queueResult,
    results
  };
}

/** DF：处理限流重试队列 */
async function processDfRetryQueue() {
  const now = new Date();
  const items = await prisma.amazonVcDfSyncRetryQueue.findMany({
    where: { retryAt: { lte: now } },
    include: { amazonStore: true },
    orderBy: { retryAt: 'asc' }
  });
  if (items.length === 0) return { processed: 0, success: 0, failed: 0, deferred: 0 };

  let success = 0;
  let failed = 0;
  let deferred = 0;
  for (const item of items) {
    const store = item.amazonStore;
    if (!store || !store.refreshToken) {
      await prisma.amazonVcDfSyncRetryQueue.delete({ where: { id: item.id } });
      failed++;
      continue;
    }
    try {
      const spClient = createSpClient(store);
      let totalRecords = 0;
      let nextToken = null;
      do {
        const q = { createdAfter: item.createdAfter, createdBefore: item.createdBefore, includeDetails: 'true' };
        if (nextToken) q.nextToken = nextToken;
        const page = await fetchDfOrdersPage(spClient, q);
        for (const ord of page.orders) {
          await saveDfOrderWithItems(store.id, ord);
          totalRecords++;
        }
        nextToken = page.nextToken;
        if (nextToken) await sleep(200);
        else await sleep(RESTORE_DELAY_MS);
      } while (nextToken);

      const segmentEnd = new Date(item.createdBefore);
      const state = await prisma.amazonVcSyncState.findUnique({ where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'df' } } });
      const shouldUpdate = !state || Number(state.lastEndAt) < segmentEnd.getTime();
      if (shouldUpdate) {
        await prisma.amazonVcSyncState.upsert({
          where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'df' } },
          update: { lastEndAt: segmentEnd.getTime(), lastSyncAt: new Date(), recordCount: totalRecords },
          create: { amazonStoreId: store.id, syncType: 'df', lastEndAt: segmentEnd.getTime(), recordCount: totalRecords }
        });
      }
      await prisma.amazonVcDfSyncRetryQueue.delete({ where: { id: item.id } });
      success++;
      console.log(`[VC DF] [${generateAmazonRequestId()}] 队列任务成功: store=${store.id} ${item.createdAfter}~${item.createdBefore} 拉取 ${totalRecords} 条`);
    } catch (e) {
      if (isThrottleError(e) && item.attempts + 1 < QUEUE_MAX_ATTEMPTS) {
        const nextRetryAt = new Date(Date.now() + QUEUE_RETRY_AFTER_MINUTES * 60 * 1000);
        await prisma.amazonVcDfSyncRetryQueue.update({
          where: { id: item.id },
          data: { retryAt: nextRetryAt, attempts: item.attempts + 1 }
        });
        deferred++;
        console.warn(`[VC DF] [${generateAmazonRequestId()}] 队列任务再次限流，${QUEUE_RETRY_AFTER_MINUTES} 分钟后重试 (attempts=${item.attempts + 1}):`, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e));
      } else {
        await prisma.amazonVcDfSyncRetryQueue.delete({ where: { id: item.id } });
        failed++;
        console.error(`[VC DF] [${generateAmazonRequestId()}] 队列任务失败或超过最大重试次数，已移除:`, item.id, e?.code ?? e?.name, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
      }
    }
  }
  return { processed: items.length, success, failed, deferred };
}

/** DF：对单个 VC 店铺做增量拉取 */
async function incrementalSyncVcDfForStore(store, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS } = options;
  const { start, end } = await getNextDateRangeDf(store.id, { endDate, defaultLookbackDays });
  if (start >= end) {
    return { storeId: store.id, success: true, recordCount: 0, message: '无新时间范围需拉取' };
  }

  const spClient = createSpClient(store);
  let totalRecords = 0;
  let queuedSegments = 0;
  let currentStart = new Date(start);
  const endTime = end.getTime();

  while (currentStart.getTime() < endTime) {
    const segmentEnd = new Date(Math.min(currentStart.getTime() + MAX_DAYS_PER_REQUEST * 24 * 60 * 60 * 1000, endTime));
    const createdAfter = currentStart.toISOString();
    const createdBefore = segmentEnd.toISOString();

    let nextToken = null;
    let segmentDone = false;
    try {
      do {
        const q = { createdAfter, createdBefore, includeDetails: 'true' };
        if (nextToken) q.nextToken = nextToken;
        const page = await fetchDfOrdersPage(spClient, q);
        for (const ord of page.orders) {
          await saveDfOrderWithItems(store.id, ord);
          totalRecords++;
        }
        nextToken = page.nextToken;
        if (nextToken) await sleep(200);
        else await sleep(RESTORE_DELAY_MS);
      } while (nextToken);
      segmentDone = true;
    } catch (e) {
      if (isThrottleError(e)) {
        await enqueueDfSegmentForRetry(store.id, createdAfter, createdBefore);
        queuedSegments++;
        return {
          storeId: store.id,
          success: true,
          recordCount: totalRecords,
          queuedSegments,
          message: `部分时段因限流已入队，约 ${QUEUE_RETRY_AFTER_MINUTES} 分钟后重试`
        };
      }
      console.error(`[VC DF] [${generateAmazonRequestId()}] 店铺增量同步失败 storeId=%s:`, store.id, e?.code ?? e?.name, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
      throw e;
    }

    if (segmentDone) {
      await prisma.amazonVcSyncState.upsert({
        where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'df' } },
        update: { lastEndAt: segmentEnd.getTime(), lastSyncAt: new Date(), recordCount: totalRecords },
        create: { amazonStoreId: store.id, syncType: 'df', lastEndAt: segmentEnd.getTime(), recordCount: totalRecords }
      });
    }

    currentStart = new Date(segmentEnd.getTime() + 1);
    if (currentStart.getTime() < endTime) await sleep(500);
  }

  return { storeId: store.id, success: true, recordCount: totalRecords, queuedSegments };
}

/** DF：对所有 VC 店铺增量拉取 DF 单 */
async function incrementalSyncAllVcDf(options = {}) {
  const queueResult = await processDfRetryQueue();

  const stores = await getVcStores();
  if (stores.length === 0) {
    return {
      success: true,
      storeCount: 0,
      results: [],
      queue: queueResult,
      message: queueResult.processed > 0 ? `已处理 ${queueResult.processed} 个队列任务，无 VC 店铺` : '没有已授权的 VC 店铺'
    };
  }
  const results = [];
  for (const store of stores) {
    try {
      const r = await incrementalSyncVcDfForStore(store, options);
      results.push(r);
    } catch (e) {
      console.error(`[VC DF] [${generateAmazonRequestId()}] 店铺同步异常 storeId=%s:`, store.id, e?.message, e?.stack, 'apiRequestId=', getAmazonRequestIdFromError(e));
      results.push({ storeId: store.id, success: false, error: e?.message || String(e) });
    }
  }
  const successCount = results.filter((x) => x.success).length;
  const totalRecords = results.reduce((s, x) => s + (x.recordCount ?? 0), 0);
  const queuedSegments = results.reduce((s, x) => s + (x.queuedSegments ?? 0), 0);
  return {
    success: successCount === stores.length && queuedSegments === 0,
    storeCount: stores.length,
    successCount,
    totalRecords,
    queuedSegments,
    queue: queueResult,
    results
  };
}

/** Shipments：对单个 VC 店铺拉取最近 N 天 GetShipmentDetails（默认 60 天） */
async function incrementalSyncVcShipmentsForStore(store, options = {}) {
  const { endDate, defaultLookbackDays = DEFAULT_LOOKBACK_DAYS_SHIPMENTS } = options;
  const { start, end } = await getNextDateRangeShipments(store.id, { endDate, defaultLookbackDays });
  if (start >= end) {
    return { storeId: store.id, success: true, recordCount: 0, message: '无新时间范围需拉取' };
  }

  const spClient = createSpClient(store);
  let totalRecords = 0;
  let currentStart = new Date(start);
  const endTime = end.getTime();

  while (currentStart.getTime() < endTime) {
    const segmentEnd = new Date(Math.min(currentStart.getTime() + MAX_DAYS_PER_REQUEST * 24 * 60 * 60 * 1000, endTime));
    const shipAfter = currentStart.toISOString();
    const shipBefore = segmentEnd.toISOString();

    let nextToken = null;
    try {
      do {
        const q = { shipAfter, shipBefore, limit: 100 };
        if (nextToken) q.nextToken = nextToken;
        const page = await fetchShipmentDetailsPage(spClient, q);
        for (const s of page.shipments) {
          await saveShipmentDetail(store.id, s);
          totalRecords++;
        }
        nextToken = page.nextToken;
        if (nextToken) await sleep(200);
        else await sleep(RESTORE_DELAY_MS);
      } while (nextToken);
    } catch (e) {
      console.error(`[VC Shipments] [${generateAmazonRequestId()}] 店铺拉取失败 storeId=%s:`, store.id, e?.code ?? e?.name, e?.message, 'apiRequestId=', getAmazonRequestIdFromError(e), typeof e?.response === 'object' ? { status: e.response?.status, data: e.response?.data } : '');
      throw e;
    }

    await prisma.amazonVcSyncState.upsert({
      where: { amazonStoreId_syncType: { amazonStoreId: store.id, syncType: 'shipments' } },
      update: { lastEndAt: segmentEnd.getTime(), lastSyncAt: new Date(), recordCount: totalRecords },
      create: { amazonStoreId: store.id, syncType: 'shipments', lastEndAt: segmentEnd.getTime(), recordCount: totalRecords }
    });

    currentStart = new Date(segmentEnd.getTime() + 1);
    if (currentStart.getTime() < endTime) await sleep(500);
  }

  return { storeId: store.id, success: true, recordCount: totalRecords };
}

/** Shipments：对所有 VC 店铺拉取 GetShipmentDetails（最近 60 天内） */
async function incrementalSyncAllVcShipments(options = {}) {
  const stores = await getVcStores();
  if (stores.length === 0) {
    return { success: true, storeCount: 0, results: [], message: '没有已授权的 VC 店铺' };
  }
  const results = [];
  for (const store of stores) {
    try {
      const r = await incrementalSyncVcShipmentsForStore(store, options);
      results.push(r);
    } catch (e) {
      console.error(`[VC Shipments] [${generateAmazonRequestId()}] 店铺同步异常 storeId=%s:`, store.id, e?.message, e?.stack, 'apiRequestId=', getAmazonRequestIdFromError(e));
      results.push({ storeId: store.id, success: false, error: e?.message || String(e) });
    }
  }
  const successCount = results.filter((x) => x.success).length;
  const totalRecords = results.reduce((s, x) => s + (x.recordCount ?? 0), 0);
  return {
    success: successCount === stores.length,
    storeCount: stores.length,
    successCount,
    totalRecords,
    results
  };
}

export default {
  getVcStores,
  incrementalSyncVcPoForStore,
  incrementalSyncAllVcPo,
  processRetryQueue,
  incrementalSyncVcDfForStore,
  incrementalSyncAllVcDf,
  processDfRetryQueue,
  incrementalSyncVcShipmentsForStore,
  incrementalSyncAllVcShipments
};
