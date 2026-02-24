import prisma from '../../../config/database.js';
import lingXingAmazonService from '../amazon/lingXingAmazonService.js';
import lingXingReportService from '../reports/lingXingReportService.js';
import lingXingFinanceService from '../finance/lingXingFinanceService.js';
import lingXingWarehouseService from '../warehouse/lingXingWarehouseService.js';
import lingXingVcService from '../vc/lingXingVcService.js';
import lingXingPurchaseService from '../purchase/lingXingPurchaseService.js';
import lingxingProductService from '../products/lingxingProductService.js';
import lingXingSalesService from '../sales/lingXingSalesService.js';

const LOG_PREFIX = '[LingXingUnifiedSync]';

/**
 * 支持增量拉取的任务注册表
 * taskType -> { service, methodName, description }
 */
const INCREMENTAL_TASK_REGISTRY = {
  // 亚马逊报表
  allOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncAllOrdersReport', description: '所有订单' },
  fbaOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaOrdersReport', description: 'FBA订单' },
  fbaExchangeOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaExchangeOrdersReport', description: 'FBA换货订单' },
  fbaRefundOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaRefundOrdersReport', description: 'FBA退货订单' },
  fbmReturnOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbmReturnOrdersReport', description: 'FBM退货订单' },
  removalOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncRemovalOrdersReport', description: '移除订单' },
  removalShipment: { service: lingXingAmazonService, methodName: 'incrementalSyncRemovalShipmentReport', description: '移除货件' },
  transaction: { service: lingXingAmazonService, methodName: 'incrementalSyncTransactionReport', description: '交易明细' },
  amazonFulfilledShipments: { service: lingXingAmazonService, methodName: 'incrementalSyncAmazonFulfilledShipmentsReport', description: 'Amazon Fulfilled Shipments' },
  fbaInventoryEventDetail: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaInventoryEventDetailReport', description: 'FBA库存动销明细' },
  adjustmentList: { service: lingXingAmazonService, methodName: 'incrementalSyncAdjustmentListReport', description: '盘存记录' },

  // 报表
  salesReport: { service: lingXingReportService, methodName: 'incrementalSyncSalesReport', description: '销量报表' },
  productPerformance: { service: lingXingReportService, methodName: 'incrementalSyncProductPerformance', description: '产品表现' },
  mskuProfitStatistics: { service: lingXingReportService, methodName: 'incrementalSyncMskuProfitStatistics', description: '利润统计MSKU' },

  // 财务
  feeDetail: { service: lingXingFinanceService, methodName: 'incrementalSyncFeeDetails', description: '费用明细' },
  requestFundsOrder: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsOrders', description: '请款单' },

  // 仓库
  purchaseReceiptOrder: { service: lingXingWarehouseService, methodName: 'incrementalSyncPurchaseReceiptOrders', description: '收货单' },
  inboundOrder: { service: lingXingWarehouseService, methodName: 'incrementalSyncInboundOrders', description: '入库单' },
  outboundOrder: { service: lingXingWarehouseService, methodName: 'incrementalSyncOutboundOrders', description: '出库单' },

  // VC
  vcOrder: { service: lingXingVcService, methodName: 'incrementalSyncVcOrders', description: 'VC订单' },
  vcInvoice: { service: lingXingVcService, methodName: 'incrementalSyncVcInvoices', description: 'VC发货单' },

  // 采购
  purchaseOrder: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseOrders', description: '采购单' },

  // 产品
  localProduct: { service: lingxingProductService, methodName: 'incrementalSyncLocalProducts', description: '本地产品' },

  // 销售
  salesAmazonOrder: { service: lingXingSalesService, methodName: 'incrementalSyncAmazonOrders', description: '销售-亚马逊订单' }
};

/**
 * 领星统一同步服务
 * - 不接收 accountId，从数据表遍历所有启用账户
 * - 仅对支持增量拉取的任务使用增量方法
 */
class LingXingUnifiedSyncService {
  /**
   * 从数据表获取所有启用账户
   * @returns {Promise<Array<{ id: string, name: string }>>}
   */
  async getActiveAccounts() {
    const accounts = await prisma.lingXingAccount.findMany({
      where: { isActive: true },
      select: { id: true, name: true },
      orderBy: { name: 'asc' }
    });
    return accounts;
  }

  /**
   * 获取所有支持增量拉取的任务类型及说明
   * @returns {Array<{ taskType: string, description: string }>}
   */
  getSupportedIncrementalTaskTypes() {
    return Object.entries(INCREMENTAL_TASK_REGISTRY).map(([taskType, { description }]) => ({
      taskType,
      description
    }));
  }

  /**
   * 判断某任务类型是否支持增量
   */
  isIncrementalSupported(taskType) {
    return !!INCREMENTAL_TASK_REGISTRY[taskType];
  }

  /**
   * 对单个账户执行指定任务类型的增量同步
   * @param {string} accountId - 账户ID
   * @param {string} taskType - 任务类型
   * @param {Object} options - 透传给该任务增量方法的选项
   * @returns {Promise<{ accountId, accountName?, taskType, success, summary?, error? }>}
   */
  async runIncrementalSyncForAccount(accountId, taskType, options = {}) {
    const entry = INCREMENTAL_TASK_REGISTRY[taskType];
    if (!entry) {
      return { accountId, taskType, success: false, error: `不支持的增量任务类型: ${taskType}` };
    }
    const { service, methodName } = entry;
    const method = service[methodName];
    if (typeof method !== 'function') {
      return { accountId, taskType, success: false, error: `服务方法不存在: ${methodName}` };
    }
    try {
      const result = await method.call(service, accountId, options);
      return {
        accountId,
        taskType,
        success: true,
        summary: result?.summary,
        results: result?.results
      };
    } catch (err) {
      const message = err?.message || String(err);
      console.error(`${LOG_PREFIX} accountId=${accountId} taskType=${taskType} 失败:`, message);
      return { accountId, taskType, success: false, error: message };
    }
  }

  /**
   * 按任务类型执行增量同步：从数据表获取所有启用账户，逐账户执行该任务的增量方法
   * @param {string} taskType - 任务类型（见 getSupportedIncrementalTaskTypes）
   * @param {Object} options - 透传给各账户的增量方法选项（如 endDate, defaultLookbackDays, delayBetweenShops 等）
   * @returns {Promise<{ taskType, description, accountCount, results: Array<{ accountId, accountName?, success, summary?, error? }>, summary }>}
   */
  async runIncrementalSyncByTaskType(taskType, options = {}) {
    const entry = INCREMENTAL_TASK_REGISTRY[taskType];
    if (!entry) {
      throw new Error(`不支持的增量任务类型: ${taskType}，可选: ${Object.keys(INCREMENTAL_TASK_REGISTRY).join(', ')}`);
    }

    const accounts = await this.getActiveAccounts();
    if (accounts.length === 0) {
      console.warn(`${LOG_PREFIX} 无启用账户，跳过 taskType=${taskType}`);
      return {
        taskType,
        description: entry.description,
        accountCount: 0,
        results: [],
        summary: { successCount: 0, failCount: 0, totalRecords: 0, message: '无启用账户' }
      };
    }

    console.log(`${LOG_PREFIX} [${taskType}] 开始 共 ${accounts.length} 个账户`);
    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (const account of accounts) {
      const one = await this.runIncrementalSyncForAccount(account.id, taskType, options);
      results.push({ ...one, accountName: account.name });
      if (one.success) {
        successCount++;
        const s = one.summary || {};
        totalRecords += s.totalRecords ?? s.recordCount ?? 0;
      } else {
        failCount++;
      }
    }

    const summary = { accountCount: accounts.length, successCount, failCount, totalRecords };
    console.log(`${LOG_PREFIX} [${taskType}] 结束`, summary);
    return {
      taskType,
      description: entry.description,
      accountCount: accounts.length,
      results,
      summary
    };
  }

  /**
   * 执行全部支持增量拉取的任务：按任务类型依次执行，每种类型会遍历所有启用账户
   * @param {Object} options - 透传给各任务增量方法的选项
   * @param {Array<string>} taskTypes - 仅执行这些任务类型；不传则执行全部
   * @returns {Promise<{ accountCount, taskResults: Array<{ taskType, description, results, summary }>, summary }>}
   */
  async runAllIncrementalSync(options = {}, taskTypes = null) {
    const list = taskTypes && taskTypes.length > 0
      ? taskTypes.filter(t => this.isIncrementalSupported(t))
      : Object.keys(INCREMENTAL_TASK_REGISTRY);

    if (list.length === 0) {
      return {
        accountCount: 0,
        taskResults: [],
        summary: { message: '没有可执行的增量任务', taskCount: 0 }
      };
    }

    const accounts = await this.getActiveAccounts();
    const taskResults = [];

    for (const taskType of list) {
      try {
        const taskResult = await this.runIncrementalSyncByTaskType(taskType, options);
        taskResults.push(taskResult);
      } catch (err) {
        console.error(`${LOG_PREFIX} runAllIncrementalSync taskType=${taskType} 异常:`, err?.message);
        taskResults.push({
          taskType,
          description: INCREMENTAL_TASK_REGISTRY[taskType]?.description || taskType,
          accountCount: accounts.length,
          results: [],
          summary: { successCount: 0, failCount: accounts.length, error: err?.message }
        });
      }
    }

    const totalSuccess = taskResults.reduce((s, r) => s + (r.summary?.successCount ?? 0), 0);
    const totalFail = taskResults.reduce((s, r) => s + (r.summary?.failCount ?? 0), 0);
    const totalRecords = taskResults.reduce((s, r) => s + (r.summary?.totalRecords ?? 0), 0);

    return {
      accountCount: accounts.length,
      taskResults,
      summary: {
        taskCount: list.length,
        totalSuccess,
        totalFail,
        totalRecords
      }
    };
  }
}

export default new LingXingUnifiedSyncService();
export { INCREMENTAL_TASK_REGISTRY };
