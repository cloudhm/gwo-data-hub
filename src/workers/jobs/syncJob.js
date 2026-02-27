import lingXingUnifiedSyncService from '../../services/lingxing/sync/lingXingUnifiedSyncService.js';
import jobTaskStatusService from '../../services/jobTaskStatusService.js';

const LOG_PREFIX = '[sync-job]';
const CRON_EXPRESSION = '0 1,7 * * *'; // 每天1点/7点执行

/**
 * 增量同步子任务列表（与 lingXingUnifiedSyncService.INCREMENTAL_TASK_REGISTRY 一致）
 * [ taskType, 中文描述 ]，供 task-status 接口展示完整任务列表
 */
export const SYNC_TASKS = [
  // 亚马逊市场列表全量同步 /api/lingxing/sync/run-full/marketplaces
  ['marketplaces', '基础信息-亚马逊市场列表全量同步', 1],
  // 世界州列表全量同步 /api/lingxing/sync/run-full/worldStates
  ['worldStates', '基础信息-世界州列表全量同步', 1],
  // 汇率全量同步 /api/lingxing/sync/run-full/currencyRates
  ['currencyRates', '基础信息-汇率全量同步', 1],
  // 账户用户全量同步 /api/lingxing/sync/run-full/accountUsers
  ['accountUsers', '基础信息-账户用户全量同步', 1],
  // 亚马逊店铺列表全量同步 /api/lingxing/sync/run-full/sellerLists
  ['sellerLists', '基础信息-亚马逊店铺列表全量同步', 1],
  // 概念店铺列表全量同步 /api/lingxing/sync/run-full/conceptSellerLists
  ['conceptSellerLists', '基础信息-概念店铺列表全量同步', 1],

  // 本地产品增量同步 /api/lingxing/sync/run/localProduct
  ['localProduct', '产品-本地产品增量同步', 0],

  // /api/lingxing/sync/run-full/suppliers
  ['suppliers', '采购-供应商全量同步', 1],
  // /api/lingxing/sync/run-full/purchasers
  ['purchasers', '采购-采购商全量同步', 1],
  // /api/lingxing/sync/run/purchaseOrder
  ['purchaseOrder', '采购-采购单增量同步', 0],
  // /api/lingxing/sync/run/purchasePlan
  ['purchasePlan', '采购-采购计划增量同步', 0],
  // /api/lingxing/sync/run/purchaseReturnOrder
  ['purchaseReturnOrder', '采购-采购退货单增量同步', 0],
  // /api/lingxing/sync/run/incrementalSyncPurchaseChangeOrders
  ['purchaseChangeOrder', '采购-采购变更单增量同步', 0],

  // /api/lingxing/sync/run-full/channels
  ['channels', '物流-物流渠道全量同步', 1],
  // /api/lingxing/sync/run-full/headLogisticsProviders
  ['headLogisticsProviders', '物流-头程物流商全量同步', 1],
  // /api/lingxing/sync/run-full/transportMethods
  ['transportMethods', '物流-运输方式全量同步', 1],

  // /api/lingxing/sync/run/salesAmazonOrder
  ['salesAmazonOrder', '销售-亚马逊订单增量同步', 0],
  // /api/lingxing/sync/run-full/listings
  ['listings', '销售-Listing全量同步', 1],

  // /api/lingxing/sync/run-full/warehouses
  ['warehouses', '仓库-仓库全量同步', 1],
  // /api/lingxing/sync/run-full/warehouseBins
  ['warehouseBins', '仓库-仓库库位全量同步', 1],
  // /api/lingxing/sync/run-full/inventoryDetails
  ['inventoryDetails', '仓库-库存明细全量同步', 1],
  // /api/lingxing/sync/run-full/fbaWarehouseDetails
  ['fbaWarehouseDetails', '仓库-FBA仓库明细全量同步', 1],
  // /api/lingxing/sync/run-full/inventoryBinDetails
  ['inventoryBinDetails', '仓库-库位库存明细全量同步', 1],
  // /api/lingxing/sync/run/purchaseReceiptOrder
  ['purchaseReceiptOrder', '仓库-收货单增量同步', 0],
  // /api/lingxing/sync/run/inboundOrder
  ['inboundOrder', '仓库-入库单增量同步', 0],
  // /api/lingxing/sync/run/outboundOrder
  ['outboundOrder', '仓库-出库单增量同步', 0],

  // /api/lingxing/sync/run-full/keywords
  ['keywords', '工具-关键词全量同步', 1],

  // /api/lingxing/sync/run/allOrders
  ['allOrders', '亚马逊源数据-所有订单增量同步', 0],
  // /api/lingxing/sync/run/fbaOrders
  ['fbaOrders', '亚马逊源数据-FBA订单增量同步', 0],
  // /api/lingxing/sync/run/fbaExchangeOrders
  ['fbaExchangeOrders', '亚马逊源数据-FBA换货订单增量同步', 0],
  // /api/lingxing/sync/run/fbaRefundOrders
  ['fbaRefundOrders', '亚马逊源数据-FBA退款订单增量同步', 0],
  // /api/lingxing/sync/run/fbmReturnOrders
  ['fbmReturnOrders', '亚马逊源数据-FBM退货订单增量同步', 0],
  // /api/lingxing/sync/run/removalOrders
  ['removalOrders', '亚马逊源数据-移除订单增量同步', 0],
  // /api/lingxing/sync/run/removalShipment
  ['removalShipment', '亚马逊源数据-移除货件增量同步', 0],
  // /api/lingxing/sync/run/transaction
  ['transaction', '亚马逊源数据-交易明细增量同步', 0],
  // /api/lingxing/sync/run/amazonFulfilledShipments
  ['amazonFulfilledShipments', '亚马逊源数据-Amazon Fulfilled Shipments增量同步', 0],
  // /api/lingxing/sync/run/fbaInventoryEventDetail
  ['fbaInventoryEventDetail', '亚马逊源数据-FBA库存动销明细增量同步', 0],
  // /api/lingxing/sync/run/adjustmentList
  ['adjustmentList', '亚马逊源数据-盘存记录增量同步', 0],

  // /api/lingxing/sync/run/requestFundsPoolPurchase
  ['requestFundsPoolPurchase', '财务-请款池采购单增量同步', 0],
  // /api/lingxing/sync/run/requestFundsPoolInbound
  ['requestFundsPoolInbound', '财务-请款池入库单增量同步', 0],
  // /api/lingxing/sync/run/requestFundsPoolPrepay
  ['requestFundsPoolPrepay', '财务-请款池预付单增量同步', 0],
  // /api/lingxing/sync/run/requestFundsPoolLogistics
  ['requestFundsPoolLogistics', '财务-请款池物流单增量同步', 0],
  // /api/lingxing/sync/run/requestFundsPoolCustomFee
  ['requestFundsPoolCustomFee', '财务-请款池自定义费用增量同步', 0],
  // /api/lingxing/sync/run/requestFundsPoolOtherFee
  ['requestFundsPoolOtherFee', '财务-请款池其他费用增量同步', 0],

  // /api/lingxing/sync/run-full/feeTypes
  ['feeTypes', '财务-费用类型全量同步', 1],
  // /api/lingxing/sync/run/receivableReport
  ['receivableReport', '财务-应收报告增量同步', 0],
  // /api/lingxing/sync/run/feeDetail
  ['feeDetail', '财务-费用明细增量同步', 0],
  // /api/lingxing/sync/run/requestFundsOrder
  ['requestFundsOrder', '财务-请款单增量同步', 0],
  // // /api/lingxing/sync/run-full/requestFundsPoolPurchase
  // ['requestFundsPoolPurchase', '财务-请款池采购单全量同步', 1],
  // // /api/lingxing/sync/run-full/requestFundsPoolInbound
  // ['requestFundsPoolInbound', '财务-请款池入库单全量同步', 1],
  // // /api/lingxing/sync/run-full/requestFundsPoolPrepay
  // ['requestFundsPoolPrepay', '财务-请款池预付单全量同步', 1],
  // // /api/lingxing/sync/run-full/requestFundsPoolLogistics
  // ['requestFundsPoolLogistics', '财务-请款池物流单全量同步', 1],
  // // /api/lingxing/sync/run-full/requestFundsPoolCustomFee
  // ['requestFundsPoolCustomFee', '财务-请款池自定义费用全量同步', 1],
  // // /api/lingxing/sync/run-full/requestFundsPoolOtherFee
  // ['requestFundsPoolOtherFee', '财务-请款池其他费用全量同步', 1],
  // /api/lingxing/sync/run/profitReportOrderTransactionFull
  ['profitReportOrderTransactionFull', '财务-利润报表订单交易明细全量同步', 1],
  // /api/lingxing/sync/run/receivableReportDetail
  ['receivableReportDetail', '财务-应收报告明细增量同步', 0],
  // /api/lingxing/sync/run/receivableReportDetailInfo
  ['receivableReportDetailInfo', '财务-应收报告明细信息增量同步', 0],
  // /api/lingxing/sync/run/settlementSummary
  ['settlementSummary', '财务-结算汇总增量同步', 0],
  // /api/lingxing/sync/run/settlementTransactionDetail
  ['settlementTransactionDetail', '财务-结算交易明细增量同步', 0],
  // /api/lingxing/sync/run/inventoryLedgerDetail
  ['inventoryLedgerDetail', '财务-库存流水明细增量同步', 0],
  // /api/lingxing/sync/run/inventoryLedgerSummary
  ['inventoryLedgerSummary', '财务-库存流水汇总增量同步', 0],
  // /api/lingxing/sync/run/settlementReport
  ['settlementReport', '财务-结算报表增量同步', 0],
  // /api/lingxing/sync/run/fbaCostStream
  ['fbaCostStream', '财务-FBA成本流增量同步', 0],
  // /api/lingxing/sync/run/adsInvoice
  ['adsInvoice', '财务-广告发票增量同步', 0],

  // /api/lingxing/sync/run/salesReport
  ['salesReport', '报表-销量报表增量同步', 0],
  // /api/lingxing/sync/run/productPerformance
  ['productPerformance', '报表-产品表现增量同步', 0],
  // /api/lingxing/sync/run/mskuProfitStatistics
  ['mskuProfitStatistics', '报表-利润统计MSKU增量同步', 0],
  // /api/lingxing/sync/run/profitReportOrder
  ['profitReportOrder', '报表-利润报表订单增量同步', 0],
  // /api/lingxing/sync/run/profitReportOrderTransaction
  ['profitReportOrderTransaction', '报表-利润报表订单交易明细增量同步', 0],
  // /api/lingxing/sync/run/asinProfitReport
  ['asinProfitReport', '报表-利润报表ASIN增量同步', 0],
  // /api/lingxing/sync/run/parentAsinProfitReport
  ['parentAsinProfitReport', '报表-利润报表父ASIN增量同步', 0],
  // /api/lingxing/sync/run/sellerProfitReport
  ['sellerProfitReport', '报表-利润报表卖家增量同步', 0],  
  // /api/lingxing/sync/run/reimbursementReport
  ['reimbursementReport', '报表-理赔报告增量同步', 0],
  // /api/lingxing/sync/run/returnOrderAnalysis
  ['returnOrderAnalysis', '报表-退货订单分析增量同步', 0],
  // /api/lingxing/sync/run/purchaseReportProduct
  ['purchaseReportProduct', '报表-采购报表产品增量同步', 0],
  // /api/lingxing/sync/run/purchaseReportSupplier
  ['purchaseReportSupplier', '报表-采购报表供应商增量同步', 0],
  // /api/lingxing/sync/run/purchaseReportBuyer
  ['purchaseReportBuyer', '报表-采购报表采购商增量同步', 0],
  // /api/lingxing/sync/run/operateLog
  ['operateLog', '报表-操作日志增量同步', 0],
  // /api/lingxing/sync/run/storeSummarySales
  ['storeSummarySales', '报表-店铺汇总销量增量同步', 0],
  // /api/lingxing/sync/run/storageReportLocalAggregate
  ['storageReportLocalAggregate', '报表-本地仓库存报表增量同步', 0],
  // /api/lingxing/sync/run/storageReportLocalDetail
  ['storageReportLocalDetail', '报表-本地仓库存报表明细增量同步', 0],
  // /api/lingxing/sync/run/storageReportOverseasAggregate
  ['storageReportOverseasAggregate', '报表-海外仓库存报表增量同步', 0],
  // /api/lingxing/sync/run/storageReportOverseasDetail
  ['storageReportOverseasDetail', '报表-海外仓库存报表明细增量同步', 0],
  // /api/lingxing/sync/run/storageReportFbaGather
  ['storageReportFbaGather', '报表-FBA库存报表增量同步', 0],
  // /api/lingxing/sync/run/storageReportFbaDetail
  ['storageReportFbaDetail', '报表-FBA库存报表明细增量同步', 0],
  // /api/lingxing/sync/run/fbaStorageFeeMonth
  ['fbaStorageFeeMonth', '财务-FBA仓储费月度报表增量同步', 0],
  
  // /api/lingxing/sync/run-full/vcSellers
  ['vcSellers', 'VC-VC店铺全量同步', 1],
  // /api/lingxing/sync/run-full/vcListings
  ['vcListings', 'VC-VCListing全量同步', 1],
  // /api/lingxing/sync/run/vcOrder
  ['vcOrder', 'VC-VC订单增量同步', 0],
  // /api/lingxing/sync/run/vcInvoice
  ['vcInvoice', 'VC-VC发货单增量同步', 0],
];

/**
 * 生成多 job：每个 taskType 一个独立 job，同一 cron 触发时由调度层串行执行
 * 执行前后写入 JobTaskStatus（最近执行时间、是否异常、异常原因、下次计划执行时间）
 */
function buildSyncJobs() {
  return SYNC_TASKS.map(([taskType, label, full]) => {
    const jobName = `sync-job-${taskType}`;
    return {
      name: jobName,
      cronExpression: CRON_EXPRESSION,
      enabled: true, // 可按 taskType 单独设为 false 关闭
      async handler() {
        const nextRun = jobTaskStatusService.getNextScheduledRun(new Date());
        try {
          await jobTaskStatusService.upsertJobTaskStatus({
            jobName,
            taskType,
            nextScheduledAt: nextRun
          });
        } catch (e) {
          console.error(`${LOG_PREFIX} [${taskType}] 写入 nextScheduledAt 失败:`, e?.message);
        }

        try {
          full ? await lingXingUnifiedSyncService.runFullSyncByTaskType(taskType, {useCache: false}) : await lingXingUnifiedSyncService.runIncrementalSyncByTaskType(taskType, {useCache: false});
          try {
            await jobTaskStatusService.upsertJobTaskStatus({
              jobName,
              taskType,
              lastRunAt: new Date(),
              lastStatus: 'success',
              lastError: null
            });
          } catch (e) {
            console.error(`${LOG_PREFIX} [${taskType}] 写入执行状态失败:`, e?.message);
          }
        } catch (err) {
          const msg = err?.message ?? String(err);
          console.error(`${LOG_PREFIX} [${taskType}] ${label} 失败:`, msg);
          try {
            await jobTaskStatusService.upsertJobTaskStatus({
              jobName,
              taskType,
              lastRunAt: new Date(),
              lastStatus: 'failed',
              lastError: msg
            });
          } catch (e) {
            console.error(`${LOG_PREFIX} [${taskType}] 写入失败状态失败:`, e?.message);
          }
        }
      }
    };
  });
}

export const syncJobs = buildSyncJobs();
