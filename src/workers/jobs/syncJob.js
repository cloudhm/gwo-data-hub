import lingXingUnifiedSyncService from '../../services/lingxing/sync/lingXingUnifiedSyncService.js';

const LOG_PREFIX = '[sync-job]';

/**
 * 定时增量同步任务：按任务类型依次执行，每种类型会遍历所有启用领星账户。
 * 实际执行的任务类型（与 lingXingUnifiedSyncService.INCREMENTAL_TASK_REGISTRY 一致，便于核对）：
 *
 * 1.  allOrders - 所有订单
 * 2.  fbaOrders - FBA订单
 * 3.  fbaExchangeOrders - FBA换货订单
 * 4.  fbaRefundOrders - FBA退货订单
 * 5.  fbmReturnOrders - FBM退货订单
 * 6.  removalOrders - 移除订单
 * 7.  removalShipment - 移除货件
 * 8.  transaction - 交易明细
 * 9.  amazonFulfilledShipments - Amazon Fulfilled Shipments
 * 10. fbaInventoryEventDetail - FBA库存动销明细
 * 11. adjustmentList - 盘存记录
 * 12. salesReport - 销量报表
 * 13. productPerformance - 产品表现
 * 14. mskuProfitStatistics - 利润统计MSKU
 * 15. feeDetail - 费用明细
 * 16. requestFundsOrder - 请款单
 * 17. purchaseReceiptOrder - 收货单
 * 18. inboundOrder - 入库单
 * 19. outboundOrder - 出库单
 * 20. vcOrder - VC订单
 * 21. vcInvoice - VC发货单
 * 22. purchaseOrder - 采购单
 * 23. purchasePlan - 采购计划
 * 24. purchaseReturnOrder - 采购退货单
 * 25. purchaseChangeOrder - 采购变更单
 * 26. localProduct - 本地产品
 * 27. salesAmazonOrder - 销售-亚马逊订单
 */
export const syncJob = {
    name: 'sync-job',
    cronExpression: '0 1,7 * * *', // 每天1点/7点执行
    enabled: true, // 设为 false 可关闭此任务
    async handler() {
      const runTask = async (taskType, label) => {
        try {
          await lingXingUnifiedSyncService.runIncrementalSyncByTaskType(taskType);
        } catch (err) {
          console.error(`${LOG_PREFIX} [${taskType}] ${label} 失败:`, err?.message ?? err);
        }
      };

      // 1. 所有订单
      await runTask('allOrders', '所有订单');
      // 2. FBA订单
      await runTask('fbaOrders', 'FBA订单');
      // 3. FBA换货订单
      await runTask('fbaExchangeOrders', 'FBA换货订单');
      // 4. FBA退货订单
      await runTask('fbaRefundOrders', 'FBA退货订单');
      // 5. FBM退货订单
      await runTask('fbmReturnOrders', 'FBM退货订单');
      // 6. 移除订单
      await runTask('removalOrders', '移除订单');
      // 7. 移除货件
      await runTask('removalShipment', '移除货件');
      // 8. 交易明细
      await runTask('transaction', '交易明细');
      // 9. Amazon Fulfilled Shipments
      await runTask('amazonFulfilledShipments', 'Amazon Fulfilled Shipments');
      // 10. FBA库存动销明细
      await runTask('fbaInventoryEventDetail', 'FBA库存动销明细');
      // 11. 盘存记录
      await runTask('adjustmentList', '盘存记录');
      // 12. 销量报表
      await runTask('salesReport', '销量报表');
      // 13. 产品表现
      await runTask('productPerformance', '产品表现');
      // 14. 利润统计MSKU
      await runTask('mskuProfitStatistics', '利润统计MSKU');
      // 15. 费用明细
      await runTask('feeDetail', '费用明细');
      // 16. 请款单
      await runTask('requestFundsOrder', '请款单');
      // 17. 收货单
      await runTask('purchaseReceiptOrder', '收货单');
      // 18. 入库单
      await runTask('inboundOrder', '入库单');
      // 19. 出库单
      await runTask('outboundOrder', '出库单');
      // 20. VC订单
      await runTask('vcOrder', 'VC订单');
      // 21. VC发货单
      await runTask('vcInvoice', 'VC发货单');
      // 22. 采购单
      await runTask('purchaseOrder', '采购单');
      // 23. 采购计划
      await runTask('purchasePlan', '采购计划');
      // 24. 采购退货单
      await runTask('purchaseReturnOrder', '采购退货单');
      // 25. 采购变更单
      await runTask('purchaseChangeOrder', '采购变更单');
      // 26. 本地产品
      await runTask('localProduct', '本地产品');
      // 27. 销售-亚马逊订单
      await runTask('salesAmazonOrder', '销售-亚马逊订单');
    }
  };
  