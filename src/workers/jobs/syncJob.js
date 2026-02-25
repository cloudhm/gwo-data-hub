import lingXingUnifiedSyncService from '../../services/lingxing/sync/lingXingUnifiedSyncService.js';
import jobTaskStatusService from '../../services/jobTaskStatusService.js';

const LOG_PREFIX = '[sync-job]';
const CRON_EXPRESSION = '0 1,7 * * *'; // 每天1点/7点执行

/**
 * 增量同步子任务列表（与 lingXingUnifiedSyncService.INCREMENTAL_TASK_REGISTRY 一致）
 * [ taskType, 中文描述 ]，供 task-status 接口展示完整任务列表
 */
export const SYNC_TASKS = [
  ['allOrders', '所有订单'],
  ['fbaOrders', 'FBA订单'],
  ['fbaExchangeOrders', 'FBA换货订单'],
  ['fbaRefundOrders', 'FBA退货订单'],
  ['fbmReturnOrders', 'FBM退货订单'],
  ['removalOrders', '移除订单'],
  ['removalShipment', '移除货件'],
  ['transaction', '交易明细'],
  ['amazonFulfilledShipments', 'Amazon Fulfilled Shipments'],
  ['fbaInventoryEventDetail', 'FBA库存动销明细'],
  ['adjustmentList', '盘存记录'],
  ['salesReport', '销量报表'],
  ['productPerformance', '产品表现'],
  ['mskuProfitStatistics', '利润统计MSKU'],
  ['feeDetail', '费用明细'],
  ['requestFundsOrder', '请款单'],
  ['purchaseReceiptOrder', '收货单'],
  ['inboundOrder', '入库单'],
  ['outboundOrder', '出库单'],
  ['vcOrder', 'VC订单'],
  ['vcInvoice', 'VC发货单'],
  ['purchaseOrder', '采购单'],
  ['purchasePlan', '采购计划'],
  ['purchaseReturnOrder', '采购退货单'],
  ['purchaseChangeOrder', '采购变更单'],
  ['localProduct', '本地产品'],
  ['salesAmazonOrder', '销售-亚马逊订单']
];

/**
 * 生成多 job：每个 taskType 一个独立 job，同一 cron 触发时由调度层串行执行
 * 执行前后写入 JobTaskStatus（最近执行时间、是否异常、异常原因、下次计划执行时间）
 */
function buildSyncJobs() {
  return SYNC_TASKS.map(([taskType, label]) => {
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
          await lingXingUnifiedSyncService.runIncrementalSyncByTaskType(taskType);
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
