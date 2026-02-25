import lingXingSyncStateService from './lingXingSyncStateService.js';

const LOG_PREFIX = '[IncrementalSync]';

/**
 * 账户级增量同步通用执行器（不按店铺 sid，taskType 对应一条同步状态）
 * 优先使用接口支持的「更新时间」维度（由各服务在 extraParams 中传入 search_field_time / date_type 等）
 *
 * @param {string} accountId - 领星账户ID
 * @param {string} taskType - 任务类型，用于读写 LingXingSyncState
 * @param {Object} options - 选项
 *   - endDate: string Y-m-d，不传则到昨天
 *   - defaultLookbackDays: number 无历史状态时回溯天数
 *   - timezone: string
 *   - extraParams: object 合并进 fetchFn 的日期参数（如 search_field_time: 'update_time', date_type: 4）
 *   - pageSize, delayBetweenPages 等由 fetchFn 内部使用
 * @param {Function} fetchFn - async (accountId, { start_date, end_date, ...extraParams }, options) => Promise<{ total?: number, data?: Array }>
 * @returns {Promise<{ success: boolean, recordCount: number, start_date: string, end_date: string, error?: string, skipped?: boolean }>}
 */
async function runAccountLevelIncrementalSync(accountId, taskType, options, fetchFn) {
  const {
    endDate = null,
    defaultLookbackDays = 7000,
    timezone = 'Asia/Shanghai',
    extraParams = {}
  } = options;

  const syncState = lingXingSyncStateService;
  const dateRange = await syncState.getIncrementalDateRange(accountId, taskType, null, {
    defaultLookbackDays,
    endDate,
    timezone
  });

  if (dateRange.isEmpty) {
    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 日期范围为空，跳过`);
    return {
      success: true,
      recordCount: 0,
      start_date: dateRange.start_date,
      end_date: dateRange.end_date,
      skipped: true
    };
  }

  try {
    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 拉取 ${dateRange.start_date} ~ ${dateRange.end_date} ...`);
    const result = await fetchFn(accountId, {
      start_date: dateRange.start_date,
      end_date: dateRange.end_date,
      ...extraParams
    }, options);

    const recordCount = result?.total ?? result?.data?.length ?? result?.orders?.length ?? result?.orderList?.length ?? result?.inboundOrderList?.length ?? result?.outboundOrderList?.length ?? result?.feeDetails?.length ?? result?.plans?.length ?? result?.returnOrders?.length ?? result?.changeOrders?.length ?? 0;

    await syncState.upsertSyncState(accountId, taskType, null, {
      lastEndTimestamp: dateRange.end_timestamp,
      lastSyncAt: new Date(),
      lastRecordCount: recordCount,
      lastStatus: 'success',
      lastErrorMessage: null
    });

    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 成功 拉取${recordCount}条 lastEndTimestamp=${dateRange.end_datetime}`);
    return {
      success: true,
      recordCount,
      start_date: dateRange.start_date,
      end_date: dateRange.end_date
    };
  } catch (err) {
    const message = err?.message || String(err);
    await syncState.upsertSyncState(accountId, taskType, null, {
      lastEndTimestamp: null,
      lastSyncAt: new Date(),
      lastRecordCount: null,
      lastStatus: 'failed',
      lastErrorMessage: message
    }).catch(() => {});

    console.error(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 失败 ${dateRange.start_date}~${dateRange.end_date} error=${message}`);
    return {
      success: false,
      recordCount: 0,
      start_date: dateRange.start_date,
      end_date: dateRange.end_date,
      error: message
    };
  }
}

export {
  runAccountLevelIncrementalSync
};

export default {
  runAccountLevelIncrementalSync
};
