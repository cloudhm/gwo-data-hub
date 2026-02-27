import prisma from '../../../config/database.js';

const LOG_PREFIX = '[IncrementalSync]';

/**
 * 领星增量同步状态服务
 * 用于读写「上次同步结束日期」等状态，支持按接口的增量同步
 */
class LingXingSyncStateService {
  /**
   * 获取指定任务的上次同步状态
   * @param {string} accountId - 领星账户ID
   * @param {string} taskType - 任务类型（如 allOrders）
   * @param {number|null} sid - 店铺ID，按店铺维度的任务必传；不按店铺传 null
   */
  async getSyncState(accountId, taskType, sid = null) {
    const sidVal = sid === undefined || sid === null ? 0 : parseInt(sid, 10);
    return prisma.lingXingSyncState.findUnique({
      where: {
        accountId_taskType_sid: { accountId, taskType, sid: sidVal }
      }
    });
  }

  /**
   * 创建或更新同步状态（upsert）
   */
  async upsertSyncState(accountId, taskType, sid, data = {}) {
    const sidVal = sid === undefined || sid === null ? 0 : parseInt(sid, 10);
    return prisma.lingXingSyncState.upsert({
      where: {
        accountId_taskType_sid: { accountId, taskType, sid: sidVal }
      },
      update: {
        ...(data.lastEndTimestamp !== undefined && { lastEndTimestamp: data.lastEndTimestamp }),
        ...(data.lastSyncAt !== undefined && { lastSyncAt: data.lastSyncAt }),
        ...(data.lastRecordCount !== undefined && { lastRecordCount: data.lastRecordCount }),
        ...(data.lastStatus !== undefined && { lastStatus: data.lastStatus }),
        ...(data.lastErrorMessage !== undefined && { lastErrorMessage: data.lastErrorMessage })
      },
      create: {
        accountId,
        taskType,
        sid: sidVal,
        lastEndTimestamp: data.lastEndTimestamp ?? null,
        lastSyncAt: data.lastSyncAt ?? null,
        lastRecordCount: data.lastRecordCount ?? null,
        lastStatus: data.lastStatus ?? null,
        lastErrorMessage: data.lastErrorMessage ?? null
      }
    });
  }

  /**
   * 计算本次增量同步的日期范围
   * 若从未同步过：从 defaultLookbackDays 天前到 endDate；否则：从上次同步的 lastEndTimestamp 到当前时间点。
   * 传入的 endDate 不做任何处理，原样使用。
   * @param {string} accountId - 领星账户ID
   * @param {string} taskType - 任务类型
   * @param {number|null} sid - 店铺ID
   * @param {Object} options - defaultLookbackDays, endDate（原样使用，不处理）, timezone
   * @returns {Promise<{ start_date: string, start_timestamp: Date, end_date: string, end_datetime: string, end_timestamp: Date, isEmpty: boolean }>}
   */
  async getIncrementalDateRange(accountId, taskType, sid, options = {}) {
    const {
      defaultLookbackDays = 365,
      endDate: optionEndDate = null,
      timezone = 'Asia/Shanghai'
    } = options;

    const state = await this.getSyncState(accountId, taskType, sid);

    const hasExplicitEndDate = optionEndDate != null && optionEndDate !== '';

    let start_date;
    let start_timestamp;
    let end_date;
    let end_datetime;
    let end_timestamp;

    if (state?.lastEndTimestamp) {
      // 有历史状态：从 lastEndTimestamp 到当前时间点（不使用传入的 endDate）
      const last = new Date(state.lastEndTimestamp);
      start_date = last.toISOString().slice(0, 10);
      start_timestamp = last;
      end_timestamp = new Date();
      end_datetime = end_timestamp.toLocaleString('sv-SE', { timeZone: timezone }).replace(' ', 'T');
      end_date = end_timestamp.toLocaleString('sv-SE', { timeZone: timezone }).slice(0, 10);
      console.log(`${LOG_PREFIX} [getIncrementalDateRange] accountId=${accountId} taskType=${taskType} sid=${sid ?? 'null'} 有历史状态 lastEndTimestamp=${state.lastEndTimestamp.toISOString()} => 本次范围 ${start_date} ~ ${end_date}`);
    } else {
      // 从未同步过：从 defaultLookbackDays 天前到 endDate（未传 endDate 则到当前时间）
      if (hasExplicitEndDate) {
        end_date = optionEndDate;
        end_datetime = optionEndDate;
        end_timestamp = new Date(optionEndDate);
      } else {
        end_timestamp = new Date();
        end_datetime = end_timestamp.toLocaleString('sv-SE', { timeZone: timezone }).replace(' ', 'T');
        end_date = end_timestamp.toLocaleString('sv-SE', { timeZone: timezone }).slice(0, 10);
      }
      const endForStart = new Date(end_timestamp);
      endForStart.setUTCDate(endForStart.getUTCDate() - defaultLookbackDays + 1);
      start_date = endForStart.toISOString().slice(0, 10);
      start_timestamp = endForStart;
      console.log(`${LOG_PREFIX} [getIncrementalDateRange] accountId=${accountId} taskType=${taskType} sid=${sid ?? 'null'} 无历史状态 使用回溯${defaultLookbackDays}天 => 本次范围 ${start_date} ~ ${end_date}`);
    }

    return { start_date, start_timestamp, end_date, end_datetime, end_timestamp, isEmpty: false };
  }
}

export default new LingXingSyncStateService();
