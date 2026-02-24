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
    const sidVal = sid === undefined || sid === null ? null : parseInt(sid, 10);
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
    const sidVal = sid === undefined || sid === null ? null : parseInt(sid, 10);
    return prisma.lingXingSyncState.upsert({
      where: {
        accountId_taskType_sid: { accountId, taskType, sid: sidVal }
      },
      update: {
        ...(data.lastEndDate !== undefined && { lastEndDate: data.lastEndDate }),
        ...(data.lastSyncAt !== undefined && { lastSyncAt: data.lastSyncAt }),
        ...(data.lastRecordCount !== undefined && { lastRecordCount: data.lastRecordCount }),
        ...(data.lastStatus !== undefined && { lastStatus: data.lastStatus }),
        ...(data.lastErrorMessage !== undefined && { lastErrorMessage: data.lastErrorMessage })
      },
      create: {
        accountId,
        taskType,
        sid: sidVal,
        lastEndDate: data.lastEndDate ?? null,
        lastSyncAt: data.lastSyncAt ?? null,
        lastRecordCount: data.lastRecordCount ?? null,
        lastStatus: data.lastStatus ?? null,
        lastErrorMessage: data.lastErrorMessage ?? null
      }
    });
  }

  /**
   * 计算本次增量同步的日期范围
   * 若从未同步过，则从 defaultLookbackDays 天前到 endDate；否则从 lastEndDate 的次日到 endDate
   * @param {string} accountId - 领星账户ID
   * @param {string} taskType - 任务类型
   * @param {number|null} sid - 店铺ID
   * @param {Object} options - defaultLookbackDays, endDate (Y-m-d), timezone
   * @returns {Promise<{ start_date: string, end_date: string, isEmpty: boolean }>}
   */
  async getIncrementalDateRange(accountId, taskType, sid, options = {}) {
    const {
      defaultLookbackDays = 7,
      endDate: optionEndDate = null,
      timezone = 'Asia/Shanghai'
    } = options;

    const state = await this.getSyncState(accountId, taskType, sid);

    let end_date = optionEndDate;
    if (!end_date) {
      const now = new Date();
      const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      });
      const parts = formatter.formatToParts(now);
      const y = parts.find(p => p.type === 'year').value;
      const m = parts.find(p => p.type === 'month').value;
      const d = parts.find(p => p.type === 'day').value;
      const todayDate = new Date(parseInt(y, 10), parseInt(m, 10) - 1, parseInt(d, 10));
      todayDate.setDate(todayDate.getDate() - 1);
      end_date = todayDate.toISOString().slice(0, 10);
    }

    let start_date;
    if (state?.lastEndDate) {
      const last = new Date(state.lastEndDate + 'T12:00:00Z');
      last.setUTCDate(last.getUTCDate() + 1);
      start_date = last.toISOString().slice(0, 10);
      console.log(`${LOG_PREFIX} [getIncrementalDateRange] accountId=${accountId} taskType=${taskType} sid=${sid ?? 'null'} 有历史状态 lastEndDate=${state.lastEndDate} => 本次范围 ${start_date} ~ ${end_date}`);
    } else {
      const end = new Date(end_date + 'T12:00:00Z');
      end.setUTCDate(end.getUTCDate() - defaultLookbackDays + 1);
      start_date = end.toISOString().slice(0, 10);
      console.log(`${LOG_PREFIX} [getIncrementalDateRange] accountId=${accountId} taskType=${taskType} sid=${sid ?? 'null'} 无历史状态 使用回溯${defaultLookbackDays}天 => 本次范围 ${start_date} ~ ${end_date}`);
    }

    if (start_date > end_date) {
      console.log(`${LOG_PREFIX} [getIncrementalDateRange] accountId=${accountId} taskType=${taskType} sid=${sid ?? 'null'} 日期无交集 start_date>end_date 跳过`);
      return { start_date: end_date, end_date, isEmpty: true };
    }
    return { start_date, end_date, isEmpty: false };
  }
}

export default new LingXingSyncStateService();
