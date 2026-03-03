import prisma from '../config/database.js';

/**
 * 计算 cron '0 1,7 * * *'（每天 1:00、7:00）的下一次执行时间
 * @param {Date} [from=new Date()] - 从该时间起算
 * @returns {Date} 下一次执行时刻
 */
export function getNextScheduledRun(from = new Date()) {
  const d = new Date(from);
  const hour = d.getHours();
  const minute = d.getMinutes();
  const currentMinutes = hour * 60 + minute;

  // 1:00 = 60, 7:00 = 420
  if (currentMinutes < 60) {
    // 0:00 ~ 0:59 → 今天 1:00
    d.setHours(1, 0, 0, 0);
    return d;
  }
  if (currentMinutes < 420) {
    // 1:00 ~ 6:59 → 今天 7:00
    d.setHours(7, 0, 0, 0);
    return d;
  }
  // 7:00 及以后 → 明天 1:00
  d.setDate(d.getDate() + 1);
  d.setHours(1, 0, 0, 0);
  return d;
}

/**
 * 当前 cron 轮次的开始时间（用于重启后恢复：本轮未完成的任务会重新入队）
 * '0 1,7 * * *' → 1:00 或 7:00 为轮次起点
 * @param {Date} [from=new Date()]
 * @returns {Date} 本轮开始时刻
 */
export function getRoundStart(from = new Date()) {
  const d = new Date(from);
  const hour = d.getHours();
  const minute = d.getMinutes();
  const currentMinutes = hour * 60 + minute;
  if (currentMinutes < 60) {
    d.setDate(d.getDate() - 1);
    d.setHours(7, 0, 0, 0);
    return d;
  }
  if (currentMinutes < 420) {
    d.setHours(1, 0, 0, 0);
    return d;
  }
  d.setHours(7, 0, 0, 0);
  return d;
}

/**
 * 查询本轮尚未完成的任务类型（lastRunAt < roundStart 或无记录）
 * 用于 PM2 重启后恢复未执行/未完成的任务，不修改表结构
 * @param {Date} roundStart - 本轮开始时间（getRoundStart 返回值）
 * @param {string[]} taskTypes - 全部 sync 任务类型，如 ['marketplaces', 'allOrders', ...]
 * @returns {Promise<string[]>} 未完成的 taskType 列表
 */
export async function listTaskTypesNotCompletedInRound(roundStart, taskTypes) {
  if (taskTypes.length === 0) return [];
  const jobNames = taskTypes.map((t) => `sync-job-${t}`);
  const list = await prisma.jobTaskStatus.findMany({
    where: { jobName: { in: jobNames } },
    select: { jobName: true, taskType: true, lastRunAt: true }
  });
  const completedByTask = new Map(list.map((r) => [r.taskType, r.lastRunAt]));
  return taskTypes.filter((taskType) => {
    const lastRun = completedByTask.get(taskType);
    return lastRun == null || lastRun < roundStart;
  });
}

/**
 * 更新或创建任务执行状态（仅更新传入的字段）
 * @param {Object} data - jobName, taskType 必填；lastRunAt, lastStatus, lastError, nextScheduledAt 选填
 */
export async function upsertJobTaskStatus(data) {
  const { jobName, taskType, lastRunAt, lastStatus, lastError, nextScheduledAt } = data;
  const updatePayload = {};
  if (lastRunAt !== undefined) updatePayload.lastRunAt = lastRunAt;
  if (lastStatus !== undefined) updatePayload.lastStatus = lastStatus;
  if (lastError !== undefined) updatePayload.lastError = lastError;
  if (nextScheduledAt !== undefined) updatePayload.nextScheduledAt = nextScheduledAt;

  await prisma.jobTaskStatus.upsert({
    where: { jobName },
    create: {
      jobName,
      taskType,
      ...updatePayload
    },
    update: updatePayload
  });
}

/**
 * 查询任务执行状态列表（供接口使用）
 * @param {Object} options - jobNamePrefix: 按任务名前缀过滤，如 'sync-job'
 * @returns {Promise<Array<{ jobName, taskType, lastRunAt, lastStatus, lastError, nextScheduledAt, updatedAt }>>}
 */
export async function listJobTaskStatus(options = {}) {
  const { jobNamePrefix } = options;
  const where = jobNamePrefix
    ? { jobName: { startsWith: jobNamePrefix } }
    : {};

  const list = await prisma.jobTaskStatus.findMany({
    where,
    orderBy: [{ taskType: 'asc' }],
    select: {
      jobName: true,
      taskType: true,
      lastRunAt: true,
      lastStatus: true,
      lastError: true,
      nextScheduledAt: true,
      updatedAt: true
    }
  });

  return list;
}

export default {
  getNextScheduledRun,
  getRoundStart,
  listTaskTypesNotCompletedInRound,
  upsertJobTaskStatus,
  listJobTaskStatus
};
