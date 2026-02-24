import jobTaskStatusService from '../services/jobTaskStatusService.js';
import { SYNC_TASKS } from '../workers/jobs/syncJob.js';

/**
 * 定时任务状态相关接口（方案二：按 task 维度查询执行情况）
 */
async function jobRoutes(fastify) {
  /**
   * GET /task-status
   * 查询各 job 执行状态：最近执行时间、是否异常、异常原因、下次计划执行时间
   * 始终返回完整任务列表（27 个 sync-job），未执行过的项用默认值 + 计算出的 nextScheduledAt
   * Query: jobNamePrefix - 可选，默认 sync-job
   */
  fastify.get('/task-status', async (request, reply) => {
    try {
      const { jobNamePrefix } = request.query || {};
      const prefix = jobNamePrefix || 'sync-job';
      const dbList = await jobTaskStatusService.listJobTaskStatus({ jobNamePrefix: prefix });
      const dbByJobName = Object.fromEntries(dbList.map((row) => [row.jobName, row]));

      const nextRun = jobTaskStatusService.getNextScheduledRun(new Date());
      const defaultItem = (jobName, taskType) => ({
        jobName,
        taskType,
        lastRunAt: null,
        lastStatus: null,
        lastError: null,
        nextScheduledAt: nextRun,
        updatedAt: null
      });

      const list =
        prefix === 'sync-job'
          ? SYNC_TASKS.map(([taskType]) => {
              const jobName = `sync-job-${taskType}`;
              const row = dbByJobName[jobName];
              return row
                ? { ...row, nextScheduledAt: row.nextScheduledAt ?? nextRun }
                : defaultItem(jobName, taskType);
            })
          : dbList;

      return reply.send({
        success: true,
        data: list
      });
    } catch (error) {
      fastify.log.error('查询任务状态失败:', error);
      return reply.code(500).send({
        success: false,
        message: error?.message || '查询任务状态失败'
      });
    }
  });
}

export default jobRoutes;
