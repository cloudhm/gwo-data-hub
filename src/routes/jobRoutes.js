import jobTaskStatusService from '../services/jobTaskStatusService.js';

/**
 * 定时任务状态相关接口（方案二：按 task 维度查询执行情况）
 */
async function jobRoutes(fastify) {
  /**
   * GET /task-status
   * 查询各 job 执行状态：最近执行时间、是否异常、异常原因、下次计划执行时间
   * Query: jobNamePrefix - 可选，按任务名前缀过滤，如 sync-job
   */
  fastify.get('/task-status', async (request, reply) => {
    try {
      const { jobNamePrefix } = request.query || {};
      const list = await jobTaskStatusService.listJobTaskStatus({
        jobNamePrefix: jobNamePrefix || 'sync-job'
      });
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
