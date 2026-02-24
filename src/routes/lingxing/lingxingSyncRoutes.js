import lingXingUnifiedSyncService from '../../services/lingxing/sync/lingXingUnifiedSyncService.js';

/**
 * 领星统一同步路由
 * - 不要求传 accountId，服务内从数据表遍历所有启用账户
 * - 仅对支持增量拉取的任务使用增量方法
 */
async function lingxingSyncRoutes(fastify, options) {
  /**
   * 获取所有支持增量拉取的任务类型
   * GET /api/lingxing/sync/task-types
   */
  fastify.get('/task-types', async (request, reply) => {
    const list = lingXingUnifiedSyncService.getSupportedIncrementalTaskTypes();
    return { success: true, data: list };
  });

  /**
   * 获取当前启用的账户列表（用于同步的账户）
   * GET /api/lingxing/sync/accounts
   */
  fastify.get('/accounts', async (request, reply) => {
    const accounts = await lingXingUnifiedSyncService.getActiveAccounts();
    return { success: true, data: accounts };
  });

  /**
   * 按任务类型执行增量同步（遍历所有启用账户）
   * POST /api/lingxing/sync/run/:taskType
   * Body: { endDate?, defaultLookbackDays?, timezone?, ... } 透传给各服务
   */
  fastify.post('/run/:taskType', async (request, reply) => {
    const { taskType } = request.params;
    const options = request.body || {};
    try {
      const result = await lingXingUnifiedSyncService.runIncrementalSyncByTaskType(taskType, options);
      return { success: true, data: result };
    } catch (err) {
      return reply.code(400).send({
        success: false,
        message: err?.message || '执行增量同步失败',
        taskType
      });
    }
  });

  /**
   * 执行全部支持增量拉取的任务（按任务类型依次执行，每种类型遍历所有启用账户）
   * POST /api/lingxing/sync/run-all
   * Body: { taskTypes?: string[], endDate?, defaultLookbackDays?, ... }
   * - taskTypes: 不传则执行全部；传则只执行列表中的任务类型
   */
  fastify.post('/run-all', async (request, reply) => {
    const body = request.body || {};
    const { taskTypes, ...options } = body;
    try {
      const result = await lingXingUnifiedSyncService.runAllIncrementalSync(options, taskTypes);
      return { success: true, data: result };
    } catch (err) {
      return reply.code(500).send({
        success: false,
        message: err?.message || '执行全量增量同步失败'
      });
    }
  });
}

export default lingxingSyncRoutes;
