import lingxingWarehouseService from '../services/lingxing/warehouse/lingXingWarehouseService.js';

/**
 * 领星ERP仓库路由插件
 * 仓库相关接口
 */
async function lingxingWarehouseRoutes(fastify, options) {
  /**
   * 查询仓库列表
   * POST /api/lingxing/warehouse/list/:accountId
   * Body: {
   *   type: 1,              // 仓库类型（可选，默认1）：1 本地仓，3 海外仓，4 亚马逊平台仓，6 AWD仓
   *   sub_type: 2,          // 海外仓子类型（可选，只在type=3生效）：1 无API海外仓，2 有API海外仓
   *   is_delete: 0,         // 是否删除（可选，默认0）：0 未删除，1 已删除
   *   offset: 0,            // 分页偏移量（可选，默认0）
   *   length: 1000           // 分页长度（可选，默认1000）
   * }
   * 支持查询【设置】>【仓库设置】仓库列表
   */
  fastify.post('/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getWarehouseList(accountId, params);

      return {
        success: true,
        message: '获取仓库列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取仓库列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取仓库列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有仓库列表
   * POST /api/lingxing/warehouse/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   type: 1,              // 仓库类型（可选）
   *   sub_type: 2,          // 海外仓子类型（可选）
   *   is_delete: 0,         // 是否删除（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000）
   *     delayBetweenPages: 500,  // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllWarehouses(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有仓库列表成功',
        data: result.warehouses,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有仓库列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有仓库列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询本地仓位列表
   * PO