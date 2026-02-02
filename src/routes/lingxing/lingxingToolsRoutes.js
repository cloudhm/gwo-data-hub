import lingxingToolsService from '../../services/lingxing/tools/lingXingToolsService.js';

/**
 * 领星ERP工具路由插件
 * 工具相关接口
 */
async function lingxingToolsRoutes(fastify, options) {
  /**
   * 查询关键词列表
   * POST /api/lingxing/tools/keywords/:accountId
   * Body: {
   *   mid: 1,                    // 国家id（可选）
   *   start_date: "2024-08-01", // 开始日期，格式：Y-m-d（可选）
   *   end_date: "2024-08-01",   // 结束日期，格式：Y-m-d（可选）
   *   offset: 0,                // 分页偏移量（必填，默认0）
   *   length: 20                 // 分页长度（必填，默认20，最大2000）
   * }
   * 支持查询关键词排名数据
   */
  fastify.post('/keywords/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingToolsService.getKeywordList(accountId, params);

      return {
        success: true,
        message: '获取关键词列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取关键词列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('offset') || error.message.includes('length'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取关键词列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有关键词列表
   * POST /api/lingxing/tools/fetch-all-keywords/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   mid: 1,                    // 国家id（可选）
   *   start_date: "2024-08-01", // 开始日期（可选）
   *   end_date: "2024-08-01",   // 结束日期（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 2000,                 // 每页大小（可选，默认2000，最大2000）
   *     delayBetweenPages: 500,         // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-keywords/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingToolsService.fetchAllKeywords(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有关键词列表成功',
        data: result.keywordList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有关键词列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有关键词列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingToolsRoutes;

