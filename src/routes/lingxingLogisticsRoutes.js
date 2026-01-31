import lingxingLogisticsService from '../services/lingxing/logistics/lingXingLogisticsService.js';

/**
 * 领星ERP物流路由插件
 * 头程物流相关接口
 */
async function lingxingLogisticsRoutes(fastify, options) {
  /**
   * 查询头程物流渠道列表
   * POST /api/lingxing/logistics/channels/:accountId
   * Body: {
   *   offset: 0,    // 分页偏移量（必填）
   *   length: 20    // 分页长度（必填）
   * }
   * 支持查询【物流】>【头程物流】>【物流渠道】数据
   */
  fastify.post('/channels/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingLogisticsService.getChannelList(accountId, params);

      return {
        success: true,
        message: '获取物流渠道列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取物流渠道列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取物流渠道列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有物流渠道列表
   * POST /api/lingxing/logistics/fetch-all-channels/:accountId
   * Body: {
   *   options: {
   *     pageSize: 500,               // 每页大小（可选，默认500）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-channels/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { options = {} } = request.body || {};

    try {
      const result = await lingxingLogisticsService.fetchAllChannels(accountId, options);

      return {
        success: true,
        message: '自动拉取所有物流渠道列表成功',
        data: result.channels,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有物流渠道列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有物流渠道列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询头程物流商列表
   * POST /api/lingxing/logistics/head-providers/:accountId
   * Body: {
   *   search: {
   *     length: 20,                    // 分页长度（必填）
   *     page: 1,                       // 页码，从1开始（必填）
   *     enabled: 1,                    // 启用状态（可选）：0-禁用, 1-启用，默认启用
   *     isAuth: 1,                     // 是否api对接（可选）：0-否, 1-是，默认是
   *     payMethod: 1,                  // 结算方式（可选）：1-现结, 2-月结，默认现结
   *     searchField: "name",           // 搜索字段（可选）：code 代码，name 物流商，默认物流商
   *     searchValue: "顺丰"            // 搜索值（可选）：用于模糊搜索物流商名称、编码等
   *   }
   * }
   * 支持查询【物流】>【头程物流商】数据，默认返回已启用现结api对接的物流商
   */
  fastify.post('/head-providers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingLogisticsService.getHeadLogisticsProviderList(accountId, params);

      return {
        success: true,
        message: '获取物流商列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取物流商列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取物流商列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有头程物流商列表
   * POST /api/lingxing/logistics/fetch-all-head-providers/:accountId
   * Body: {
   *   // 搜索参数（可选）
   *   enabled: 1,                    // 启用状态（可选）
   *   isAuth: 1,                     // 是否api对接（可选）
   *   payMethod: 1,                  // 结算方式（可选）
   *   searchField: "name",           // 搜索字段（可选）
   *   searchValue: "顺丰",            // 搜索值（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,                 // 每页大小（可选，默认20）
   *     delayBetweenPages: 500,       // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-head-providers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...searchParams } = body;

    try {
      const result = await lingxingLogisticsService.fetchAllHeadLogisticsProviders(accountId, searchParams, options || {});

      return {
        success: true,
        message: '自动拉取所有物流商列表成功',
        data: result.providers,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有物流商列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有物流商列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询已启用的自发货物流方式列表
   * POST /api/lingxing/logistics/used-logistics-types/:accountId
   * Body: {
   *   provider_type: 0,              // 物流商类型（必填）：0 API物流，1 自定义物流，2 海外仓物流，4 平台物流
   *   page: 1,                        // 分页页码（可选，默认1）
   *   length: 20                      // 分页长度（可选，默认20）
   * }
   * 支持查询【物流】>【物流管理】当中的 API 物流、三方仓物流、平台物流、自定义物流列表
   */
  fastify.post('/used-logistics-types/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingLogisticsService.getUsedLogisticsTypeList(accountId, params);

      return {
        success: true,
        message: '获取物流方式列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取物流方式列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取物流方式列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有已启用的自发货物流方式列表
   * POST /api/lingxing/logistics/fetch-all-used-logistics-types/:accountId
   * Body: {
   *   provider_type: 0,              // 物流商类型（必填）：0 API物流，1 自定义物流，2 海外仓物流，4 平台物流
   *   options: {
   *     pageSize: 20,                 // 每页大小（可选，默认20）
   *     delayBetweenPages: 500,       // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-used-logistics-types/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { provider_type, options } = body;

    if (provider_type === undefined) {
      return reply.code(400).send({
        success: false,
        message: 'provider_type 为必填参数'
      });
    }

    try {
      const result = await lingxingLogisticsService.fetchAllUsedLogisticsTypes(accountId, provider_type, options || {});

      return {
        success: true,
        message: '自动拉取所有物流方式列表成功',
        data: result.logisticsTypes,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有物流方式列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有物流方式列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询运输方式列表
   * POST /api/lingxing/logistics/transport-methods/:accountId
   * Body: {}  // 不需要参数
   * 支持查询【设置】>【业务配置】>【物流】当中的运输方式
   */
  fastify.post('/transport-methods/:accountId', async (request, reply) => {
    const { accountId } = request.params;

    try {
      const result = await lingxingLogisticsService.getTransportMethodList(accountId);

      return {
        success: true,
        message: '获取运输方式列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取运输方式列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取运输方式列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有运输方式列表
   * POST /api/lingxing/logistics/fetch-all-transport-methods/:accountId
   * Body: {}  // 不需要参数
   * 注意：这个接口不需要分页参数，直接返回所有数据
   */
  fastify.post('/fetch-all-transport-methods/:accountId', async (request, reply) => {
    const { accountId } = request.params;

    try {
      const result = await lingxingLogisticsService.fetchAllTransportMethods(accountId);

      return {
        success: true,
        message: '自动拉取所有运输方式列表成功',
        data: result.transportMethods,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有运输方式列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有运输方式列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingLogisticsRoutes;

