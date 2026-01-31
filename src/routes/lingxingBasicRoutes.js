import lingxingBasicDataService from '../services/lingxing/basic/lingxingBasicDataService.js';

/**
 * 领星ERP基础数据路由插件
 */
async function lingxingBasicRoutes(fastify, options) {
  /**
   * 查询亚马逊市场列表
   * GET /api/lingxing/basic/marketplaces/:accountId
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.get('/marketplaces/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true' } = request.query;

    try {
      const marketplaces = await lingxingBasicDataService.getAllMarketplaces(
        accountId,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取市场列表成功',
        data: {
          count: marketplaces.length,
          marketplaces: marketplaces
        }
      };
    } catch (error) {
      fastify.log.error('获取市场列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取市场列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 根据mid获取市场信息
   * GET /api/lingxing/basic/marketplaces/mid/:mid
   */
  fastify.get('/marketplaces/mid/:mid', async (request, reply) => {
    const { mid } = request.params;

    try {
      const marketplace = await lingxingBasicDataService.getMarketplaceByMid(parseInt(mid));

      if (!marketplace) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的市场信息'
        });
      }

      return {
        success: true,
        data: marketplace
      };
    } catch (error) {
      fastify.log.error('获取市场信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取市场信息失败'
      });
    }
  });

  /**
   * 根据marketplace_id获取市场信息
   * GET /api/lingxing/basic/marketplaces/marketplace-id/:marketplaceId
   */
  fastify.get('/marketplaces/marketplace-id/:marketplaceId', async (request, reply) => {
    const { marketplaceId } = request.params;

    try {
      const marketplace = await lingxingBasicDataService.getMarketplaceByMarketplaceId(marketplaceId);

      if (!marketplace) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的市场信息'
        });
      }

      return {
        success: true,
        data: marketplace
      };
    } catch (error) {
      fastify.log.error('获取市场信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取市场信息失败'
      });
    }
  });

  /**
   * 查询亚马逊国家下地区列表
   * POST /api/lingxing/basic/world-states/:accountId
   * Body: { countryCode: "US" }
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.post('/world-states/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { countryCode } = request.body;
    const { useCache = 'true' } = request.query;

    if (!countryCode) {
      return reply.code(400).send({
        success: false,
        message: '请提供国家code'
      });
    }

    try {
      const states = await lingxingBasicDataService.getWorldStates(
        accountId,
        countryCode,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取地区列表成功',
        data: {
          count: states.length,
          total: states.length,
          states: states
        }
      };
    } catch (error) {
      fastify.log.error('获取地区列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取地区列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 根据国家code和地区code获取地区信息
   * GET /api/lingxing/basic/world-states/:countryCode/:stateCode
   */
  fastify.get('/world-states/:countryCode/:stateCode', async (request, reply) => {
    const { countryCode, stateCode } = request.params;

    try {
      const state = await lingxingBasicDataService.getWorldStateByCode(countryCode, stateCode);

      if (!state) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的地区信息'
        });
      }

      return {
        success: true,
        data: state
      };
    } catch (error) {
      fastify.log.error('获取地区信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取地区信息失败'
      });
    }
  });

  /**
   * 查询亚马逊店铺列表
   * GET /api/lingxing/basic/sellers/:accountId
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.get('/sellers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true' } = request.query;

    try {
      const sellers = await lingxingBasicDataService.getSellerLists(
        accountId,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取店铺列表成功',
        data: {
          count: sellers.length,
          total: sellers.length,
          sellers: sellers
        }
      };
    } catch (error) {
      fastify.log.error('获取店铺列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取店铺列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 根据sid获取店铺信息
   * GET /api/lingxing/basic/sellers/sid/:sid
   */
  fastify.get('/sellers/sid/:sid', async (request, reply) => {
    const { sid } = request.params;

    try {
      const seller = await lingxingBasicDataService.getSellerBySid(parseInt(sid));

      if (!seller) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的店铺信息'
        });
      }

      return {
        success: true,
        data: seller
      };
    } catch (error) {
      fastify.log.error('获取店铺信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取店铺信息失败'
      });
    }
  });

  /**
   * 根据状态筛选店铺列表
   * GET /api/lingxing/basic/sellers/:accountId/status/:status
   * status: 0 停止同步，1 正常，2 授权异常，3 欠费停服
   */
  fastify.get('/sellers/:accountId/status/:status', async (request, reply) => {
    const { accountId, status } = request.params;

    try {
      const sellers = await lingxingBasicDataService.getSellersByStatus(accountId, parseInt(status));

      return {
        success: true,
        message: '获取店铺列表成功',
        data: {
          count: sellers.length,
          total: sellers.length,
          sellers: sellers
        }
      };
    } catch (error) {
      fastify.log.error('获取店铺列表错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取店铺列表失败'
      });
    }
  });

  /**
   * 查询亚马逊概念店铺列表
   * GET /api/lingxing/basic/concept-sellers/:accountId
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.get('/concept-sellers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true' } = request.query;

    try {
      const conceptSellers = await lingxingBasicDataService.getConceptSellerLists(
        accountId,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取概念店铺列表成功',
        data: {
          count: conceptSellers.length,
          total: conceptSellers.length,
          conceptSellers: conceptSellers
        }
      };
    } catch (error) {
      fastify.log.error('获取概念店铺列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取概念店铺列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 根据conceptId获取概念店铺信息
   * GET /api/lingxing/basic/concept-sellers/concept-id/:conceptId
   */
  fastify.get('/concept-sellers/concept-id/:conceptId', async (request, reply) => {
    const { conceptId } = request.params;

    try {
      const conceptSeller = await lingxingBasicDataService.getConceptSellerByConceptId(conceptId);

      if (!conceptSeller) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的概念店铺信息'
        });
      }

      return {
        success: true,
        data: conceptSeller
      };
    } catch (error) {
      fastify.log.error('获取概念店铺信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取概念店铺信息失败'
      });
    }
  });

  /**
   * 根据状态筛选概念店铺列表
   * GET /api/lingxing/basic/concept-sellers/:accountId/status/:status
   * status: 1 启用，2 禁用
   */
  fastify.get('/concept-sellers/:accountId/status/:status', async (request, reply) => {
    const { accountId, status } = request.params;

    try {
      const conceptSellers = await lingxingBasicDataService.getConceptSellersByStatus(accountId, parseInt(status));

      return {
        success: true,
        message: '获取概念店铺列表成功',
        data: {
          count: conceptSellers.length,
          total: conceptSellers.length,
          conceptSellers: conceptSellers
        }
      };
    } catch (error) {
      fastify.log.error('获取概念店铺列表错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取概念店铺列表失败'
      });
    }
  });

  /**
   * 查询汇率
   * POST /api/lingxing/basic/currency-rates/:accountId
   * Body: { date: "2021-08" }
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.post('/currency-rates/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { date } = request.body;
    const { useCache = 'true' } = request.query;

    if (!date) {
      return reply.code(400).send({
        success: false,
        message: '请在请求体中提供汇率月份 "date"'
      });
    }

    try {
      const rates = await lingxingBasicDataService.getCurrencyRates(
        accountId,
        date,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取汇率列表成功',
        data: {
          count: rates.length,
          rates: rates
        }
      };
    } catch (error) {
      fastify.log.error('获取汇率列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取汇率列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询ERP用户信息列表
   * GET /api/lingxing/basic/account-users/:accountId
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.get('/account-users/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true' } = request.query;

    try {
      const users = await lingxingBasicDataService.getAccountUsers(
        accountId,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取用户列表成功',
        data: {
          count: users.length,
          total: users.length,
          users: users
        }
      };
    } catch (error) {
      fastify.log.error('获取用户列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取用户列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 根据uid获取用户信息
   * GET /api/lingxing/basic/account-users/uid/:uid
   */
  fastify.get('/account-users/uid/:uid', async (request, reply) => {
    const { uid } = request.params;

    try {
      const user = await lingxingBasicDataService.getAccountUserByUid(parseInt(uid));

      if (!user) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的用户信息'
        });
      }

      return {
        success: true,
        data: user
      };
    } catch (error) {
      fastify.log.error('获取用户信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取用户信息失败'
      });
    }
  });

  /**
   * 根据状态筛选用户列表
   * GET /api/lingxing/basic/account-users/:accountId/status/:status
   * status: 0 禁用，1 正常
   */
  fastify.get('/account-users/:accountId/status/:status', async (request, reply) => {
    const { accountId, status } = request.params;

    try {
      const users = await lingxingBasicDataService.getAccountUsersByStatus(accountId, parseInt(status));

      return {
        success: true,
        message: '获取用户列表成功',
        data: {
          count: users.length,
          total: users.length,
          users: users
        }
      };
    } catch (error) {
      fastify.log.error('获取用户列表错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取用户列表失败'
      });
    }
  });
}

export default lingxingBasicRoutes;
