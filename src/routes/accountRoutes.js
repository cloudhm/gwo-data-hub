import prisma from '../config/database.js';

/**
 * 账户管理路由插件
 */
async function accountRoutes(fastify, options) {
  /**
   * 创建亚马逊店铺
   * POST /api/accounts/amazon
   */
  fastify.post('/amazon', async (request, reply) => {
    const { storeName, storeId, region, accessKey, secretKey, refreshToken } = request.body;

    if (!storeName || !storeId || !region) {
      return reply.code(400).send({
        success: false,
        message: '请提供店铺名称、店铺ID和区域'
      });
    }

    const store = await prisma.amazonStore.create({
      data: {
        storeName,
        storeId,
        region,
        accessKey,
        secretKey,
        refreshToken
      }
    });

    return {
      success: true,
      message: '亚马逊店铺创建成功',
      data: store
    };
  });

  /**
   * 创建领星ERP账户
   * POST /api/accounts/lingxing
   * 一个账户有一个APP ID和APP Secret，可以绑定多个亚马逊店铺
   */
  fastify.post('/lingxing', async (request, reply) => {
    const { name, appId, appSecret, description } = request.body;

    if (!name || !appId || !appSecret) {
      return reply.code(400).send({
        success: false,
        message: '请提供账户名称、APP ID和APP Secret'
      });
    }

    const account = await prisma.lingXingAccount.create({
      data: {
        name,
        appId, // APP ID
        appSecret, // APP Secret (用于签名和获取token)
        description
      }
    });

    return {
      success: true,
      message: '领星ERP账户创建成功',
      data: account
    };
  });

  /**
   * 获取所有亚马逊店铺
   * GET /api/accounts/amazon
   */
  fastify.get('/amazon', async (request, reply) => {
    const stores = await prisma.amazonStore.findMany({
      orderBy: { createdAt: 'desc' }
    });

    return {
      success: true,
      data: stores
    };
  });

  /**
   * 获取所有领星ERP账户
   * GET /api/accounts/lingxing
   */
  fastify.get('/lingxing', async (request, reply) => {
    const accounts = await prisma.lingXingAccount.findMany({
      include: {
        amazonSellers: true // 包含该账户下的所有店铺
      },
      orderBy: { createdAt: 'desc' }
    });

    return {
      success: true,
      data: accounts
    };
  });

  /**
   * 获取同步日志
   * GET /api/accounts/sync-logs
   */
  fastify.get('/sync-logs', async (request, reply) => {
    const { source, storeId, limit = 50 } = request.query;

    const where = {};
    if (source) where.source = source;
    if (storeId) where.storeId = storeId;

    const logs = await prisma.syncLog.findMany({
      where,
      orderBy: { createdAt: 'desc' },
      take: parseInt(limit)
    });

    return {
      success: true,
      data: logs
    };
  });
}

export default accountRoutes;

