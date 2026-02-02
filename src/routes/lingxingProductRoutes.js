import lingxingProductService from '../services/lingxing/products/lingxingProductService.js';

/**
 * 领星ERP产品路由插件
 */
async function lingxingProductRoutes(fastify, options) {
  /**
   * 查询本地产品列表
   * POST /api/lingxing/products/local-products/:accountId
   * Body: {
   *   offset: 0,
   *   length: 1000,
   *   update_time_start: 1719799582,
   *   update_time_end: 1721873182,
   *   create_time_start: 1606790813,
   *   create_time_end: 1609343999,
   *   sku_list: ["ceshi001","lingcui001"],
   *   sku_identifier_list: ["ceshi001","lingcui001"]
   * }
   * Query: ?useCache=false (可选，默认false)
   */
  fastify.post('/local-products/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'false' } = request.query;
    const params = request.body || {};

    try {
      const result = await lingxingProductService.getLocalProductList(
        accountId,
        params,
        useCache === 'true'
      );

      return {
        success: true,
        message: '获取产品列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取产品列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取产品列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 根据产品ID获取产品信息
   * GET /api/lingxing/products/local-products/id/:productId
   */
  fastify.get('/local-products/id/:productId', async (request, reply) => {
    const { productId } = request.params;

    try {
      const product = await lingxingProductService.getLocalProductById(parseInt(productId));

      if (!product) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的产品信息'
        });
      }

      return {
        success: true,
        data: product
      };
    } catch (error) {
      fastify.log.error('获取产品信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取产品信息失败'
      });
    }
  });

  /**
   * 根据SKU获取产品信息
   * GET /api/lingxing/products/local-products/sku/:accountId/:sku
   */
  fastify.get('/local-products/sku/:accountId/:sku', async (request, reply) => {
    const { accountId, sku } = request.params;

    try {
      const product = await lingxingProductService.getLocalProductBySku(accountId, sku);

      if (!product) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的产品信息'
        });
      }

      return {
        success: true,
        data: product
      };
    } catch (error) {
      fastify.log.error('获取产品信息错误:', error);
      reply.code(500).send({
        success: false,
        message: error.message || '获取产品信息失败'
      });
    }
  });

  /**
   * 查询本地产品详细信息