import lingxingService from '../../services/lingxing/lingxingService.js';

/**
 * 领星ERP路由插件
 */
async function lingxingRoutes(fastify, options) {
  /**
   * 拉取领星ERP订单
   * POST /api/lingxing/orders/:accountId
   * Body: { startDate, endDate }
   */
  fastify.post('/orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { startDate, endDate } = request.body;

    if (!startDate || !endDate) {
      return reply.code(400).send({
        success: false,
        message: '请提供开始日期和结束日期'
      });
    }

    const orders = await lingxingService.fetchOrders(accountId, startDate, endDate);

    return {
      success: true,
      message: '订单拉取成功',
      data: {
        count: orders.length,
        orders: orders
      }
    };
  });

  /**
   * 拉取领星ERP产品
   * POST /api/lingxing/products/:accountId
   */
  fastify.post('/products/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const products = await lingxingService.fetchProducts(accountId);

    return {
      success: true,
      message: '产品拉取成功',
      data: {
        count: products.length,
        products: products
      }
    };
  });

  /**
   * 拉取领星ERP库存
   * POST /api/lingxing/inventory/:accountId
   */
  fastify.post('/inventory/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const inventories = await lingxingService.fetchInventory(accountId);

    return {
      success: true,
      message: '库存拉取成功',
      data: {
        count: inventories.length,
        inventories: inventories
      }
    };
  });
}

export default lingxingRoutes;

