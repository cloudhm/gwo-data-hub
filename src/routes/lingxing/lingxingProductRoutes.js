import lingxingProductService from '../../services/lingxing/products/lingxingProductService.js';

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
   * POST /api/lingxing/products/local-products/info/:accountId
   * Body: {
   *   id: 10001,           // 产品id（三选一必填）
   *   sku: "ceshi001",     // 产品SKU（三选一必填）
   *   sku_identifier: "ceshi001"  // SKU识别码（三选一必填）
   * }
   * 对应系统【产品】>【产品管理】数据
   */
  fastify.post('/local-products/info/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const productInfo = await lingxingProductService.getLocalProductInfo(accountId, params);

      if (!productInfo) {
        return reply.code(404).send({
          success: false,
          message: '未找到对应的产品信息'
        });
      }

      return {
        success: true,
        message: '获取产品详细信息成功',
        data: productInfo
      };
    } catch (error) {
      fastify.log.error('获取产品详细信息错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('三选一必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误',
          code: 'C60610'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取产品详细信息失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 批量查询本地产品详细信息
   * POST /api/lingxing/products/local-products/batch-info/:accountId
   * Body: {
   *   productIds: ["10001", "10002"],        // 产品id数组，上限100个（三选一必填）
   *   skus: ["ceshi001", "测试002"],         // 产品SKU数组，上限100个（三选一必填）
   *   sku_identifiers: ["ceshi001", "测试002"]  // SKU识别码数组，上限100个（三选一必填）
   * }
   * 对应系统【产品】>【产品管理】数据
   */
  fastify.post('/local-products/batch-info/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const productInfoList = await lingxingProductService.batchGetLocalProductInfo(accountId, params);

      return {
        success: true,
        message: '批量获取产品详细信息成功',
        data: productInfoList,
        total: productInfoList.length
      };
    } catch (error) {
      fastify.log.error('批量获取产品详细信息错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('三选一必填') || error.message.includes('数组长度不能超过'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误',
          code: 'C60610'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '批量获取产品详细信息失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询本地产品列表并自动批量查询产品详情
   * POST /api/lingxing/products/local-products/with-details/:accountId
   * Body: {
   *   // 产品列表查询参数（同 local-products 接口）
   *   offset: 0,
   *   length: 1000,
   *   update_time_start: 1719799582,
   *   update_time_end: 1721873182,
   *   // ... 其他列表查询参数
   *   
   *   // 批量查询选项（可选）
   *   options: {
   *     useCache: false,              // 是否使用缓存
   *     batchSize: 100,                // 每批查询数量（最大100）
   *     delayBetweenBatches: 1000,     // 批次间延迟（毫秒）
   *     maxRetries: 3                  // 每批最大重试次数
   *   }
   * }
   * Query: ?useCache=false (可选，默认false)
   */
  fastify.post('/local-products/with-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'false' } = request.query;
    const body = request.body || {};
    
    // 分离列表查询参数和选项
    const { options, ...listParams } = body;
    
    // 合并 useCache 选项
    const finalOptions = {
      useCache: useCache === 'true' || (options && options.useCache === true),
      ...(options || {})
    };

    try {
      const result = await lingxingProductService.getLocalProductListWithDetails(
        accountId,
        listParams,
        finalOptions
      );

      return {
        success: true,
        message: '查询产品列表并获取详情成功',
        data: {
          products: result.products,
          productDetails: result.productDetails
        },
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('查询产品列表并获取详情错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '查询产品列表并获取详情失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有商品列表（自动处理分页）
   * POST /api/lingxing/products/fetch-all/:accountId
   * Body: {
   *   // 产品列表查询参数（可选）
   *   update_time_start: 1719799582,
   *   update_time_end: 1721873182,
   *   create_time_start: 1606790813,
   *   create_time_end: 1609343999,
   *   sku_list: ["ceshi001","lingcui001"],
   *   sku_identifier_list: ["ceshi001","lingcui001"],
   *   
   *   // 选项（可选）
   *   options: {
   *     fetchDetails: false,           // 是否自动批量查询产品详情
   *     pageSize: 1000,                // 每页大小（最大1000）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒）
   *     batchSize: 100,                // 批量查询详情时每批数量（最大100）
   *     delayBetweenBatches: 1000,     // 批量查询批次之间延迟（毫秒）
   *     maxRetries: 3                  // 每批查询的最大重试次数
   *   }
   * }
   */
  fastify.post('/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    
    // 分离列表查询参数和选项
    const { options, ...listParams } = body;
    
    const finalOptions = options || {};

    try {
      const result = await lingxingProductService.fetchAllLocalProducts(
        accountId,
        listParams,
        finalOptions
      );

      return {
        success: true,
        message: '自动拉取所有商品列表成功',
        data: {
          products: result.products,
          productDetails: result.productDetails
        },
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有商品列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有商品列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingProductRoutes;

