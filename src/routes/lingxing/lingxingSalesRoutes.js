import lingxingSalesService from '../../services/lingxing/sales/lingXingSalesService.js';

/**
 * 领星ERP销售路由插件
 * 亚马逊订单相关接口
 */
async function lingxingSalesRoutes(fastify, options) {
  /**
   * 查询亚马逊订单列表
   * POST /api/lingxing/sales/orders/:accountId
   * Body: {
   *   sid: 113,                                    // 店铺id（可选）
   *   sid_list: [113, 112],                       // 店铺id列表，最大长度20（可选）
   *   start_date: "2022-04-18 11:23:47",         // 查询时间开始（必填）
   *   end_date: "2022-05-18 11:23:47",            // 查询时间结束（必填）
   *   date_type: 1,                               // 查询日期类型：1 订购时间，2 订单修改时间，3 平台更新时间，10 发货时间（可选，默认1）
   *   order_status: ["Pending", "Unshipped"],     // 订单状态数组（可选）
   *   sort_desc_by_date_type: 1,                  // 是否按查询日期类型排序：0 否，1 降序，2 升序（可选，默认0）
   *   fulfillment_channel: 1,                     // 配送方式：1 亚马逊订单-AFN，2 自发货-MFN（可选）
   *   offset: 0,                                  // 分页偏移量（可选，默认0）
   *   length: 1000                                // 分页长度（可选，默认1000，上限5000）
   * }
   */
  fastify.post('/orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingSalesService.getAmazonOrderList(accountId, params);

      return {
        success: true,
        message: '获取订单列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取订单列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('最多支持'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取订单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊 Listing（【销售】>【Listing】）
   * POST /api/lingxing/sales/listing/:accountId
   * Body: {
   *   sid: "1,16",                                    // 店铺id，多个逗号分隔（必填）
   *   is_pair: 1,                                     // 1 已配对，2 未配对（可选）
   *   is_delete: 0,                                   // 0 未删除，1 已删除（可选）
   *   pair_update_start_time: "2022-01-01 12:01:21",  // 配对更新时间开始-北京时间（可选）
   *   pair_update_end_time: "2022-10-01 12:01:21",    // 配对更新时间结束-北京时间（可选）
   *   listing_update_start_time: "2021-09-01 01:22:00", // All Listing 更新时间开始-零时区（可选）
   *   listing_update_end_time: "2022-10-01 11:22:00",  // All Listing 更新时间结束-零时区（可选）
   *   search_field: "asin",                           // seller_sku / asin / sku（可选）
   *   search_value: ["asin1", "asin2"],                // 上限10个（可选）
   *   exact_search: 1,                                // 0 模糊 1 精确（可选，默认1）
   *   store_type: 1,                                  // 1 非低价 2 低价（可选）
   *   offset: 0,                                      // 分页偏移（可选）
   *   length: 1000                                    // 分页长度，上限1000（可选）
   * }
   */
  fastify.post('/listing/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingSalesService.getListingList(accountId, params);

      return {
        success: true,
        message: '获取 Listing 列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取 Listing 列表错误:', error);

      if (error.message && (error.message.includes('必填') || error.message.includes('sid'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取 Listing 列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊订单详情
   * POST /api/lingxing/sales/order-detail/:accountId
   * Body: {
   *   order_id: "123-1234567-1234567,789-1234567-1234567"  // 亚马逊订单号，多个使用英文逗号分隔，上限200（必填）
   * }
   */
  fastify.post('/order-detail/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const orderDetails = await lingxingSalesService.getAmazonOrderDetail(accountId, params);

      return {
        success: true,
        message: '获取订单详情成功',
        data: orderDetails,
        total: orderDetails.length
      };
    } catch (error) {
      fastify.log.error('获取订单详情错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('最多支持'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取订单详情失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动同步所有亚马逊订单（自动处理分页）
   * POST /api/lingxing/sales/fetch-all/:accountId
   * Body: {
   *   // 订单列表查询参数（必填）
   *   start_date: "2022-04-18 11:23:47",         // 查询时间开始（必填）
   *   end_date: "2022-05-18 11:23:47",           // 查询时间结束（必填）
   *   sid: 113,                                  // 店铺id（可选）
   *   sid_list: [113, 112],                      // 店铺id列表（可选）
   *   date_type: 1,                              // 查询日期类型（可选）
   *   order_status: ["Pending", "Unshipped"],    // 订单状态数组（可选）
   *   // 其他查询参数...
   *   
   *   // 选项（可选）
   *   options: {
   *     fetchDetails: false,           // 是否自动批量查询订单详情
   *     pageSize: 1000,                // 每页大小（最大5000）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒）
   *     batchSize: 200,                // 批量查询详情时每批数量（最大200）
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
      const result = await lingxingSalesService.fetchAllAmazonOrders(
        accountId,
        listParams,
        finalOptions
      );

      return {
        success: true,
        message: '自动同步所有亚马逊订单成功',
        data: {
          orders: result.orders,
          orderDetails: result.orderDetails
        },
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动同步所有亚马逊订单错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('最多支持'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动同步所有亚马逊订单失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingSalesRoutes;

