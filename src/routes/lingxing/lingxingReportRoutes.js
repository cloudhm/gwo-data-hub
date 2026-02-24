import lingxingReportService from '../../services/lingxing/reports/lingXingReportService.js';

/**
 * 领星ERP报表路由插件
 * 销量、订单量、销售额报表相关接口
 */
async function lingxingReportRoutes(fastify, options) {
  /**
   * 查询销量、订单量、销售额报表（按ASIN或MSKU）
   * POST /api/lingxing/reports/sales/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id（可选，不传则查询所有店铺）
   *   event_date: "2024-08-05",    // 报表时间【站点时间】，格式：Y-m-d（必填）
   *   asin_type: 1,                // 查询维度：1=asin, 2=msku（可选，默认1）
   *   type: 1,                     // 类型：1=销售额, 2=销量, 3=订单量（可选，默认1）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                 // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/sales/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingReportService.getSalesReport(accountId, params);

      return {
        success: true,
        message: '获取销量报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取销量报表错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('格式错误'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取销量报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 批量查询多天的销量报表（自动处理分页）
   * POST /api/lingxing/reports/sales-range/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id（可选，不传则查询所有店铺）
   *   start_date: "2024-08-01",    // 开始日期，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 结束日期，格式：Y-m-d（必填）
   *   asin_type: 1,                // 查询维度：1=asin, 2=msku（可选，不传则遍历1和2）
   *   type: 1,                     // 类型：1=销售额, 2=销量, 3=订单量（可选，不传则遍历1、2、3）
   *   pageSize: 1000,              // 每页大小（可选，默认1000）
   *   delayBetweenDays: 500,       // 每天之间的延迟时间（毫秒，可选，默认500）
   *   delayBetweenShops: 1000,     // 店铺之间的延迟时间（毫秒，可选，默认1000）
   *   delayBetweenTypes: 500       // 类型组合之间的延迟时间（毫秒，可选，默认500）
   * }
   */
  fastify.post('/sales-range/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingReportService.getSalesReportByDateRange(accountId, params);

      return {
        success: true,
        message: '批量查询销量报表成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('批量查询销量报表错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('格式错误'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '批量查询销量报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询产品表现
   * POST /api/lingxing/reports/product-performance/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（必填）
   *   length: 20,                   // 分页长度，最大10000（必填）
   *   sort_field: "volume",         // 排序字段（必填，默认volume）
   *   sort_type: "desc",            // 排序方式：desc/asc（必填，默认desc）
   *   search_field: "asin",         // 搜索字段（可选）
   *   search_value: ["B085M7NH7K"], // 搜索值，最多批量搜索50个（可选）
   *   mid: 1,                       // 站点id（可选）
   *   sid: [1,109],                 // 店铺id，单店铺传字符串，多店铺传数组，上限200（必填）
   *   start_date: "2024-08-01",     // 开始日期，格式：YYYY-MM-DD（必填，建议与end_date相同）
   *   end_date: "2024-08-01",       // 结束日期，格式：YYYY-MM-DD（必填，建议与start_date相同）
   *   summary_field: "asin",        // 汇总行维度：asin/parent_asin/msku/sku（必填）
   *   currency_code: "CNY",         // 货币类型（可选）
   *   is_recently_enum: true,       // 是否仅查询活跃商品（可选，默认true）
   *   purchase_status: 0            // 退货退款统计方式（可选，默认0）
   * }
   */
  fastify.post('/product-performance/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingReportService.getProductPerformance(accountId, params);

      return {
        success: true,
        message: '获取产品表现成功',
        data: result.list,
        total: result.total,
        chain_start_date: result.chain_start_date,
        chain_end_date: result.chain_end_date,
        available_inventory_formula_zh: result.available_inventory_formula_zh
      };
    } catch (error) {
      fastify.log.error('获取产品表现错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('格式错误'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取产品表现失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 批量查询产品表现（自动处理分页和限流）
   * POST /api/lingxing/reports/product-performance-all/:accountId
   * Body: {
   *   // 查询参数（同 product-performance，但 offset 和 length 可选）
   *   sid: [1,109],                 // 店铺id，单店铺传字符串，多店铺传数组（可选，不传则查询所有店铺）
   *   start_date: "2024-08-01",     // 开始日期，格式：YYYY-MM-DD（必填）
   *   end_date: "2024-08-01",       // 结束日期，格式：YYYY-MM-DD（必填）
   *   summary_field: "asin",        // 汇总行维度：asin/parent_asin/msku/sku（可选，不传则遍历所有）
   *   // 其他查询参数...
   *   pageSize: 10000,              // 每页大小（可选，默认10000，最大10000）
   *   delayBetweenPages: 1000,      // 分页之间的延迟时间（毫秒，可选，默认1000）
   *   delayBetweenSummaryFields: 1000 // summary_field之间的延迟时间（毫秒，可选，默认1000）
   * }
   */
  fastify.post('/product-performance-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingReportService.fetchAllProductPerformance(accountId, params);

      return {
        success: true,
        message: '批量查询产品表现成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('批量查询产品表现错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('格式错误'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '批量查询产品表现失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润统计-MSKU
   * POST /api/lingxing/reports/msku-profit-statistics/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选，默认0）
   *   length: 1000,                 // 分页长度，上限10000（可选，默认1000）
   *   mids: [2],                    // 站点id（可选，数组）
   *   sids: [17],                   // 店铺id（可选，数组）
   *   startDate: "2023-07-16",      // 开始时间，双闭区间（必填，格式：YYYY-MM-DD，开始结束时间间隔最长不能跨度7天）
   *   endDate: "2023-07-21",        // 结束时间，双闭区间（必填，格式：YYYY-MM-DD）
   *   searchField: "msku",          // 搜索值类型：msku（可选）
   *   searchValue: ["test123"],     // 搜索值（可选，数组）
   *   currencyCode: "CNY"           // 币种code（可选）
   * }
   */
  fastify.post('/msku-profit-statistics/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingReportService.getMskuProfitStatistics(accountId, params);

      return {
        success: true,
        message: '获取利润统计MSKU成功',
        data: result.records,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取利润统计MSKU错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('格式错误') || error.message.includes('不能跨度'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润统计MSKU失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingReportRoutes;

