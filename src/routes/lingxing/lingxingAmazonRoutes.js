import lingxingAmazonService from '../../services/lingxing/amazon/lingXingAmazonService.js';

/**
 * 领星ERP亚马逊原表数据路由插件
 * 用于查询亚马逊源报表数据
 */
async function lingxingAmazonRoutes(fastify, options) {
  /**
   * 查询亚马逊源报表-所有订单
   * 查询 All Orders Report By last update 报表
   * POST /api/lingxing/amazon/all-orders/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   date_type: 1,                // 时间查询类型（可选，默认1）：1 下单日期，2 亚马逊订单更新时间
   *   start_date: "2020-04-01",    // 亚马逊当地下单时间，左闭区间，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 亚马逊当地下单时间，右开区间，格式：Y-m-d（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/all-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getAllOrdersReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊所有订单报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊所有订单报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊所有订单报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有订单数据（自动处理分页）
   * POST /api/lingxing/amazon/all-orders/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   sid: 109,                    // 店铺id（必填）
   *   date_type: 1,                // 时间查询类型（可选，默认1）
   *   start_date: "2020-04-01",    // 开始日期（必填）
   *   end_date: "2024-08-05",      // 结束日期（必填）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/all-orders/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllOrdersReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有订单数据成功',
        data: result.orders,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * getAllOrdersReport 增量同步（按上次同步结束日期拉取每个店铺的订单增量）
   * POST /api/lingxing/amazon/all-orders/incremental-sync/:accountId
   * Body: {
   *   endDate: "2025-02-23",       // 可选，同步截止日期 Y-m-d，不传则到昨天
   *   defaultLookbackDays: 7,     // 可选，无历史状态时回溯天数
   *   timezone: "Asia/Shanghai",  // 可选
   *   date_type: 1,               // 可选，1=下单日期 2=订单更新时间
   *   pageSize: 1000,
   *   delayBetweenPages: 500,
   *   delayBetweenShops: 500
   * }
   */
  fastify.post('/all-orders/incremental-sync/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingAmazonService.incrementalSyncAllOrdersReport(accountId, body);

      return {
        success: true,
        message: '所有订单增量同步执行完成',
        data: result.results,
        summary: result.summary
      };
    } catch (error) {
      fastify.log.error('所有订单增量同步错误:', error);

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '所有订单增量同步失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 通用增量同步（按 taskType 分发到对应报表的增量同步方法）
   * POST /api/lingxing/amazon/incremental-sync/:accountId
   * Body: {
   *   taskType: "fbaOrders" | "fbaExchangeOrders" | "fbaRefundOrders" | "fbmReturnOrders" | "removalOrders" | "removalShipment" | "transaction" | "amazonFulfilledShipments" | "fbaInventoryEventDetail" | "adjustmentList",
   *   endDate?: "Y-m-d",
   *   defaultLookbackDays?: number,
   *   timezone?: string,
   *   date_type?: number,
   *   pageSize?: number,
   *   delayBetweenPages?: number,
   *   delayBetweenShops?: number
   * }
   */
  fastify.post('/incremental-sync/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { taskType, ...options } = body;

    if (!taskType) {
      return reply.code(400).send({
        success: false,
        message: '请提供 body.taskType，可选: fbaOrders, fbaExchangeOrders, fbaRefundOrders, fbmReturnOrders, removalOrders, removalShipment, transaction, amazonFulfilledShipments, fbaInventoryEventDetail, adjustmentList'
      });
    }

    const methodMap = {
      fbaOrders: 'incrementalSyncFbaOrdersReport',
      fbaExchangeOrders: 'incrementalSyncFbaExchangeOrdersReport',
      fbaRefundOrders: 'incrementalSyncFbaRefundOrdersReport',
      fbmReturnOrders: 'incrementalSyncFbmReturnOrdersReport',
      removalOrders: 'incrementalSyncRemovalOrdersReport',
      removalShipment: 'incrementalSyncRemovalShipmentReport',
      transaction: 'incrementalSyncTransactionReport',
      amazonFulfilledShipments: 'incrementalSyncAmazonFulfilledShipmentsReport',
      fbaInventoryEventDetail: 'incrementalSyncFbaInventoryEventDetailReport',
      adjustmentList: 'incrementalSyncAdjustmentListReport'
    };

    const methodName = methodMap[taskType];
    if (!methodName || typeof lingxingAmazonService[methodName] !== 'function') {
      return reply.code(400).send({
        success: false,
        message: `不支持的 taskType: ${taskType}，可选: ${Object.keys(methodMap).join(', ')}`
      });
    }

    try {
      const result = await lingxingAmazonService[methodName](accountId, options);
      return {
        success: true,
        message: `${taskType} 增量同步执行完成`,
        data: result.results,
        summary: result.summary
      };
    } catch (error) {
      fastify.log.error(`${taskType} 增量同步错误:`, error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || `${taskType} 增量同步失败`,
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-FBA订单
   * 查询 Amazon-Fulfilled Shipments Report 报表
   * POST /api/lingxing/amazon/fba-orders/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   date_type: 1,                // 日期搜索维度（可选，默认1）：1 下单日期，2 配送日期
   *   start_date: "2020-04-01",    // 开始日期，左闭区间，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 结束日期，右开区间，格式：Y-m-d（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fba-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaOrdersReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊FBA订单报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊FBA订单报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊FBA订单报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA订单数据（自动处理分页）
   * POST /api/lingxing/amazon/fba-orders/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   sid: 109,                    // 店铺id（必填）
   *   date_type: 1,                // 日期搜索维度（可选，默认1）
   *   start_date: "2020-04-01",    // 开始日期（必填）
   *   end_date: "2024-08-05",      // 结束日期（必填）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fba-orders/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbaOrdersReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA订单数据成功',
        data: result.orders,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-FBA换货订单
   * 查询 Replacements Report 报表
   * POST /api/lingxing/amazon/fba-exchange-orders/:accountId
   * Body: {
   *   sid: 43,                     // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   start_date: "2020-01-01",    // 开始时间，左闭区间，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 结束时间，右开区间，格式：Y-m-d（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fba-exchange-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaExchangeOrdersReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊FBA换货订单报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊FBA换货订单报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊FBA换货订单报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA换货订单数据（自动处理分页）
   * POST /api/lingxing/amazon/fba-exchange-orders/fetch-all/:accountId
   */
  fastify.post('/fba-exchange-orders/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbaExchangeOrdersReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA换货订单数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA换货订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA换货订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-FBA退货订单
   * 查询 FBA customer returns 报表
   * POST /api/lingxing/amazon/fba-refund-orders/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   date_type: 1,                // 时间查询类型（可选，默认1）：1 退货时间【站点时间】，2 更新时间【北京时间】
   *   start_date: "2020-01-01",    // 开始时间，左闭右开，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 结束时间，左闭右开，格式：Y-m-d（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fba-refund-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaRefundOrdersReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊FBA退货订单报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊FBA退货订单报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊FBA退货订单报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA退货订单数据（自动处理分页）
   * POST /api/lingxing/amazon/fba-refund-orders/fetch-all/:accountId
   */
  fastify.post('/fba-refund-orders/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbaRefundOrdersReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA退货订单数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA退货订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA退货订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-FBM退货订单
   * 查询 Returns Reports 报表
   * POST /api/lingxing/amazon/fbm-return-orders/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   start_date: "2020-01-01",    // 开始时间，左闭区间，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 结束时间，右开区间，格式：Y-m-d（必填）
   *   date_type: 1,                // 时间查询类型（可选，默认1）：1 退货日期，2 下单日期
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fbm-return-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbmReturnOrdersReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊FBM退货订单报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊FBM退货订单报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊FBM退货订单报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBM退货订单数据（自动处理分页）
   * POST /api/lingxing/amazon/fbm-return-orders/fetch-all/:accountId
   */
  fastify.post('/fbm-return-orders/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbmReturnOrdersReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBM退货订单数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBM退货订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBM退货订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-移除订单（新）
   * 查询 Reports-Fulfillment-Removal Order Detail 报表
   * 报表为seller_id维度，按sid请求会返回对应seller_id下所有移除订单数据，同一个seller_id授权的店铺任取一个sid请求报表数据即可
   * POST /api/lingxing/amazon/removal-orders-new/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   start_date: "2020-01-01",    // 查询时间【更新时间】，左闭区间，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 查询时间【更新时间】，右开区间，格式：Y-m-d（必填）
   *   search_field_time: "last_updated_date",  // 搜索时间类型（可选，默认 last_updated_date）：last_updated_date 更新时间，request_date 创建时间
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/removal-orders-new/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getRemovalOrdersReportNew(accountId, params);

      return {
        success: true,
        message: '获取亚马逊移除订单报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊移除订单报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊移除订单报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有移除订单数据（自动处理分页）
   * POST /api/lingxing/amazon/removal-orders-new/fetch-all/:accountId
   */
  fastify.post('/removal-orders-new/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllRemovalOrdersReportNew(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有移除订单数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有移除订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有移除订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-移除货件（新）
   * 查询 Reports-Fulfillment-Removal Shipment Detail 报表
   * 报表为seller_id维度，按sid请求会返回对应seller_id下所有移除订单数据，同一个seller_id授权的店铺任取一个sid请求报表数据即可
   * POST /api/lingxing/amazon/removal-shipments-new/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id【seller_id同时传值时，以sid为准】（可选，但sid和seller_id至少需要传一个）
   *   seller_id: "A1MQMW3JWPNCBX", // 亚马逊店铺id（可选，但sid和seller_id至少需要传一个）
   *   start_date: "2020-01-01",    // 开始日期【发货日期】，左闭右开，格式：Y-m-d（必填）
   *   end_date: "2024-08-05",      // 结束日期【发货日期】，左闭右开，格式：Y-m-d（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/removal-shipments-new/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getRemovalShipmentReportNew(accountId, params);

      return {
        success: true,
        message: '获取亚马逊移除货件报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊移除货件报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊移除货件报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有移除货件数据（自动处理分页）
   * POST /api/lingxing/amazon/removal-shipments-new/fetch-all/:accountId
   */
  fastify.post('/removal-shipments-new/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllRemovalShipmentReportNew(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有移除货件数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有移除货件数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有移除货件数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-FBA库存
   * 查询 FBA Manage Inventory 报表
   * POST /api/lingxing/amazon/fba-inventory/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fba-inventory/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaInventoryReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊FBA库存报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊FBA库存报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊FBA库存报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA库存数据（自动处理分页）
   * POST /api/lingxing/amazon/fba-inventory/fetch-all/:accountId
   */
  fastify.post('/fba-inventory/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbaInventoryReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA库存数据成功',
        data: result.inventory,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA库存数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA库存数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-每日库存
   * 查询 FBA Daily Inventory History Report 报表
   * 注意：由于亚马逊对应报表下线，2023年12月1日后不再更新此接口数据，获取数据请使用 查询库存分类账summary数据
   * POST /api/lingxing/amazon/daily-inventory/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id【欧洲传UK下的店铺，美国传US下的店铺】（必填）
   *   event_date: "2024-08-05",    // 报表日期，格式：Y-m-d（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/daily-inventory/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getDailyInventoryReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊每日库存报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊每日库存报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊每日库存报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA可售库存数据（自动处理分页）
   * POST /api/lingxing/amazon/fba-fulfillable-quantity/fetch-all/:accountId
   */
  fastify.post('/fba-fulfillable-quantity/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbaFulfillableQuantityReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA可售库存数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA可售库存数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA可售库存数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-FBA可售库存
   * 查询 FBA Multi-Country Inventory Report 报表
   * POST /api/lingxing/amazon/fba-fulfillable-quantity/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fba-fulfillable-quantity/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaFulfillableQuantityReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊FBA可售库存报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊FBA可售库存报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊FBA可售库存报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有预留库存数据（自动处理分页）
   * POST /api/lingxing/amazon/reserved-inventory/fetch-all/:accountId
   */
  fastify.post('/reserved-inventory/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllReservedInventoryReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有预留库存数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有预留库存数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有预留库存数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-预留库存
   * 查询 FBA Reserved Inventory Report 报表
   * POST /api/lingxing/amazon/reserved-inventory/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/reserved-inventory/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getReservedInventoryReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊预留库存报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊预留库存报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊预留库存报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有交易明细数据（自动处理分页）
   * POST /api/lingxing/amazon/transaction/fetch-all/:accountId
   */
  fastify.post('/transaction/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllTransactionReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有交易明细数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有交易明细数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有交易明细数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-库龄表
   * 查询 Manage Inventory Health 报表
   * POST /api/lingxing/amazon/fba-age-list/:accountId
   * Body: {
   *   sid: "109",                  // 店铺id，多个使用英文逗号分隔（必填，字符串类型）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 20                   // 分页长度（可选，默认20）
   * }
   */
  fastify.post('/fba-age-list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaAgeListReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊库龄表报表成功',
        data: result.data.list,
        total: result.data.total,
        responseTotal: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊库龄表报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊库龄表报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有Amazon Fulfilled Shipments数据（自动处理分页）
   * POST /api/lingxing/amazon/fulfilled-shipments/fetch-all/:accountId
   */
  fastify.post('/fulfilled-shipments/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllAmazonFulfilledShipmentsReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有Amazon Fulfilled Shipments数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有Amazon Fulfilled Shipments数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有Amazon Fulfilled Shipments数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-交易明细
   * 查询领星插件下载的 Transaction 报表数据
   * 注意：本接口即将下线，建议使用查询结算中心 - 交易明细
   * POST /api/lingxing/amazon/transaction/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   event_date: "2020-06-07",    // 报表日期，格式：Y-m-d【每月３日后支持查询上月数据】（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/transaction/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getTransactionReport(accountId, params);

      return {
        success: true,
        message: '获取亚马逊交易明细报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取亚马逊交易明细报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取亚马逊交易明细报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有Inventory Event Detail数据（自动处理分页）
   * POST /api/lingxing/amazon/inventory-event-detail/fetch-all/:accountId
   */
  fastify.post('/inventory-event-detail/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllFbaInventoryEventDetailReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有Inventory Event Detail数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有Inventory Event Detail数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有Inventory Event Detail数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-Amazon Fulfilled Shipments
   * 查询 Amazon Fulfilled Shipments 报表
   * POST /api/lingxing/amazon/fulfilled-shipments/:accountId
   * Body: {
   *   sid: 109,                              // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   shipment_date_after: "2024-01-01 00:00:00",  // 快照开始时间【shipment_date_locale】，格式：Y-m-d hh-mm-ss，开始结束时间区间支持7天（必填）
   *   shipment_date_before: "2024-01-06 00:00:00", // 快照结束时间【shipment_date_locale】，格式：Y-m-d hh-mm-ss，开始结束时间区间支持7天（必填）
   *   amazon_order_id: ["123-1234567-1234567","789-1234567-1234567"],  // 订单ID数组（可选）
   *   offset: 0,                             // 分页偏移量（可选，默认0）
   *   length: 1000                            // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/fulfilled-shipments/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getAmazonFulfilledShipmentsReport(accountId, params);

      return {
        success: true,
        message: '获取Amazon Fulfilled Shipments报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取Amazon Fulfilled Shipments报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取Amazon Fulfilled Shipments报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有盘存记录数据（自动处理分页）
   * POST /api/lingxing/amazon/adjustment-list/fetch-all/:accountId
   */
  fastify.post('/adjustment-list/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingAmazonService.fetchAllAdjustmentListReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有盘存记录数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有盘存记录数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有盘存记录数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-Inventory Event Detail
   * 查询 FBA Inventory Event Detail 报表
   * 注意：2023年3月后不再更新此接口数据【亚马逊对应报表下线】，获取之后的数据请使用 查询亚马逊库存分类账detail数据
   * POST /api/lingxing/amazon/inventory-event-detail/:accountId
   * Body: {
   *   sid: 109,                    // 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   snapshot_date_after: "2024-06-01",  // 快照开始时间【snapshot_date_locale】，格式：Y-m-d，开始结束时间区间支持7天（必填）
   *   snapshot_date_before: "2024-06-07", // 快照结束时间【snapshot_date_locale】，格式：Y-m-d，开始结束时间区间支持7天（必填）
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 1000                  // 分页长度（可选，默认1000）
   * }
   */
  fastify.post('/inventory-event-detail/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getFbaInventoryEventDetailReport(accountId, params);

      return {
        success: true,
        message: '获取Inventory Event Detail报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取Inventory Event Detail报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取Inventory Event Detail报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询亚马逊源报表-盘存记录
   * POST /api/lingxing/amazon/adjustment-list/:accountId
   * Body: {
   *   offset: 0,                   // 分页偏移量（必填，默认0）
   *   length: 20,                  // 分页长度（必填，默认20，上限10000）
   *   sids: "1,2,109",             // 店铺id，多个店铺以英文逗号分隔（可选）
   *   search_field: "msku",        // 搜索的字段（可选）：asin ASIN, msku MSKU, fnsku FNSKU, item_name 标题, transaction_item_id 交易编号
   *   search_value: "Black_ Head_Rop",  // 搜索值（可选）
   *   start_date: "2022-08-01",    // 发货日期开始时间【闭区间】，格式Y-m-d【report_date】（必填）
   *   end_date: "2024-08-01"        // 发货日期结束时间【闭区间】，格式Y-m-d【report_date】（必填）
   * }
   */
  fastify.post('/adjustment-list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingAmazonService.getAdjustmentListReport(accountId, params);

      return {
        success: true,
        message: '获取盘存记录报表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取盘存记录报表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取盘存记录报表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingAmazonRoutes;

