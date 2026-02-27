import lingXingVcService from '../../services/lingxing/vc/lingXingVcService.js';

/**
 * 领星ERP VC相关路由插件
 * 包含VC店铺、VC Listing、VC订单、VC发货单等接口
 */
async function lingxingVcRoutes(fastify, options) {
  /**
   * 查询VC店铺列表
   * POST /api/lingxing/vc/vc-sellers/:accountId
   * Body: { offset: 0, length: 20 } (可选)
   */
  fastify.post('/vc-sellers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { offset = 0, length = 20 } = request.body;

    // 参数验证
    const pageOffset = parseInt(offset) || 0;
    const pageLength = parseInt(length) || 20;

    if (pageOffset < 0) {
      return reply.code(400).send({
        success: false,
        message: 'offset必须大于等于0'
      });
    }

    if (pageLength < 1 || pageLength > 200) {
      return reply.code(400).send({
        success: false,
        message: 'length必须在1-200之间'
      });
    }

    try {
      const result = await lingXingVcService.getVcSellerPageList(
        accountId,
        pageOffset,
        pageLength
      );

      return {
        success: true,
        message: '获取VC店铺列表成功',
        data: {
          total: result.total,
          count: result.data.length,
          vcSellers: result.data
        },
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC店铺列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC店铺列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 从数据库获取VC店铺列表（缓存数据）
   * GET /api/lingxing/vc/vc-sellers/:accountId
   * Query: ?useCache=true (可选，默认true)
   */
  fastify.get('/vc-sellers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true' } = request.query;

    try {
      const vcSellers = await lingXingVcService.getVcSellerListsFromDB(accountId);

      if (useCache === 'true' && vcSellers.length === 0) {
        // 如果缓存为空且允许使用缓存，尝试从API获取并保存
        const result = await lingXingVcService.getVcSellerPageList(accountId, 0, 200);
        return {
          success: true,
          message: '获取VC店铺列表成功（已保存到数据库）',
          data: {
            count: result.data.length,
            total: result.total,
            vcSellers: result.data
          }
        };
      }

      return {
        success: true,
        message: '获取VC店铺列表成功（来自数据库）',
        data: {
          count: vcSellers.length,
          total: vcSellers.length,
          vcSellers: vcSellers
        }
      };
    } catch (error) {
      fastify.log.error('获取VC店铺列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC店铺列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有VC店铺数据（自动处理分页）
   * POST /api/lingxing/vc/vc-sellers/fetch-all/:accountId
   * Body: {
   *   // 选项（可选）
   *   options: {
   *     pageSize: 200,              // 每页大小（可选，默认200，最大200）
   *     delayBetweenPages: 500,     // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/vc-sellers/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options } = body;

    try {
      const result = await lingXingVcService.fetchAllVcSellers(accountId, options || {});

      return {
        success: true,
        message: '自动拉取所有VC店铺数据成功',
        data: {
          vcSellers: result.vcSellers,
          total: result.total,
          stats: result.stats
        }
      };
    } catch (error) {
      fastify.log.error('自动拉取所有VC店铺数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有VC店铺数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询VC Listing列表
   * POST /api/lingxing/vc/vc-listings/:accountId
   * Body: { 
   *   offset: 0, 
   *   length: 20,
   *   vc_store_ids: ["134225003201380860"] (可选)
   * }
   */
  fastify.post('/vc-listings/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { offset = 0, length = 20, vc_store_ids = null } = request.body;

    // 参数验证
    const pageOffset = parseInt(offset) || 0;
    const pageLength = parseInt(length) || 20;

    if (pageOffset < 0) {
      return reply.code(400).send({
        success: false,
        message: 'offset必须大于等于0'
      });
    }

    if (pageLength < 1 || pageLength > 200) {
      return reply.code(400).send({
        success: false,
        message: 'length必须在1-200之间'
      });
    }

    try {
      const result = await lingXingVcService.getVcListingPageList(
        accountId,
        pageOffset,
        pageLength,
        vc_store_ids
      );

      return {
        success: true,
        message: '获取VC Listing列表成功',
        data: {
          total: result.total,
          count: result.data.length,
          listings: result.data
        },
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC Listing列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC Listing列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 从数据库获取VC Listing列表（缓存数据）
   * GET /api/lingxing/vc/vc-listings/:accountId
   * Query: 
   *   ?useCache=true (可选，默认true)
   *   &vc_store_ids=id1,id2 (可选，多个用逗号分隔)
   */
  fastify.get('/vc-listings/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true', vc_store_ids } = request.query;

    try {
      // 解析vc_store_ids参数
      let vcStoreIds = null;
      if (vc_store_ids) {
        vcStoreIds = vc_store_ids.split(',').map(id => id.trim()).filter(id => id);
      }

      const listings = await lingXingVcService.getVcListingListsFromDB(accountId, vcStoreIds);

      if (useCache === 'true' && listings.length === 0) {
        // 如果缓存为空且允许使用缓存，尝试从API获取并保存
        const result = await lingXingVcService.getVcListingPageList(accountId, 0, 200, vcStoreIds);
        return {
          success: true,
          message: '获取VC Listing列表成功（已保存到数据库）',
          data: {
            count: result.data.length,
            total: result.total,
            listings: result.data
          }
        };
      }

      return {
        success: true,
        message: '获取VC Listing列表成功（来自数据库）',
        data: {
          count: listings.length,
          total: listings.length,
          listings: listings
        }
      };
    } catch (error) {
      fastify.log.error('获取VC Listing列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC Listing列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有VC Listing数据（自动处理分页）
   * POST /api/lingxing/vc/vc-listings/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   vc_store_ids: ["134225003201380860"],  // VC店铺id数组（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 200,              // 每页大小（可选，默认200，最大200）
   *     delayBetweenPages: 500,     // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/vc-listings/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, vc_store_ids } = body;

    try {
      const result = await lingXingVcService.fetchAllVcListings(
        accountId,
        vc_store_ids || null,
        options || {}
      );

      return {
        success: true,
        message: '自动拉取所有VC Listing数据成功',
        data: {
          listings: result.listings,
          total: result.total,
          stats: result.stats
        }
      };
    } catch (error) {
      fastify.log.error('自动拉取所有VC Listing数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有VC Listing数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询VC订单列表
   * POST /api/lingxing/vc/vc-orders/:accountId
   * Body: {
   *   purchase_order_type: ["0"],  // 必填：订单类型数组 ["0"] DF 或 ["1"] PO
   *   offset: 0,                    // 可选：分页偏移量
   *   length: 20,                   // 可选：分页长度
   *   vc_store_ids: ["134225003201380860"],  // 可选：VC店铺id数组
   *   search_field_time: "1",       // 可选：查询时间类型
   *   start_date: "2023-10-16",     // 可选：开始时间
   *   end_date: "2023-10-17",       // 可选：结束时间
   *   search_field: "purchase_order_number",  // 可选：搜索类型
   *   search_value: ["67HDLN3R"]    // 可选：搜索值数组
   * }
   */
  fastify.post('/vc-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    // 参数验证
    if (!params.purchase_order_type || !Array.isArray(params.purchase_order_type) || params.purchase_order_type.length === 0) {
      return reply.code(400).send({
        success: false,
        message: 'purchase_order_type参数必填，且必须为数组，例如：["0"] 或 ["1"]'
      });
    }

    const pageOffset = parseInt(params.offset) || 0;
    const pageLength = parseInt(params.length) || 20;

    if (pageOffset < 0) {
      return reply.code(400).send({
        success: false,
        message: 'offset必须大于等于0'
      });
    }

    if (pageLength < 1 || pageLength > 200) {
      return reply.code(400).send({
        success: false,
        message: 'length必须在1-200之间'
      });
    }

    try {
      const result = await lingXingVcService.getVcOrderPageList(accountId, params);

      return {
        success: true,
        message: '获取VC订单列表成功',
        data: {
          total: result.total,
          count: result.data.length,
          orders: result.data
        },
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC订单列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC订单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 从数据库获取VC订单列表（缓存数据）
   * GET /api/lingxing/vc/vc-orders/:accountId
   * Query: 
   *   ?useCache=true (可选，默认true)
   *   &vc_store_ids=id1,id2 (可选，多个用逗号分隔)
   *   &purchase_order_type=0 (可选：0 DF，1 PO)
   *   &start_date=2023-10-16 (可选)
   *   &end_date=2023-10-17 (可选)
   */
  fastify.get('/vc-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { useCache = 'true', vc_store_ids, purchase_order_type, start_date, end_date } = request.query;

    try {
      // 解析筛选参数
      const filters = {};
      if (vc_store_ids) {
        filters.vc_store_ids = vc_store_ids.split(',').map(id => id.trim()).filter(id => id);
      }
      if (purchase_order_type !== undefined) {
        filters.purchase_order_type = parseInt(purchase_order_type);
      }
      if (start_date && end_date) {
        filters.start_date = start_date;
        filters.end_date = end_date;
      }

      const orders = await lingXingVcService.getVcOrdersFromDB(accountId, filters);

      if (useCache === 'true' && orders.length === 0) {
        // 如果缓存为空且允许使用缓存，尝试从API获取并保存
        const params = {
          purchase_order_type: purchase_order_type !== undefined ? [String(purchase_order_type)] : ["0"],
          offset: 0,
          length: 200
        };
        if (filters.vc_store_ids) {
          params.vc_store_ids = filters.vc_store_ids;
        }
        if (start_date && end_date) {
          params.start_date = start_date;
          params.end_date = end_date;
        }
        const result = await lingXingVcService.getVcOrderPageList(accountId, params);
        return {
          success: true,
          message: '获取VC订单列表成功（已保存到数据库）',
          data: {
            count: result.data.length,
            total: result.total,
            orders: result.data
          }
        };
      }

      return {
        success: true,
        message: '获取VC订单列表成功（来自数据库）',
        data: {
          count: orders.length,
          total: orders.length,
          orders: orders
        }
      };
    } catch (error) {
      fastify.log.error('获取VC订单列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC订单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有VC订单数据（自动处理分页）
   * POST /api/lingxing/vc/vc-orders/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   purchase_order_type: ["0"],  // 订单类型数组（必填）
   *   
   *   // 筛选参数（可选）
   *   vc_store_ids: ["134225003201380860"],  // VC店铺id数组（可选）
   *   search_field_time: "1",       // 查询时间类型（可选）
   *   start_date: "2023-10-16",     // 开始时间（可选）
   *   end_date: "2023-10-17",       // 结束时间（可选）
   *   search_field: "purchase_order_number",  // 搜索类型（可选）
   *   search_value: ["67HDLN3R"]    // 搜索值数组（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 200,              // 每页大小（可选，默认200，最大200）
   *     delayBetweenPages: 500,     // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/vc-orders/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    // 验证必填参数
    if (!filterParams.purchase_order_type || !Array.isArray(filterParams.purchase_order_type) || filterParams.purchase_order_type.length === 0) {
      return reply.code(400).send({
        success: false,
        message: 'purchase_order_type参数必填，且必须为数组，例如：["0"] 或 ["1"]'
      });
    }

    try {
      const result = await lingXingVcService.fetchAllVcOrders(
        accountId,
        filterParams,
        options || {}
      );

      return {
        success: true,
        message: '自动拉取所有VC订单数据成功',
        data: {
          orders: result.orders,
          total: result.total,
          stats: result.stats
        }
      };
    } catch (error) {
      fastify.log.error('自动拉取所有VC订单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有VC订单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询VC订单详情（PO类型）
   * POST /api/lingxing/vc/vc-orders/po-detail/:accountId
   * Body: {
   *   local_po_number: "402242689523401371"  // 必填：本地po号
   * }
   */
  fastify.post('/vc-orders/po-detail/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { local_po_number } = request.body;

    if (!local_po_number) {
      return reply.code(400).send({
        success: false,
        message: 'local_po_number参数必填'
      });
    }

    try {
      const result = await lingXingVcService.getVcOrderPoDetail(accountId, local_po_number);

      return {
        success: true,
        message: '获取VC订单详情成功',
        data: result.data,
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC订单详情错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC订单详情失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询VC订单详情（DF类型）
   * POST /api/lingxing/vc/vc-orders/df-detail/:accountId
   * Body: {
   *   vc_store_id: "134225003201380864",   // 必填：VC店铺id
   *   purchase_order_number: "XB95bX69r"  // 必填：订单编号
   * }
   */
  fastify.post('/vc-orders/df-detail/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { vc_store_id, purchase_order_number } = request.body;

    if (!vc_store_id || !purchase_order_number) {
      return reply.code(400).send({
        success: false,
        message: 'vc_store_id、purchase_order_number 参数必填'
      });
    }

    try {
      const result = await lingXingVcService.getVcOrderDfDetail(accountId, vc_store_id, purchase_order_number);

      return {
        success: true,
        message: '获取VC订单详情(DF)成功',
        data: result.data,
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC订单详情(DF)错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC订单详情(DF)失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询VC发货单列表
   * POST /api/lingxing/vc/vc-invoices/:accountId
   * Body: {
   *   shipmentType: "1",  // 必填：出库类型，1:DF 2:PO 3:DI
   *   offset: 0,           // 可选：偏移量
   *   length: 20,          // 可选：每页条数
   *   sids: ["1","2"],     // 可选：店铺id数组
   *   wid: [1,2],          // 可选：国家id数组
   *   status: 0,           // 可选：订单状态，0:全部 5:待配货 10:待出库 15:已完成 100:已作废
   *   createTimeStartTime: "2025-01-01",  // 可选：创建日期-开始
   *   createTimeEndTime: "2025-12-31",    // 可选：创建日期-结束
   *   shipmentTimeStartTime: "2025-01-01",  // 可选：出库日期-开始
   *   shipmentTimeEndTime: "2025-12-31"      // 可选：出库日期-结束
   * }
   */
  fastify.post('/vc-invoices/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    // 参数验证
    if (!params.shipmentType) {
      return reply.code(400).send({
        success: false,
        message: 'shipmentType参数必填，1:DF 2:PO 3:DI'
      });
    }

    const pageOffset = parseInt(params.offset) || 0;
    const pageLength = parseInt(params.length) || 20;

    if (pageOffset < 0) {
      return reply.code(400).send({
        success: false,
        message: 'offset必须大于等于0'
      });
    }

    if (pageLength < 1 || pageLength > 200) {
      return reply.code(400).send({
        success: false,
        message: 'length必须在1-200之间'
      });
    }

    try {
      const result = await lingXingVcService.getVcInvoicePageList(accountId, params);

      return {
        success: true,
        message: '获取VC发货单列表成功',
        data: {
          total: result.total,
          count: result.data.length,
          invoices: result.data
        },
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC发货单列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC发货单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 从数据库获取VC发货单列表（缓存数据）
   * GET /api/lingxing/vc/vc-invoices/:accountId
   * Query: 
   *   ?useCache=true (可选，默认true)
   *   &shipmentType=1 (可选：1 DF，2 PO，3 DI)
   *   &status=5 (可选：0 全部，5 待配货，10 待出库，15 已完成，100 已作废)
   *   &createTimeStartTime=2025-01-01 (可选)
   *   &createTimeEndTime=2025-12-31 (可选)
   *   &shipmentTimeStartTime=2025-01-01 (可选)
   *   &shipmentTimeEndTime=2025-12-31 (可选)
   */
  fastify.get('/vc-invoices/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { 
      useCache = 'true', 
      shipmentType, 
      status, 
      createTimeStartTime, 
      createTimeEndTime,
      shipmentTimeStartTime,
      shipmentTimeEndTime
    } = request.query;

    try {
      // 解析筛选参数
      const filters = {};
      if (shipmentType) {
        filters.shipmentType = shipmentType;
      }
      if (status !== undefined && status !== null) {
        filters.status = parseInt(status);
      }
      if (createTimeStartTime && createTimeEndTime) {
        filters.createTimeStartTime = createTimeStartTime;
        filters.createTimeEndTime = createTimeEndTime;
      }
      if (shipmentTimeStartTime && shipmentTimeEndTime) {
        filters.shipmentTimeStartTime = shipmentTimeStartTime;
        filters.shipmentTimeEndTime = shipmentTimeEndTime;
      }

      const invoices = await lingXingVcService.getVcInvoicesFromDB(accountId, filters);

      if (useCache === 'true' && invoices.length === 0 && shipmentType) {
        // 如果缓存为空且允许使用缓存，尝试从API获取并保存
        const params = {
          shipmentType: shipmentType,
          offset: 0,
          length: 200
        };
        if (status !== undefined) {
          params.status = parseInt(status);
        }
        if (createTimeStartTime && createTimeEndTime) {
          params.createTimeStartTime = createTimeStartTime;
          params.createTimeEndTime = createTimeEndTime;
        }
        if (shipmentTimeStartTime && shipmentTimeEndTime) {
          params.shipmentTimeStartTime = shipmentTimeStartTime;
          params.shipmentTimeEndTime = shipmentTimeEndTime;
        }
        const result = await lingXingVcService.getVcInvoicePageList(accountId, params);
        return {
          success: true,
          message: '获取VC发货单列表成功（已保存到数据库）',
          data: {
            count: result.data.length,
            total: result.total,
            invoices: result.data
          }
        };
      }

      return {
        success: true,
        message: '获取VC发货单列表成功（来自数据库）',
        data: {
          count: invoices.length,
          total: invoices.length,
          invoices: invoices
        }
      };
    } catch (error) {
      fastify.log.error('获取VC发货单列表错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC发货单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有VC发货单数据（自动处理分页）
   * POST /api/lingxing/vc/vc-invoices/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   shipmentType: "1",  // 出库类型（必填）
   *   
   *   // 筛选参数（可选）
   *   sids: ["1","2"],     // 店铺id数组（可选）
   *   wid: [1,2],          // 国家id数组（可选）
   *   status: 0,           // 订单状态（可选）
   *   createTimeStartTime: "2025-01-01",  // 创建日期-开始（可选）
   *   createTimeEndTime: "2025-12-31",    // 创建日期-结束（可选）
   *   shipmentTimeStartTime: "2025-01-01",  // 出库日期-开始（可选）
   *   shipmentTimeEndTime: "2025-12-31"    // 出库日期-结束（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 200,              // 每页大小（可选，默认200，最大200）
   *     delayBetweenPages: 500,     // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/vc-invoices/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    // 验证必填参数
    if (!filterParams.shipmentType) {
      return reply.code(400).send({
        success: false,
        message: 'shipmentType参数必填，1:DF 2:PO 3:DI'
      });
    }

    try {
      const result = await lingXingVcService.fetchAllVcInvoices(
        accountId,
        filterParams,
        options || {}
      );

      return {
        success: true,
        message: '自动拉取所有VC发货单数据成功',
        data: {
          invoices: result.invoices,
          total: result.total,
          stats: result.stats
        }
      };
    } catch (error) {
      fastify.log.error('自动拉取所有VC发货单数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有VC发货单数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询VC发货单详情
   * POST /api/lingxing/vc/vc-invoices/detail/:accountId
   * Body: {
   *   orderNo: "RO250000000"  // 必填：发货单号
   * }
   */
  fastify.post('/vc-invoices/detail/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { orderNo } = request.body;

    if (!orderNo) {
      return reply.code(400).send({
        success: false,
        message: 'orderNo参数必填'
      });
    }

    try {
      const result = await lingXingVcService.getVcInvoiceDetail(accountId, orderNo);

      return {
        success: true,
        message: '获取VC发货单详情成功',
        data: result.data,
        meta: {
          code: result.code,
          message: result.message,
          error_details: result.error_details,
          request_id: result.request_id,
          response_time: result.response_time
        }
      };
    } catch (error) {
      fastify.log.error('获取VC发货单详情错误:', error);
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取VC发货单详情失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingVcRoutes;

