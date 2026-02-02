import lingxingPurchaseService from '../services/lingxing/purchase/lingXingPurchaseService.js';

/**
 * 领星ERP采购路由插件
 * 供应商相关接口
 */
async function lingxingPurchaseRoutes(fastify, options) {
  /**
   * 查询供应商列表
   * POST /api/lingxing/purchase/suppliers/:accountId
   * Body: {
   *   offset: 0,    // 分页偏移量（可选，默认0）
   *   length: 1000  // 分页长度（可选，默认1000）
   * }
   * 支持查询【采购】>【供应商】中的供应商信息
   */
  fastify.post('/suppliers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getSupplierList(accountId, params);

      return {
        success: true,
        message: '获取供应商列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取供应商列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取供应商列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询采购方列表
   * POST /api/lingxing/purchase/purchasers/:accountId
   * Body: {
   *   offset: 0,    // 分页偏移量（可选，默认0）
   *   length: 500   // 分页长度（可选，默认500）
   * }
   */
  fastify.post('/purchasers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getPurchaserList(accountId, params);

      return {
        success: true,
        message: '获取采购方列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取采购方列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取采购方列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询采购计划列表
   * POST /api/lingxing/purchase/purchase-plans/:accountId
   * Body: {
   *   search_field_time: "creator_time",  // 时间搜索维度（必填）：creator_time/expect_arrive_time/update_time
   *   start_date: "2024-07-30",          // 开始日期（必填）：Y-m-d 或 Y-m-d H:i:s
   *   end_date: "2024-08-02",             // 结束日期（必填）：Y-m-d 或 Y-m-d H:i:s
   *   plan_sns: ["PP240730001"],          // 采购计划编号数组（可选）
   *   is_combo: 0,                        // 是否为组合商品：0 否，1 是（可选）
   *   is_related_process_plan: 0,          // 是否关联加工计划：0 否，1 是（可选）
   *   status: [-2],                       // 状态数组（可选）
   *   sids: [0],                          // 店铺id数组（可选）
   *   offset: 0,                          // 分页偏移量（可选，默认0）
   *   length: 500                         // 分页长度（可选，默认500，上限500）
   * }
   */
  fastify.post('/purchase-plans/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getPurchasePlanList(accountId, params);

      return {
        success: true,
        message: '获取采购计划列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取采购计划列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取采购计划列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询采购单列表
   * POST /api/lingxing/purchase/purchase-orders/:accountId
   * Body: {
   *   start_date: "2024-05-30",                    // 开始时间（必填）：Y-m-d 或 Y-m-d H:i:s
   *   end_date: "2024-08-02",                     // 结束时间（必填）：Y-m-d 或 Y-m-d H:i:s
   *   search_field_time: "create_time",          // 时间搜索维度（可选，默认create_time）：create_time/expect_arrive_time/update_time
   *   order_sn: ["PO240802012"],                  // 采购单号数组，上限500（可选）
   *   custom_order_sn: ["PO240802012"],           // 自定义采购单号数组，上限500（可选）
   *   purchase_type: 1,                           // 采购类型，1：普通采购，2:1688采购（可选）
   *   offset: 0,                                  // 分页偏移量（可选，默认0）
   *   length: 500                                 // 分页长度（可选，默认500，上限500）
   * }
   * 支持查询【采购】>【采购单】数据
   */
  fastify.post('/purchase-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getPurchaseOrderList(accountId, params);

      return {
        success: true,
        message: '获取采购单列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取采购单列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取采购单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询采购退货单列表
   * POST /api/lingxing/purchase/purchase-return-orders/:accountId
   * Body: {
   *   offset: 0,                           // 分页偏移量（必填）
   *   length: 20,                          // 分页长度，上限500（必填）
   *   search_field_time: "create_time",    // 时间搜索维度（可选，默认create_time）：create_time/last_time
   *   start_date: "2024-08-02",            // 开始时间（可选）：Y-m-d 或 Y-m-d H:i:s
   *   end_date: "2024-08-02",              // 结束时间（可选）：Y-m-d 或 Y-m-d H:i:s
   *   status: [10, 20]                     // 状态数组（可选）
   * }
   */
  fastify.post('/purchase-return-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getPurchaseReturnOrderList(accountId, params);

      return {
        success: true,
        message: '获取采购退货单列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取采购退货单列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取采购退货单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有供应商列表
   * POST /api/lingxing/purchase/fetch-all-suppliers/:accountId
   * Body: {
   *   options: {
   *     pageSize: 1000,              // 每页大小（可选，默认1000）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-suppliers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { options = {} } = request.body || {};

    try {
      const result = await lingxingPurchaseService.fetchAllSuppliers(accountId, options);

      return {
        success: true,
        message: '自动拉取所有供应商列表成功',
        data: result.suppliers,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有供应商列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有供应商列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有采购方列表
   * POST /api/lingxing/purchase/fetch-all-purchasers/:accountId
   * Body: {
   *   options: {
   *     pageSize: 500,               // 每页大小（可选，默认500）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-purchasers/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const { options = {} } = request.body || {};

    try {
      const result = await lingxingPurchaseService.fetchAllPurchasers(accountId, options);

      return {
        success: true,
        message: '自动拉取所有采购方列表成功',
        data: result.purchasers,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有采购方列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有采购方列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有采购计划列表
   * POST /api/lingxing/purchase/fetch-all-purchase-plans/:accountId
   * Body: {
   *   // 采购计划列表查询参数（必填）
   *   search_field_time: "creator_time",  // 时间搜索维度（必填）
   *   start_date: "2024-07-30",          // 开始日期（必填）
   *   end_date: "2024-08-02",             // 结束日期（必填）
   *   // 其他查询参数（可选）...
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 500,               // 每页大小（可选，默认500）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-purchase-plans/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...listParams } = body;

    try {
      const result = await lingxingPurchaseService.fetchAllPurchasePlans(accountId, listParams, options || {});

      return {
        success: true,
        message: '自动拉取所有采购计划列表成功',
        data: result.plans,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有采购计划列表错误:', error);
      
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有采购计划列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有采购单列表
   * POST /api/lingxing/purchase/fetch-all-purchase-orders/:accountId
   * Body: {
   *   // 采购单列表查询参数（必填）
   *   start_date: "2024-05-30",          // 开始时间（必填）
   *   end_date: "2024-08-02",             // 结束时间（必填）
   *   // 其他查询参数（可选）...
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 500,               // 每页大小（可选，默认500）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-purchase-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...listParams } = body;

    try {
      const result = await lingxingPurchaseService.fetchAllPurchaseOrders(accountId, listParams, options || {});

      return {
        success: true,
        message: '自动拉取所有采购单列表成功',
        data: result.orders,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有采购单列表错误:', error);
      
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有采购单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有采购退货单列表
   * POST /api/lingxing/purchase/fetch-all-purchase-return-orders/:accountId
   * Body: {
   *   // 采购退货单列表查询参数（可选）
   *   search_field_time: "create_time",  // 时间搜索维度（可选）
   *   start_date: "2024-08-02",          // 开始时间（可选）
   *   end_date: "2024-08-02",             // 结束时间（可选）
   *   status: [10, 20],                   // 状态数组（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 500,               // 每页大小（可选，默认500）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-purchase-return-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...listParams } = body;

    try {
      const result = await lingxingPurchaseService.fetchAllPurchaseReturnOrders(accountId, listParams, options || {});

      return {
        success: true,
        message: '自动拉取所有采购退货单列表成功',
        data: result.returnOrders,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有采购退货单列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有采购退货单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询采购变更单列表
   * POST /api/lingxing/purchase/purchase-change-orders/:accountId
   * Body: {
   *   offset: 0,                           // 分页偏移量（必填）
   *   length: 20,                          // 分页长度（必填）
   *   search_field_time: "create_time",    // 筛选时间类型（可选）：create_time/update_time，默认create_time
   *   start_date: "2024-08-02",            // 开始时间（可选）
   *   end_date: "2024-08-02",              // 结束时间（可选）
   *   multi_search_field: "purchase_order_sn",  // 搜索单号字段（可选）：order_sn/purchase_order_sn
   *   multi_search_value: ["test-01"]      // 批量搜索的单号值数组（可选）
   * }
   */
  fastify.post('/purchase-change-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getPurchaseChangeOrderList(accountId, params);

      return {
        success: true,
        message: '获取采购变更单列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取采购变更单列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取采购变更单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有采购变更单列表
   * POST /api/lingxing/purchase/fetch-all-purchase-change-orders/:accountId
   * Body: {
   *   // 采购变更单列表查询参数（可选）
   *   search_field_time: "create_time",    // 筛选时间类型（可选）：create_time/update_time
   *   start_date: "2024-08-02",            // 开始时间（可选）
   *   end_date: "2024-08-02",              // 结束时间（可选）
   *   multi_search_field: "purchase_order_sn",  // 搜索单号字段（可选）
   *   multi_search_value: ["test-01"],     // 批量搜索的单号值数组（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 500,               // 每页大小（可选，默认500）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-purchase-change-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...listParams } = body;

    try {
      const result = await lingxingPurchaseService.fetchAllPurchaseChangeOrders(accountId, listParams, options || {});

      return {
        success: true,
        message: '自动拉取所有采购变更单列表成功',
        data: result.changeOrders,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有采购变更单列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有采购变更单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询收货单列表
   * POST /api/lingxing/purchase/receipt-orders/:accountId
   * Body: {
   *   date_type: 1,                      // 查询时间类型（可选）：1 预计到货时间，2 收货时间，3 创建时间，4 更新时间
   *   start_date: "2024-07-29",          // 开始时间，格式：Y-m-d 或 Y-m-d H:i:s（可选）
   *   end_date: "2024-07-29",            // 结束时间，格式：Y-m-d 或 Y-m-d H:i:s（可选）
   *   order_sns: "CR240729025,CR240729013", // 收货单号，多个使用英文逗号分隔（可选）
   *   status: 40,                        // 状态（可选）：10 待收货，40 已完成
   *   wid: "1,2",                        // 仓库id，多个使用英文逗号分隔（可选）
   *   order_type: 1,                     // 收货类型（可选）：1 采购订单，2 委外订单
   *   qc_status: "0,1,2",                // 质检状态，多个使用英文逗号分隔（可选）：0 未质检，1 部分质检，2 完成质检
   *   offset: 0,                         // 分页偏移量（可选，默认0）
   *   length: 20                         // 分页长度（可选，默认200，上限500）
   * }
   */
  fastify.post('/receipt-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingPurchaseService.getPurchaseReceiptOrderList(accountId, params);

      return {
        success: true,
        message: '获取收货单列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取收货单列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取收货单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有收货单列表
   * POST /api/lingxing/purchase/fetch-all-receipt-orders/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   date_type: 1,                      // 查询时间类型（可选）
   *   start_date: "2024-07-29",          // 开始时间（可选）
   *   end_date: "2024-07-29",            // 结束时间（可选）
   *   order_sns: "CR240729025",          // 收货单号（可选）
   *   status: 40,                        // 状态（可选）
   *   wid: "1,2",                        // 仓库id（可选）
   *   order_type: 1,                     // 收货类型（可选）
   *   qc_status: "0,1,2",                // 质检状态（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 500,                   // 每页大小（可选，默认500，最大500）
   *     delayBetweenPages: 500,           // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-receipt-orders/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingPurchaseService.fetchAllPurchaseReceiptOrders(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有收货单列表成功',
        data: result.receiptOrderList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有收货单列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有收货单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingPurchaseRoutes;

