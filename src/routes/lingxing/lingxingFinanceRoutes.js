import lingxingFinanceService from '../../services/lingxing/finance/lingXingFinanceService.js';

/**
 * 领星ERP财务管理路由插件
 * 用于查询财务管理相关数据
 */
async function lingxingFinanceRoutes(fastify, options) {
  /**
   * 查询费用类型列表
   * POST /api/lingxing/finance/fee-types/:accountId
   */
  fastify.post('/fee-types/:accountId', async (request, reply) => {
    const { accountId } = request.params;

    try {
      const feeTypes = await lingxingFinanceService.getFeeTypeList(accountId);

      return {
        success: true,
        message: '获取费用类型列表成功',
        data: feeTypes
      };
    } catch (error) {
      fastify.log.error('获取费用类型列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取费用类型列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有费用类型列表
   * POST /api/lingxing/finance/fee-types/fetch-all/:accountId
   * Body: {
   *   // 选项（可选）
   *   options: {
   *     onProgress: null  // 进度回调函数（可选）
   *   }
   * }
   */
  fastify.post('/fee-types/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options } = body;

    try {
      const result = await lingxingFinanceService.fetchAllFeeTypes(accountId, options || {});

      return {
        success: true,
        message: '自动拉取所有费用类型列表成功',
        data: result.feeTypes,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有费用类型列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有费用类型列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询费用明细列表
   * POST /api/lingxing/finance/fee-details/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（必填）
   *   length: 20,                   // 分页长度（必填）
   *   date_type: "gmt_create",     // 时间类型：gmt_create 创建日期，date 分摊日期（必填）
   *   start_date: "2022-12-01",    // 开始时间，格式：Y-m-d（必填）
   *   end_date: "2023-12-01",      // 结束时间，格式：Y-m-d（必填）
   *   sids: [100, 113],            // 店铺id数组（可选）
   *   other_fee_type_ids: [1232, 1246],  // 费用类型id数组（可选）
   *   status_order: 3,             // 单据状态：1 待提交，2 待审批，3 已处理，4 已驳回，5 已作废（可选）
   *   dimensions: [1, 2, 3, 4],    // 分摊维度id数组（可选）
   *   apportion_status: [1, 2, 3, 4],  // 分摊状态数组（可选）
   *   status_merge: 2,            // 分摊状态：1 未分摊，2 已分摊（可选）
   *   search_field: "create_name", // 搜索类型（可选）
   *   search_value: "何芳"         // 搜索值（可选）
   * }
   */
  fastify.post('/fee-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getFeeDetailList(accountId, params);

      return {
        success: true,
        message: '获取费用明细列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取费用明细列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取费用明细列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有费用明细列表（自动处理分页）
   * POST /api/lingxing/finance/fee-details/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   date_type: "gmt_create",     // 时间类型（必填）
   *   start_date: "2022-12-01",   // 开始日期（必填）
   *   end_date: "2023-12-01",     // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   sids: [100, 113],            // 店铺id数组
   *   other_fee_type_ids: [1232, 1246],  // 费用类型id数组
   *   status_order: 3,             // 单据状态
   *   dimensions: [1, 2, 3, 4],    // 分摊维度id数组
   *   apportion_status: [1, 2, 3, 4],  // 分摊状态数组
   *   status_merge: 2,            // 分摊状态
   *   search_field: "create_name", // 搜索类型
   *   search_value: "何芳",        // 搜索值
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,              // 每页大小（可选，默认20，最大100）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fee-details/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllFeeDetails(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有费用明细列表成功',
        data: result.feeDetails,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有费用明细列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有费用明细列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润报表-MSKU
   * POST /api/lingxing/finance/profit-report/msku/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选，默认0）
   *   length: 1000,                // 分页长度，上限10000（可选，默认1000）
   *   mids: [2],                   // 站点id数组（可选）
   *   sids: [110],                 // 店铺id数组（可选）
   *   monthlyQuery: false,         // 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   startDate: "2023-09-21",     // 开始时间【结算时间，双闭区间】（必填）
   *   endDate: "2023-10-20",       // 结束时间【结算时间，双闭区间】（必填）
   *   searchField: "seller_sku",  // 搜索值类型（可选）
   *   searchValue: ["a-111"],     // 搜索的值数组（可选）
   *   currencyCode: "CNY",         // 币种code【默认原币种】（可选）
   *   summaryEnabled: false,       // 是否按msku汇总返回（可选）
   *   orderStatus: "Disbursed"     // 交易状态（可选）
   * }
   */
  fastify.post('/profit-report/msku/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getMskuProfitReport(accountId, params);

      return {
        success: true,
        message: '获取利润报表-MSKU成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取利润报表-MSKU错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润报表-MSKU失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有利润报表-MSKU数据（自动处理分页）
   * POST /api/lingxing/finance/profit-report/msku/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   startDate: "2023-09-21",    // 开始日期（必填）
   *   endDate: "2023-10-20",      // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   mids: [2],                  // 站点id数组
   *   sids: [110],                // 店铺id数组
   *   monthlyQuery: false,        // 是否按月查询
   *   searchField: "seller_sku", // 搜索值类型
   *   searchValue: ["a-111"],    // 搜索的值数组
   *   currencyCode: "CNY",        // 币种code
   *   summaryEnabled: false,      // 是否按msku汇总返回
   *   orderStatus: "Disbursed",   // 交易状态
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000，最大10000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/profit-report/msku/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllMskuProfitReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有利润报表-MSKU数据成功',
        data: result.profitReports,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有利润报表-MSKU数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有利润报表-MSKU数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润报表-ASIN
   * POST /api/lingxing/finance/profit-report/asin/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选，默认0）
   *   length: 1000,                // 分页长度，上限10000（可选，默认1000）
   *   mids: [2],                   // 站点id数组（可选）
   *   sids: [110],                 // 店铺id数组（可选）
   *   monthlyQuery: false,         // 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   startDate: "2023-09-21",     // 开始时间【结算时间，双闭区间】（必填）
   *   endDate: "2023-10-20",       // 结束时间【结算时间，双闭区间】（必填）
   *   searchField: "asin",         // 搜索值类型（可选，默认asin）
   *   searchValue: ["B085NQDDXS", "B085NQLM2D"],  // 搜索的值数组（可选）
   *   currencyCode: "CNY",         // 币种code（可选）
   *   summaryEnabled: false,      // 是否按asin汇总返回（可选）
   *   orderStatus: "Disbursed"     // 交易状态（可选）
   * }
   */
  fastify.post('/profit-report/asin/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getAsinProfitReport(accountId, params);

      return {
        success: true,
        message: '获取利润报表-ASIN成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取利润报表-ASIN错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润报表-ASIN失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有利润报表-ASIN数据（自动处理分页）
   * POST /api/lingxing/finance/profit-report/asin/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   startDate: "2023-09-21",    // 开始日期（必填）
   *   endDate: "2023-10-20",      // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   mids: [2],                  // 站点id数组
   *   sids: [110],                // 店铺id数组
   *   monthlyQuery: false,        // 是否按月查询
   *   searchField: "asin",        // 搜索值类型
   *   searchValue: ["B085NQDDXS", "B085NQLM2D"],  // 搜索的值数组
   *   currencyCode: "CNY",         // 币种code
   *   summaryEnabled: false,      // 是否按asin汇总返回
   *   orderStatus: "Disbursed",   // 交易状态
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000，最大10000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/profit-report/asin/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllAsinProfitReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有利润报表-ASIN数据成功',
        data: result.profitReports,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有利润报表-ASIN数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有利润报表-ASIN数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润报表-父ASIN
   * POST /api/lingxing/finance/profit-report/parent-asin/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选，默认0）
   *   length: 1000,                // 分页长度，上限10000（可选，默认1000）
   *   mids: [2],                   // 站点id数组（可选）
   *   sids: [110],                 // 店铺id数组（可选）
   *   monthlyQuery: false,         // 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   startDate: "2023-09-21",     // 开始时间【结算时间，双闭区间】（必填）
   *   endDate: "2023-10-20",       // 结束时间【结算时间，双闭区间】（必填）
   *   searchField: "parent_asin",  // 搜索值类型（可选，默认parent_asin）
   *   searchValue: ["B09MT9BKGH", "B09MT3989Q"],  // 搜索的值数组（可选）
   *   currencyCode: "CNY",         // 币种code（可选）
   *   summaryEnabled: false,      // 是否按父asin汇总返回（可选）
   *   orderStatus: "Disbursed"     // 交易状态（可选）
   * }
   */
  fastify.post('/profit-report/parent-asin/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getParentAsinProfitReport(accountId, params);

      return {
        success: true,
        message: '获取利润报表-父ASIN成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取利润报表-父ASIN错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润报表-父ASIN失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有利润报表-父ASIN数据（自动处理分页）
   * POST /api/lingxing/finance/profit-report/parent-asin/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   startDate: "2023-09-21",    // 开始日期（必填）
   *   endDate: "2023-10-20",      // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   mids: [2],                  // 站点id数组
   *   sids: [110],                // 店铺id数组
   *   monthlyQuery: false,        // 是否按月查询
   *   searchField: "parent_asin", // 搜索值类型
   *   searchValue: ["B09MT9BKGH", "B09MT3989Q"],  // 搜索的值数组
   *   currencyCode: "CNY",        // 币种code
   *   summaryEnabled: false,      // 是否按父asin汇总返回
   *   orderStatus: "Disbursed",   // 交易状态
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000，最大10000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/profit-report/parent-asin/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllParentAsinProfitReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有利润报表-父ASIN数据成功',
        data: result.profitReports,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有利润报表-父ASIN数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有利润报表-父ASIN数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润报表-SKU
   * POST /api/lingxing/finance/profit-report/sku/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选，默认0）
   *   length: 1000,                // 分页长度，上限10000（可选，默认1000）
   *   mids: [2],                   // 站点id数组（可选）
   *   sids: [110],                 // 店铺id数组（可选）
   *   monthlyQuery: false,         // 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   startDate: "2023-09-21",     // 开始时间【结算时间，双闭区间】（必填）
   *   endDate: "2023-10-20",       // 结束时间【结算时间，双闭区间】（必填）
   *   searchField: "local_sku",   // 搜索值类型（可选，默认local_sku）
   *   searchValue: ["#"],          // 搜索的值数组（可选）
   *   currencyCode: "CNY",         // 币种code（可选）
   *   summaryEnabled: false,      // 是否按sku汇总返回（可选）
   *   orderStatus: "Disbursed"     // 交易状态（可选）
   * }
   */
  fastify.post('/profit-report/sku/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getSkuProfitReport(accountId, params);

      return {
        success: true,
        message: '获取利润报表-SKU成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取利润报表-SKU错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润报表-SKU失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有利润报表-SKU数据（自动处理分页）
   * POST /api/lingxing/finance/profit-report/sku/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   startDate: "2023-09-21",    // 开始日期（必填）
   *   endDate: "2023-10-20",      // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   mids: [2],                  // 站点id数组
   *   sids: [110],                // 店铺id数组
   *   monthlyQuery: false,        // 是否按月查询
   *   searchField: "local_sku",   // 搜索值类型
   *   searchValue: ["#"],         // 搜索的值数组
   *   currencyCode: "CNY",        // 币种code
   *   summaryEnabled: false,      // 是否按sku汇总返回
   *   orderStatus: "Disbursed",   // 交易状态
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000，最大10000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/profit-report/sku/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllSkuProfitReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有利润报表-SKU数据成功',
        data: result.profitReports,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有利润报表-SKU数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有利润报表-SKU数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润报表-店铺
   * POST /api/lingxing/finance/profit-report/seller/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选，默认0）
   *   length: 1000,                // 分页长度，上限10000（可选，默认1000）
   *   mids: [2],                   // 站点id数组（可选）
   *   sids: [110],                 // 店铺id数组（可选）
   *   monthlyQuery: false,         // 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   startDate: "2023-09-21",     // 开始时间【结算时间，双闭区间】（必填）
   *   endDate: "2023-10-20",       // 结束时间【结算时间，双闭区间】（必填）
   *   currencyCode: "CNY",         // 币种code【默认原币种】（可选）
   *   summaryEnabled: false,      // 是否按店铺汇总返回（可选）
   *   orderStatus: "Disbursed"     // 交易状态（可选）
   * }
   */
  fastify.post('/profit-report/seller/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getSellerProfitReport(accountId, params);

      return {
        success: true,
        message: '获取利润报表-店铺成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取利润报表-店铺错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润报表-店铺失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有利润报表-店铺数据（自动处理分页）
   * POST /api/lingxing/finance/profit-report/seller/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   startDate: "2023-09-21",    // 开始日期（必填）
   *   endDate: "2023-10-20",      // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   mids: [2],                  // 站点id数组
   *   sids: [110],                // 店铺id数组
   *   monthlyQuery: false,         // 是否按月查询
   *   currencyCode: "CNY",         // 币种code
   *   summaryEnabled: false,      // 是否按店铺汇总返回
   *   orderStatus: "Disbursed",   // 交易状态
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000，最大10000）
   *     delayBetweenPages: 500,    // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/profit-report/seller/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllSellerProfitReport(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有利润报表-店铺数据成功',
        data: result.profitReports,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有利润报表-店铺数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有利润报表-店铺数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询利润报表-店铺月度汇总
   * POST /api/lingxing/finance/profit-report/seller/summary/:accountId
   * Body: {
   *   mids: [2],                   // 站点id数组（可选）
   *   sids: [110],                 // 店铺id数组（可选）
   *   monthlyQuery: false,         // 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   startDate: "2023-09-21",     // 开始时间【结算时间，双闭区间】（必填）
   *   endDate: "2023-10-20",       // 结束时间【结算时间，双闭区间】（必填）
   *   currencyCode: "CNY",         // 币种code（可选）
   *   orderStatus: "Disbursed"     // 交易状态（可选）
   * }
   * 注意：该接口不支持分页，返回汇总数据数组
   */
  fastify.post('/profit-report/seller/summary/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const summaryData = await lingxingFinanceService.getSellerProfitSummary(accountId, params);

      return {
        success: true,
        message: '获取利润报表-店铺月度汇总成功',
        data: summaryData,
        total: summaryData.length
      };
    } catch (error) {
      fastify.log.error('获取利润报表-店铺月度汇总错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取利润报表-店铺月度汇总失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询FBA成本计价流水
   * POST /api/lingxing/finance/cost-stream/fba/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量，默认0（可选）
   *   length: 200,                  // 分页长度，默认200条（可选）
   *   wh_names: ["8P-US-2024美国仓"], // 仓库名数组（可选）
   *   shop_names: ["8P-US-2024"],   // 店铺名数组（可选）
   *   skus: ["LX-0035"],            // sku数组（可选）
   *   mskus: ["HOLDER001"],         // msku数组（可选）
   *   disposition_types: [1,2,3],  // 库存属性数组：1 可用在途，2 可用，3 次品（可选）
   *   business_types: [1],          // 出入库类型数组（必填）
   *   query_type: "01",             // 日期查询类型：01 库存动作日期，02 结算日期（必填）
   *   start_date: "2023-06-01",    // 起始日期，Y-m-d，不允许跨月（必填）
   *   end_date: "2023-06-30",       // 结束日期，Y-m-d，不允许跨月（必填）
   *   business_numbers: ["FBA176M2LL9C"], // 业务编号数组（可选）
   *   origin_accounts: ["SP230413004"]    // 源头单据号数组（可选）
   * }
   */
  fastify.post('/cost-stream/fba/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getFbaCostStream(accountId, params);

      return {
        success: true,
        message: '获取FBA成本计价流水成功',
        data: result.data,
        total: result.total,
        size: result.size,
        current: result.current
      };
    } catch (error) {
      fastify.log.error('获取FBA成本计价流水错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取FBA成本计价流水失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA成本计价流水数据（自动处理分页）
   * POST /api/lingxing/finance/cost-stream/fba/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   business_types: [1],          // 出入库类型数组（必填）
   *   query_type: "01",             // 日期查询类型（必填）
   *   start_date: "2023-06-01",     // 起始日期（必填）
   *   end_date: "2023-06-30",       // 结束日期（必填）
   *   
   *   // 筛选参数（可选）
   *   wh_names: ["8P-US-2024美国仓"], // 仓库名数组
   *   shop_names: ["8P-US-2024"],   // 店铺名数组
   *   skus: ["LX-0035"],            // sku数组
   *   mskus: ["HOLDER001"],         // msku数组
   *   disposition_types: [1,2,3],  // 库存属性数组
   *   business_numbers: ["FBA176M2LL9C"], // 业务编号数组
   *   origin_accounts: ["SP230413004"],  // 源头单据号数组
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 200,              // 每页大小（可选，默认200，最大10000）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/cost-stream/fba/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllFbaCostStream(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA成本计价流水数据成功',
        data: result.costStreams,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA成本计价流水数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA成本计价流水数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询广告发票列表
   * POST /api/lingxing/finance/ads-invoice/list/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量，默认值0（可选）
   *   length: 20,                   // 分页大小，默认20（可选）
   *   sids: [1, 2, 136, 139],       // 店铺id数组（可选）
   *   mids: [1, 2],                 // 国家id数组（可选）
   *   ads_type: ["SPONSORED PRODUCTS"], // 广告类型数组（可选）
   *   invoice_start_time: "2024-06-01", // 开始时间【发票开具时间】（必填）
   *   invoice_end_time: "2024-07-20",   // 结束时间【发票开具时间】（必填）
   *   search_type: "invoice_id",    // 搜索类型（可选）
   *   search_value: "TRF6K4FJN-1"  // 搜索值（可选）
   * }
   */
  fastify.post('/ads-invoice/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getAdsInvoiceList(accountId, params);

      return {
        success: true,
        message: '获取广告发票列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取广告发票列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取广告发票列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有广告发票列表数据（自动处理分页）
   * POST /api/lingxing/finance/ads-invoice/list/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（必填）
   *   invoice_start_time: "2024-06-01", // 开始时间（必填）
   *   invoice_end_time: "2024-07-20",   // 结束时间（必填）
   *   
   *   // 筛选参数（可选）
   *   sids: [1, 2, 136, 139],       // 店铺id数组
   *   mids: [1, 2],                 // 国家id数组
   *   ads_type: ["SPONSORED PRODUCTS"], // 广告类型数组
   *   search_type: "invoice_id",    // 搜索类型
   *   search_value: "TRF6K4FJN-1",  // 搜索值
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,               // 每页大小（可选，默认20，最大10000）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/ads-invoice/list/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllAdsInvoices(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有广告发票列表数据成功',
        data: result.invoices,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有广告发票列表数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有广告发票列表数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询广告发票活动列表
   * POST /api/lingxing/finance/ads-invoice/campaign/list/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量，默认值0（可选）
   *   length: 20,                   // 分页大小，默认20（可选）
   *   invoice_id: "TRF6K4FJN-1",   // 广告发票编号（必填）
   *   sid: 136,                     // 店铺id（必填）
   *   ads_type: ["SPONSORED PRODUCTS"], // 广告类型数组（可选）
   *   search_type: "ads_campaign", // 搜索类型（可选）
   *   search_value: "CD3433L1"     // 搜索值（可选）
   * }
   */
  fastify.post('/ads-invoice/campaign/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getAdsInvoiceCampaignList(accountId, params);

      return {
        success: true,
        message: '获取广告发票活动列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取广告发票活动列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取广告发票活动列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询广告发票基本信息
   * POST /api/lingxing/finance/ads-invoice/detail/:accountId
   * Body: {
   *   invoice_id: "TRF6K4FJN-1",   // 广告发票编号（必填）
   *   sid: 136                      // 店铺id（必填）
   * }
   */
  fastify.post('/ads-invoice/detail/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const invoiceDetail = await lingxingFinanceService.getAdsInvoiceDetail(accountId, params);

      return {
        success: true,
        message: '获取广告发票基本信息成功',
        data: invoiceDetail
      };
    } catch (error) {
      fastify.log.error('获取广告发票基本信息错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取广告发票基本信息失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款单列表
   * POST /api/lingxing/finance/request-funds/order/list/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量，默认0（可选）
   *   length: 20,                  // 分页长度，默认20，上限200（可选）
   *   status: 1,                    // 状态：1 待付款，2 已完成，3 已作废，121 待审批，122 已驳回，124 已作废（可选）
   *   search_field_time: "apply_time", // 搜索时间类型：apply_time 申请时间，real_pay_time 实际付款时间，prepay_time 预计付款时间（可选）
   *   start_date: "2023-06-01",    // 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   end_date: "2023-08-01",       // 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   search_field: "order_sn",    // 搜索字段：purchase_order_sn 关联单据，order_sn 请款单号（可选）
   *   search_value: "RF230801011"  // 搜索值（可选）
   * }
   */
  fastify.post('/request-funds/order/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsOrderList(accountId, params);

      return {
        success: true,
        message: '获取请款单列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款单列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款单列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款单列表数据（自动处理分页）
   * POST /api/lingxing/finance/request-funds/order/list/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   status: 1,                    // 状态
   *   search_field_time: "apply_time", // 搜索时间类型
   *   start_date: "2023-06-01",    // 开始时间
   *   end_date: "2023-08-01",       // 结束时间
   *   search_field: "order_sn",    // 搜索字段
   *   search_value: "RF230801011",  // 搜索值
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,               // 每页大小（可选，默认20，最大200）
   *     delayBetweenPages: 500,      // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/request-funds/order/list/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsOrders(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款单列表数据成功',
        data: result.requestFundsOrders,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款单列表数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款单列表数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款池 - 货款现结
   * POST /api/lingxing/finance/request-funds-pool/purchase/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选）
   *   length: 20,                  // 分页长度（可选，默认20，上限200）
   *   pay_status: "0,1",           // 支付状态（可选，多个用逗号分隔）
   *   time_field: "create_time",   // 时间搜索类型（可选）
   *   start_time: "2024-07-25",    // 开始时间（可选）
   *   end_time: "2024-07-25",      // 结束时间（可选）
   *   search_field: "order_sn",    // 搜索类型（可选）
   *   search_value: "PO240725048"  // 查询值（可选）
   * }
   */
  fastify.post('/request-funds-pool/purchase/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsPoolPurchaseList(accountId, body);

      return {
        success: true,
        message: '获取请款池-货款现结列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款池-货款现结列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款池-货款现结列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款池-货款现结数据
   * POST /api/lingxing/finance/request-funds-pool/purchase/fetch-all/:accountId
   */
  fastify.post('/request-funds-pool/purchase/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsPoolPurchase(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款池-货款现结数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款池-货款现结数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款池-货款现结数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款池 - 货款月结
   * POST /api/lingxing/finance/request-funds-pool/inbound/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选）
   *   length: 20,                  // 分页长度（可选，默认20，上限200）
   *   pay_status: 0,               // 状态（可选）
   *   time_field: "create_time",   // 时间搜索类型（可选）
   *   start_time: "2024-07-25",    // 开始时间（可选）
   *   end_time: "2024-07-25",      // 结束时间（可选）
   *   search_field: "order_sn",    // 搜索类型（可选）
   *   search_value: "IB240725029"  // 搜索值（可选）
   * }
   */
  fastify.post('/request-funds-pool/inbound/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsPoolInboundList(accountId, body);

      return {
        success: true,
        message: '获取请款池-货款月结列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款池-货款月结列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款池-货款月结列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款池-货款月结数据
   * POST /api/lingxing/finance/request-funds-pool/inbound/fetch-all/:accountId
   */
  fastify.post('/request-funds-pool/inbound/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsPoolInbound(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款池-货款月结数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款池-货款月结数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款池-货款月结数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款池 - 货款预付款
   * POST /api/lingxing/finance/request-funds-pool/prepay/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选）
   *   length: 20,                  // 分页长度（可选，默认20，上限200）
   *   pay_status: 3,               // 支付状态（可选）
   *   start_time: "2024-07-23",    // 开始时间（可选）
   *   end_time: "2024-07-25",      // 结束时间（可选）
   *   time_field: "create_time",   // 时间搜索类型（可选）
   *   search_field: "order_sn",    // 搜索类型（可选）
   *   search_value: "PRE240724001" // 搜索值（可选）
   * }
   */
  fastify.post('/request-funds-pool/prepay/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsPoolPrepayList(accountId, body);

      return {
        success: true,
        message: '获取请款池-货款预付款列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款池-货款预付款列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款池-货款预付款列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款池-货款预付款数据
   * POST /api/lingxing/finance/request-funds-pool/prepay/fetch-all/:accountId
   */
  fastify.post('/request-funds-pool/prepay/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsPoolPrepay(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款池-货款预付款数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款池-货款预付款数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款池-货款预付款数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款池-物流请款
   * POST /api/lingxing/finance/request-funds-pool/logistics/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选）
   *   length: 20,                  // 分页长度（可选，默认20，上限200）
   *   search_field_time: "create_time", // 时间搜索类型（可选）
   *   start_time: "2024-07-23",    // 开始时间（可选）
   *   end_time: "2024-07-25",      // 结束时间（可选）
   *   search_field: "order_sn",    // 搜索类型（可选）
   *   search_value: "SP240719005"   // 搜索值（可选）
   * }
   */
  fastify.post('/request-funds-pool/logistics/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsPoolLogisticsList(accountId, body);

      return {
        success: true,
        message: '获取请款池-物流请款列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款池-物流请款列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款池-物流请款列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款池-物流请款数据
   * POST /api/lingxing/finance/request-funds-pool/logistics/fetch-all/:accountId
   */
  fastify.post('/request-funds-pool/logistics/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsPoolLogistics(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款池-物流请款数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款池-物流请款数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款池-物流请款数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款池-其他应付款
   * POST /api/lingxing/finance/request-funds-pool/custom-fee/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选）
   *   length: 20,                  // 分页长度（可选，默认20，上限200）
   *   pay_status: "3",             // 支付状态（可选，多个用逗号分隔）
   *   search_field_time: "create_time", // 时间搜索类型（可选）
   *   start_time: "2024-07-23",    // 开始时间（可选）
   *   end_time: "2024-07-25",      // 结束时间（可选）
   *   search_field: "business_sn", // 搜索类型（可选）
   *   search_value: "FY240703000001" // 搜索值（可选）
   * }
   */
  fastify.post('/request-funds-pool/custom-fee/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsPoolCustomFeeList(accountId, body);

      return {
        success: true,
        message: '获取请款池-其他应付款列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款池-其他应付款列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款池-其他应付款列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款池-其他应付款数据
   * POST /api/lingxing/finance/request-funds-pool/custom-fee/fetch-all/:accountId
   */
  fastify.post('/request-funds-pool/custom-fee/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsPoolCustomFee(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款池-其他应付款数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款池-其他应付款数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款池-其他应付款数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询请款池-其他费用
   * POST /api/lingxing/finance/request-funds-pool/other-fee/:accountId
   * Body: {
   *   offset: 0,                    // 分页偏移量（可选）
   *   length: 20,                  // 分页长度（可选，默认20，上限200）
   *   startTime: "2024-01-01",     // 开始时间（必填，格式：yyyy-MM-dd）
   *   endTime: "2024-12-31",       // 结束时间（必填，格式：yyyy-MM-dd）
   *   searchFieldTime: "create_time", // 时间维度（可选）
   *   searchField: "order_sn",     // 搜索字段（可选）
   *   searchValue: "PO20240101",   // 搜索值（可选）
   *   status: 0,                   // 付款状态（可选）
   *   purchaserIds: [1234567890],  // 采购方ID列表（可选）
   *   supplierIds: [1234567890]   // 应付对象ID列表（可选）
   * }
   */
  fastify.post('/request-funds-pool/other-fee/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getRequestFundsPoolOtherFeeList(accountId, body);

      return {
        success: true,
        message: '获取请款池-其他费用列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取请款池-其他费用列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取请款池-其他费用列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有请款池-其他费用数据
   * POST /api/lingxing/finance/request-funds-pool/other-fee/fetch-all/:accountId
   */
  fastify.post('/request-funds-pool/other-fee/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllRequestFundsPoolOtherFee(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有请款池-其他费用数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有请款池-其他费用数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有请款池-其他费用数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 应收报告-列表查询
   * POST /api/lingxing/finance/receivable-report/list/:accountId
   * Body: {
   *   settleMonth: "2023-01",      // 结算月（必填，格式：Y-m）
   *   sids: [109, 123],            // 店铺id列表（可选）
   *   mids: [1],                   // 国家id列表（可选）
   *   currencyCode: "US",          // 币种code（可选）
   *   archiveStatus: 0,            // 对账状态（可选）
   *   sortField: "beginningBalanceCurrencyAmount", // 排序字段（可选）
   *   sortType: "desc",            // 排序规则（可选）
   *   receivedState: 1,            // 转账/到账金额（可选）
   *   offset: 0,                   // 分页偏移量（可选）
   *   length: 20                   // 分页长度（可选，默认20）
   * }
   */
  fastify.post('/receivable-report/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getReceivableReportList(accountId, body);

      return {
        success: true,
        message: '获取应收报告列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取应收报告列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取应收报告列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有应收报告列表数据
   * POST /api/lingxing/finance/receivable-report/list/fetch-all/:accountId
   */
  fastify.post('/receivable-report/list/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllReceivableReportList(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有应收报告列表数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有应收报告列表数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有应收报告列表数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 应收报告-详情-列表
   * POST /api/lingxing/finance/receivable-report/detail/list/:accountId
   * Body: {
   *   sid: 6,                      // 店铺id（必填）
   *   currencyCode: "CNY",         // 币种code（必填）
   *   settleMonth: "2023-01",     // 结算月（必填）
   *   searchField: "fid",          // 搜索值类型（可选）
   *   searchValue: "UNOFZ3UMA1DZ", // 搜索值（可选）
   *   offset: 0,                   // 偏移量（可选）
   *   length: 200                  // 分页长度（可选，默认20）
   * }
   */
  fastify.post('/receivable-report/detail/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getReceivableReportDetailList(accountId, body);

      return {
        success: true,
        message: '获取应收报告详情列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取应收报告详情列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取应收报告详情列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有应收报告详情列表数据
   * POST /api/lingxing/finance/receivable-report/detail/list/fetch-all/:accountId
   */
  fastify.post('/receivable-report/detail/list/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingFinanceService.fetchAllReceivableReportDetailList(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有应收报告详情列表数据成功',
        data: result.data,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有应收报告详情列表数据错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有应收报告详情列表数据失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 应收报告-详情-基础信息
   * POST /api/lingxing/finance/receivable-report/detail/info/:accountId
   * Body: {
   *   sid: 1,                      // 店铺id（必填）
   *   currencyCode: "CNY",         // 币种code（必填）
   *   settleMonth: "2023-01"      // 结算月（必填）
   * }
   */
  fastify.post('/receivable-report/detail/info/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};

    try {
      const result = await lingxingFinanceService.getReceivableReportDetailInfo(accountId, body);

      return {
        success: true,
        message: '获取应收报告详情基础信息成功',
        data: result.data
      };
    } catch (error) {
      fastify.log.error('获取应收报告详情基础信息错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取应收报告详情基础信息失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingFinanceRoutes;

