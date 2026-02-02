import lingxingWarehouseService from '../services/lingxing/warehouse/lingXingWarehouseService.js';

/**
 * 领星ERP仓库路由插件
 * 仓库相关接口
 */
async function lingxingWarehouseRoutes(fastify, options) {
  /**
   * 查询仓库列表
   * POST /api/lingxing/warehouse/list/:accountId
   * Body: {
   *   type: 1,              // 仓库类型（可选，默认1）：1 本地仓，3 海外仓，4 亚马逊平台仓，6 AWD仓
   *   sub_type: 2,          // 海外仓子类型（可选，只在type=3生效）：1 无API海外仓，2 有API海外仓
   *   is_delete: 0,         // 是否删除（可选，默认0）：0 未删除，1 已删除
   *   offset: 0,            // 分页偏移量（可选，默认0）
   *   length: 1000           // 分页长度（可选，默认1000）
   * }
   * 支持查询【设置】>【仓库设置】仓库列表
   */
  fastify.post('/list/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getWarehouseList(accountId, params);

      return {
        success: true,
        message: '获取仓库列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取仓库列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取仓库列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有仓库列表
   * POST /api/lingxing/warehouse/fetch-all/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   type: 1,              // 仓库类型（可选）
   *   sub_type: 2,          // 海外仓子类型（可选）
   *   is_delete: 0,         // 是否删除（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 1000,           // 每页大小（可选，默认1000）
   *     delayBetweenPages: 500,  // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllWarehouses(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有仓库列表成功',
        data: result.warehouses,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有仓库列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有仓库列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询本地仓位列表
   * POST /api/lingxing/warehouse/bins/:accountId
   * Body: {
   *   wid: "5",              // 仓库ID，字符串id，多个使用英文逗号分隔（可选）
   *   id: "13",              // 仓位ID，字符串id，多个使用英文逗号分隔（可选）
   *   status: "2",           // 仓位状态：1 禁用，2 启用（可选）
   *   type: "5",             // 仓位类型：5 可用，6 次品（可选）
   *   offset: 0,              // 分页偏移量，默认为0（可选）
   *   limit: 20               // 限制条数，默认20条（可选）
   * }
   */
  fastify.post('/bins/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getWarehouseBinList(accountId, params);

      return {
        success: true,
        message: '获取仓位列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取仓位列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取仓位列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有仓位列表
   * POST /api/lingxing/warehouse/fetch-all-bins/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   wid: "5",              // 仓库ID（可选）
   *   id: "13",              // 仓位ID（可选）
   *   status: "2",           // 仓位状态（可选）
   *   type: "5",             // 仓位类型（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,              // 每页大小（可选，默认20）
   *     delayBetweenPages: 500,     // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-bins/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllWarehouseBins(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有仓位列表成功',
        data: result.bins,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有仓位列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有仓位列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询产品仓位列表
   * POST /api/lingxing/warehouse/entry-recommend-bins/:accountId
   * Body: {
   *   list: [                    // 产品列表（必填）
   *     {
   *       wid: "1",               // 仓库id（必填）
   *       productId: "39258",     // 产品id（必填）
   *       fnsku: "",              // fnsku（可选）
   *       sid: "103"              // 店铺id（可选）
   *     }
   *   ],
   *   withHistory: false          // 是否查询历史仓位（可选，默认false）
   * }
   * 用于查询产品在仓库中的可用仓位和次品仓位列表
   */
  fastify.post('/entry-recommend-bins/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getEntryRecommendBinList(accountId, params);

      return {
        success: true,
        message: '获取产品仓位列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取产品仓位列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('必须包含'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取产品仓位列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动批量拉取所有产品仓位列表
   * POST /api/lingxing/warehouse/fetch-all-entry-recommend-bins/:accountId
   * Body: {
   *   productList: [              // 产品列表（必填）
   *     {
   *       wid: "1",               // 仓库id（必填）
   *       productId: "39258",    // 产品id（必填）
   *       fnsku: "",              // fnsku（可选）
   *       sid: "103"              // 店铺id（可选）
   *     }
   *   ],
   *   options: {
   *     batchSize: 50,                    // 每批查询的产品数量（可选，默认50）
   *     delayBetweenBatches: 500,         // 批次之间延迟（毫秒，可选，默认500）
   *     withHistory: false                // 是否查询历史仓位（可选，默认false）
   *   }
   * }
   * 自动将产品列表分批查询，避免单次请求过大
   */
  fastify.post('/fetch-all-entry-recommend-bins/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { productList, options = {} } = body;

    if (!productList || !Array.isArray(productList) || productList.length === 0) {
      return reply.code(400).send({
        success: false,
        message: 'productList 为必填参数，且必须是非空数组'
      });
    }

    try {
      const result = await lingxingWarehouseService.fetchAllEntryRecommendBins(accountId, productList, options);

      return {
        success: true,
        message: '自动批量拉取产品仓位列表成功',
        data: result.binList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动批量拉取产品仓位列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && (error.message.includes('必填') || error.message.includes('必须包含'))) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动批量拉取产品仓位列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询FBA库存列表-v2
   * POST /api/lingxing/warehouse/fba-details/:accountId
   * Body: {
   *   offset: 0,                              // 分页偏移量（可选，默认0）
   *   length: 20,                             // 分页长度（可选，默认20，取值范围[20,200]）
   *   search_field: "seller_sku",             // 搜索维度（可选）
   *   search_value: "MSKUFDA5E30",            // 搜索值（可选）
   *   cid: "value",                           // 分类（可选）
   *   bid: "value",                           // 品牌（可选）
   *   attribute: "value",                      // 属性（可选）
   *   asin_principal: "value",                // Listing负责人uid（可选，多个使用,分隔）
   *   status: "1",                            // 在售状态（可选）：0 停售，1 在售
   *   senior_search_list: "[{...}]",          // 高级搜索列表（可选，JSON字符串）
   *   fulfillment_channel_type: "FBA",        // 配送方式（可选）：FBA, FBM
   *   is_hide_zero_stock: "0",                // 是否隐藏零库存行（可选）：0 不隐藏，1 隐藏
   *   is_parant_asin_merge: "0",              // 是否合并父ASIN（可选）：0 不合并，1 合并
   *   is_contain_del_ls: "0",                 // 是否显示已删除Listing（可选）：0 不显示，1 显示
   *   query_fba_storage_quantity_list: true   // 是否查询FBA可售信息列表（可选，Boolean，默认false）
   * }
   * 支持查询FBA库存，对应系统【仓库】>【FBA库存明细】数据,数量维度展示
   */
  fastify.post('/fba-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getFbaWarehouseDetailList(accountId, params);

      return {
        success: true,
        message: '获取FBA库存列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取FBA库存列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取FBA库存列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有FBA库存列表
   * POST /api/lingxing/warehouse/fetch-all-fba-details/:accountId
   * Body: {
   *   // 筛选参数（可选，所有 getFbaWarehouseDetailList 支持的筛选参数）
   *   search_field: "seller_sku",
   *   search_value: "MSKUFDA5E30",
   *   cid: "value",
   *   bid: "value",
   *   // ... 其他筛选参数
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 200,                 // 每页大小（可选，默认200，最大200）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-fba-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllFbaWarehouseDetails(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有FBA库存列表成功',
        data: result.fbaInventoryList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有FBA库存列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有FBA库存列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询仓库库存明细
   * POST /api/lingxing/warehouse/inventory-details/:accountId
   * Body: {
   *   wid: "1,578,765",          // 仓库id，多个使用英文逗号分隔（可选）
   *   offset: 0,                  // 分页偏移量（可选，默认0）
   *   length: 20,                 // 分页长度（可选，默认20，上限800）
   *   sku: "Test01"               // SKU，单个,（模糊搜索）（可选）
   * }
   * 支持查询本地仓/海外仓库存明细，对应系统【仓库】>【库存明细】数据
   */
  fastify.post('/inventory-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getInventoryDetailsList(accountId, params);

      return {
        success: true,
        message: '获取库存明细列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取库存明细列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取库存明细列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有仓库库存明细
   * POST /api/lingxing/warehouse/fetch-all-inventory-details/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   wid: "1,578,765",          // 仓库id（可选）
   *   sku: "Test01",              // SKU（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 800,                 // 每页大小（可选，默认800，最大800）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-inventory-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllInventoryDetails(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有库存明细列表成功',
        data: result.inventoryDetailsList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有库存明细列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有库存明细列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询仓位库存明细
   * POST /api/lingxing/warehouse/inventory-bin-details/:accountId
   * Body: {
   *   wid: "1,5,18",              // 仓库id，多个仓库用英文逗号分隔（可选）
   *   bin_type_list: "5",         // 仓位类型，多个类型用英文逗号分隔（可选）
   *                                 // 1 待检暂存，2 可用暂存，3 次品暂存，4 拣货暂存，5 可用，6 次品
   *   offset: 0,                   // 分页偏移量（可选，默认0）
   *   length: 20                   // 分页长度（可选，默认20，上限500）
   * }
   */
  fastify.post('/inventory-bin-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getInventoryBinDetailsList(accountId, params);

      return {
        success: true,
        message: '获取仓位库存明细列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取仓位库存明细列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取仓位库存明细列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有仓位库存明细
   * POST /api/lingxing/warehouse/fetch-all-inventory-bin-details/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   wid: "1,5,18",              // 仓库id（可选）
   *   bin_type_list: "5",         // 仓位类型（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 500,                 // 每页大小（可选，默认500，最大500）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-inventory-bin-details/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllInventoryBinDetails(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有仓位库存明细列表成功',
        data: result.inventoryBinDetailsList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有仓位库存明细列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有仓位库存明细列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询库存流水（新）
   * POST /api/lingxing/warehouse/inventory-statements/:accountId
   * Body: {
   *   offset: 0,                      // 分页偏移量（必填，默认0）
   *   length: 20,                     // 分页长度（必填，默认20）
   *   wids: "1,578,765",              // 仓库id，多个使用英文逗号分隔（可选）
   *   types: "19",                    // 流水类型，多个使用英文逗号分隔（可选）
   *   sub_types: "1901",              // 子类流水类型，多个使用英文逗号分隔（可选）
   *   start_date: "2024-06-29",       // 操作开始时间，格式：Y-m-d（可选）
   *   end_date: "2024-07-29"          // 操作结束时间，格式：Y-m-d（可选）
   * }
   */
  fastify.post('/inventory-statements/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getWarehouseInventoryStatementList(accountId, params);

      return {
        success: true,
        message: '获取库存流水列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取库存流水列表错误:', error);
      
      // 处理参数验证错误
      if (error.message && error.message.includes('必填')) {
        return reply.code(400).send({
          success: false,
          message: error.message || '参数错误'
        });
      }

      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取库存流水列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有库存流水
   * POST /api/lingxing/warehouse/fetch-all-inventory-statements/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   wids: "1,578,765",              // 仓库id（可选）
   *   types: "19",                    // 流水类型（可选）
   *   sub_types: "1901",             // 子类流水类型（可选）
   *   start_date: "2024-06-29",       // 操作开始时间（可选）
   *   end_date: "2024-07-29",         // 操作结束时间（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,                 // 每页大小（可选，默认20）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-inventory-statements/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllWarehouseInventoryStatements(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有库存流水列表成功',
        data: result.statementList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有库存流水列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有库存流水列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 查询仓位流水
   * POST /api/lingxing/warehouse/bin-statements/:accountId
   * Body: {
   *   wid: "1,578,765",              // 仓库ID，多个仓库ID用英文逗号,分隔（可选）
   *   type: "19",                    // 流水类型，多个流水类型用英文逗号分隔（可选）
   *   bin_type_list: "5",            // 仓位类型，多个类型用逗号分隔（可选）
   *                                  // 1 待检暂存，2 可用暂存，3 次品暂存，4 拣货暂存，5 可用，6 次品
   *   start_date: "2022-01-30",      // 操作开始时间，Y-m-d（可选）
   *   end_date: "2024-06-01",        // 操作结束时间，Y-m-d（可选）
   *   offset: 0,                     // 分页偏移量（可选，默认0）
   *   length: 20                     // 分页长度（可选，默认20）
   * }
   */
  fastify.post('/bin-statements/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const params = request.body || {};

    try {
      const result = await lingxingWarehouseService.getWarehouseBinStatementList(accountId, params);

      return {
        success: true,
        message: '获取仓位流水列表成功',
        data: result.data,
        total: result.total
      };
    } catch (error) {
      fastify.log.error('获取仓位流水列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '获取仓位流水列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });

  /**
   * 自动拉取所有仓位流水
   * POST /api/lingxing/warehouse/fetch-all-bin-statements/:accountId
   * Body: {
   *   // 筛选参数（可选）
   *   wid: "1,578,765",              // 仓库ID（可选）
   *   type: "19",                    // 流水类型（可选）
   *   bin_type_list: "5",            // 仓位类型（可选）
   *   start_date: "2022-01-30",      // 操作开始时间（可选）
   *   end_date: "2024-06-01",        // 操作结束时间（可选）
   *   
   *   // 选项（可选）
   *   options: {
   *     pageSize: 20,                 // 每页大小（可选，默认20）
   *     delayBetweenPages: 500,        // 分页之间延迟（毫秒，可选，默认500）
   *   }
   * }
   */
  fastify.post('/fetch-all-bin-statements/:accountId', async (request, reply) => {
    const { accountId } = request.params;
    const body = request.body || {};
    const { options, ...filterParams } = body;

    try {
      const result = await lingxingWarehouseService.fetchAllWarehouseBinStatements(accountId, filterParams, options || {});

      return {
        success: true,
        message: '自动拉取所有仓位流水列表成功',
        data: result.binStatementList,
        total: result.total,
        stats: result.stats
      };
    } catch (error) {
      fastify.log.error('自动拉取所有仓位流水列表错误:', error);
      
      reply.code(error.code === '3001008' ? 429 : 500).send({
        success: false,
        message: error.message || '自动拉取所有仓位流水列表失败',
        code: error.code,
        description: error.description,
        action: error.action
      });
    }
  });
}

export default lingxingWarehouseRoutes;

