import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';

/**
 * 领星ERP产品服务
 * 产品管理相关接口
 */
class LingXingProductService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询本地产品列表
   * API: POST /erp/sc/routing/data/local_inventory/productList
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0
   *   - length: 分页长度，默认1000，上限1000
   *   - update_time_start: 更新时间-开始时间【时间戳，单位：秒】
   *   - update_time_end: 更新时间-结束时间【时间戳，单位：秒】
   *   - create_time_start: 创建时间-开始时间【时间戳，单位：秒】
   *   - create_time_end: 创建时间-结束时间【时间戳，单位：秒】
   *   - sku_list: 本地产品sku数组
   *   - sku_identifier_list: sku识别码列表数组
   * @param {boolean} useCache - 是否优先使用缓存数据（默认false，因为产品数据变更频繁）
   * @returns {Promise<Object>} 产品列表数据 { data: [], total: 0 }
   */
  async getLocalProductList(accountId, params = {}, useCache = false) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {
        offset: params.offset || 0,
        length: params.length || 1000,
        ...(params.update_time_start && { update_time_start: params.update_time_start }),
        ...(params.update_time_end && { update_time_end: params.update_time_end }),
        ...(params.create_time_start && { create_time_start: params.create_time_start }),
        ...(params.create_time_end && { create_time_end: params.create_time_end }),
        ...(params.sku_list && params.sku_list.length > 0 && { sku_list: params.sku_list }),
        ...(params.sku_identifier_list && params.sku_identifier_list.length > 0 && { sku_identifier_list: params.sku_identifier_list })
      };

      // 如果使用缓存，先尝试从数据库获取
      if (useCache) {
        const cachedProducts = await this.getLocalProductsFromDB(accountId, params);
        if (cachedProducts && cachedProducts.length > 0) {
          console.log('从缓存获取产品列表');
          return {
            data: cachedProducts,
            total: cachedProducts.length
          };
        }
      }

      // 调用API获取产品列表（使用通用客户端，成功码为0）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/productList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取产品列表失败');
      }

      const products = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (products.length > 0) {
        await this.saveLocalProducts(accountId, products);
      }

      return {
        data: products,
        total: total
      };
    } catch (error) {
      console.error('获取本地产品列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedProducts = await this.getLocalProductsFromDB(accountId, params);
        if (cachedProducts && cachedProducts.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return {
            data: cachedProducts,
            total: cachedProducts.length
          };
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存产品列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} products - 产品列表数据
   */
  async saveLocalProducts(accountId, products) {
    try {
      for (const product of products) {
        await prisma.lingXingLocalProduct.upsert({
          where: {
            productId: product.id
          },
          update: {
            accountId: accountId,
            cid: product.cid,
            categoryName: product.category_name,
            bid: product.bid,
            brandName: product.brand_name,
            sku: product.sku,
            openStatus: product.open_status,
            skuIdentifier: product.sku_identifier,
            productName: product.product_name,
            picUrl: product.pic_url,
            psId: product.ps_id,
            spu: product.spu,
            cgDelivery: product.cg_delivery,
            cgTransportCosts: product.cg_transport_costs ? parseFloat(product.cg_transport_costs) : null,
            purchaseRemark: product.purchase_remark,
            cgPrice: product.cg_price ? parseFloat(product.cg_price) : null,
            status: product.status,
            statusText: product.status_text,
            isCombo: product.is_combo,
            createTime: product.create_time,
            updateTime: product.update_time,
            productDeveloperUid: product.product_developer_uid !== undefined && product.product_developer_uid !== null ? String(product.product_developer_uid) : null,
            productDeveloper: product.product_developer,
            cgOptUid: product.cg_opt_uid !== undefined && product.cg_opt_uid !== null ? String(product.cg_opt_uid) : null,
            cgOptUsername: product.cg_opt_username,
            globalTags: product.global_tags,
            supplierQuote: product.supplier_quote,
            customFields: product.custom_fields,
            attribute: product.attribute,
            data: product,
            updatedAt: new Date()
          },
          create: {
            productId: product.id,
            accountId: accountId,
            cid: product.cid,
            categoryName: product.category_name,
            bid: product.bid,
            brandName: product.brand_name,
            sku: product.sku,
            openStatus: product.open_status,
            skuIdentifier: product.sku_identifier,
            productName: product.product_name,
            picUrl: product.pic_url,
            psId: product.ps_id,
            spu: product.spu,
            cgDelivery: product.cg_delivery,
            cgTransportCosts: product.cg_transport_costs ? parseFloat(product.cg_transport_costs) : null,
            purchaseRemark: product.purchase_remark,
            cgPrice: product.cg_price ? parseFloat(product.cg_price) : null,
            status: product.status,
            statusText: product.status_text,
            isCombo: product.is_combo,
            createTime: product.create_time,
            updateTime: product.update_time,
            productDeveloperUid: product.product_developer_uid !== undefined && product.product_developer_uid !== null ? String(product.product_developer_uid) : null,
            productDeveloper: product.product_developer,
            cgOptUid: product.cg_opt_uid !== undefined && product.cg_opt_uid !== null ? String(product.cg_opt_uid) : null,
            cgOptUsername: product.cg_opt_username,
            globalTags: product.global_tags,
            supplierQuote: product.supplier_quote,
            customFields: product.custom_fields,
            attribute: product.attribute,
            data: product
          }
        });
      }
    } catch (error) {
      console.error('保存产品列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取产品列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   * @returns {Promise<Array>} 产品列表数据
   */
  async getLocalProductsFromDB(accountId, params = {}) {
    try {
      const where = {
        accountId: accountId
      };

      // 添加时间范围筛选
      if (params.update_time_start || params.update_time_end) {
        where.updateTime = {};
        if (params.update_time_start) {
          where.updateTime.gte = parseInt(params.update_time_start);
        }
        if (params.update_time_end) {
          where.updateTime.lte = parseInt(params.update_time_end);
        }
      }

      if (params.create_time_start || params.create_time_end) {
        where.createTime = {};
        if (params.create_time_start) {
          where.createTime.gte = parseInt(params.create_time_start);
        }
        if (params.create_time_end) {
          where.createTime.lte = parseInt(params.create_time_end);
        }
      }

      // 添加SKU筛选
      if (params.sku_list && params.sku_list.length > 0) {
        where.sku = { in: params.sku_list };
      }

      // 添加SKU识别码筛选
      if (params.sku_identifier_list && params.sku_identifier_list.length > 0) {
        where.skuIdentifier = { in: params.sku_identifier_list };
      }

      const products = await prisma.lingXingLocalProduct.findMany({
        where: where,
        skip: params.offset || 0,
        take: params.length || 1000,
        orderBy: { updateTime: 'desc' }
      });

      return products.map(p => ({
        id: p.productId,
        cid: p.cid,
        category_name: p.categoryName,
        bid: p.bid,
        brand_name: p.brandName,
        sku: p.sku,
        open_status: p.openStatus,
        sku_identifier: p.skuIdentifier,
        product_name: p.productName,
        pic_url: p.picUrl,
        ps_id: p.psId,
        spu: p.spu,
        cg_delivery: p.cgDelivery,
        cg_transport_costs: p.cgTransportCosts ? p.cgTransportCosts.toString() : null,
        purchase_remark: p.purchaseRemark,
        cg_price: p.cgPrice ? p.cgPrice.toString() : null,
        status: p.status,
        status_text: p.statusText,
        is_combo: p.isCombo,
        create_time: p.createTime,
        update_time: p.updateTime,
        product_developer_uid: p.productDeveloperUid,
        product_developer: p.productDeveloper,
        cg_opt_uid: p.cgOptUid,
        cg_opt_username: p.cgOptUsername,
        global_tags: p.globalTags,
        supplier_quote: p.supplierQuote,
        custom_fields: p.customFields,
        attribute: p.attribute,
        ...(p.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取产品列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据产品ID获取产品信息
   * @param {number} productId - 产品ID
   * @returns {Promise<Object|null>} 产品信息
   */
  async getLocalProductById(productId) {
    try {
      const product = await prisma.lingXingLocalProduct.findUnique({
        where: { productId: productId }
      });

      if (!product) {
        return null;
      }

      return {
        id: product.productId,
        cid: product.cid,
        category_name: product.categoryName,
        bid: product.bid,
        brand_name: product.brandName,
        sku: product.sku,
        open_status: product.openStatus,
        sku_identifier: product.skuIdentifier,
        product_name: product.productName,
        pic_url: product.picUrl,
        ps_id: product.psId,
        spu: product.spu,
        cg_delivery: product.cgDelivery,
        cg_transport_costs: product.cgTransportCosts ? product.cgTransportCosts.toString() : null,
        purchase_remark: product.purchaseRemark,
        cg_price: product.cgPrice ? product.cgPrice.toString() : null,
        status: product.status,
        status_text: product.statusText,
        is_combo: product.isCombo,
        create_time: product.createTime,
        update_time: product.updateTime,
        product_developer_uid: product.productDeveloperUid,
        product_developer: product.productDeveloper,
        cg_opt_uid: product.cgOptUid,
        cg_opt_username: product.cgOptUsername,
        global_tags: product.globalTags,
        supplier_quote: product.supplierQuote,
        custom_fields: product.customFields,
        attribute: product.attribute,
        ...(product.data || {})
      };
    } catch (error) {
      console.error('根据产品ID获取产品信息失败:', error.message);
      return null;
    }
  }

  /**
   * 根据SKU获取产品信息
   * @param {string} accountId - 领星账户ID
   * @param {string} sku - SKU
   * @returns {Promise<Object|null>} 产品信息
   */
  async getLocalProductBySku(accountId, sku) {
    try {
      const product = await prisma.lingXingLocalProduct.findFirst({
        where: {
          accountId: accountId,
          sku: sku
        }
      });

      if (!product) {
        return null;
      }

      return {
        id: product.productId,
        cid: product.cid,
        category_name: product.categoryName,
        bid: product.bid,
        brand_name: product.brandName,
        sku: product.sku,
        open_status: product.openStatus,
        sku_identifier: product.skuIdentifier,
        product_name: product.productName,
        pic_url: product.picUrl,
        ps_id: product.psId,
        spu: product.spu,
        cg_delivery: product.cgDelivery,
        cg_transport_costs: product.cgTransportCosts ? product.cgTransportCosts.toString() : null,
        purchase_remark: product.purchaseRemark,
        cg_price: product.cgPrice ? product.cgPrice.toString() : null,
        status: product.status,
        status_text: product.statusText,
        is_combo: product.isCombo,
        create_time: product.createTime,
        update_time: product.updateTime,
        product_developer_uid: product.productDeveloperUid,
        product_developer: product.productDeveloper,
        cg_opt_uid: product.cgOptUid,
        cg_opt_username: product.cgOptUsername,
        global_tags: product.globalTags,
        supplier_quote: product.supplierQuote,
        custom_fields: product.customFields,
        attribute: product.attribute,
        ...(product.data || {})
      };
    } catch (error) {
      console.error('根据SKU获取产品信息失败:', error.message);
      return null;
    }
  }

  /**
   * 查询本地产品详细信息
   * API: POST /erp/sc/routing/data/local_inventory/productInfo
   * 对应系统【产品】>【产品管理】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - id: 产品id【产品id、 产品SKU 、SKU识别码 三选一必填】
   *   - sku: 产品SKU【产品id、 产品SKU 、SKU识别码 三选一必填】
   *   - sku_identifier: SKU识别码【产品id、 产品SKU 、SKU识别码 三选一必填】
   * @returns {Promise<Object>} 产品详细信息
   */
  async getLocalProductInfo(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证参数：id、sku、sku_identifier 三选一必填
      if (!params.id && !params.sku && !params.sku_identifier) {
        throw new Error('id、sku、sku_identifier 三选一必填');
      }

      // 构建请求参数
      const requestParams = {};
      if (params.id) {
        requestParams.id = params.id;
      }
      if (params.sku) {
        requestParams.sku = params.sku;
      }
      if (params.sku_identifier) {
        requestParams.sku_identifier = params.sku_identifier;
      }

      // 调用API获取产品详细信息（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/productInfo', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取产品详细信息失败');
      }

      const productInfo = response.data || null;

      // 如果获取到产品详情，保存到数据库
      if (productInfo && productInfo.id) {
        await this.saveLocalProductDetail(accountId, productInfo);
      }

      return productInfo;
    } catch (error) {
      console.error('获取本地产品详细信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存产品详情到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Object} productInfo - 产品详细信息
   */
  async saveLocalProductDetail(accountId, productInfo) {
    try {
      // 确保 productId 是整数类型
      const productId = parseInt(productInfo.id);
      
      if (isNaN(productId)) {
        throw new Error(`无效的产品ID: ${productInfo.id}`);
      }

      // 使用 upsert 更新或创建产品记录，并保存产品详情
      await prisma.lingXingLocalProduct.upsert({
        where: {
          productId: productId
        },
        update: {
          accountId: accountId,
          productDetail: productInfo, // 保存完整的产品详情
          updatedAt: new Date(),
          // 同时更新其他可能变化的字段
          ...(productInfo.sku && { sku: productInfo.sku }),
          ...(productInfo.sku_identifier && { skuIdentifier: productInfo.sku_identifier }),
          ...(productInfo.product_name && { productName: productInfo.product_name }),
          ...(productInfo.pic_url && { picUrl: productInfo.pic_url }),
          ...(productInfo.status !== undefined && { status: typeof productInfo.status === 'string' ? parseInt(productInfo.status) : productInfo.status }),
          ...(productInfo.is_combo !== undefined && { isCombo: typeof productInfo.is_combo === 'string' ? parseInt(productInfo.is_combo) : productInfo.is_combo }),
          ...(productInfo.cid !== undefined && { cid: typeof productInfo.cid === 'string' ? parseInt(productInfo.cid) : productInfo.cid }),
          ...(productInfo.category_name && { categoryName: productInfo.category_name }),
          ...(productInfo.bid !== undefined && { bid: typeof productInfo.bid === 'string' ? parseInt(productInfo.bid) : productInfo.bid }),
          ...(productInfo.brand_name && { brandName: productInfo.brand_name }),
          ...(productInfo.cg_delivery !== undefined && { cgDelivery: typeof productInfo.cg_delivery === 'string' ? parseInt(productInfo.cg_delivery) : productInfo.cg_delivery }),
          ...(productInfo.cg_price && { cgPrice: parseFloat(productInfo.cg_price) }),
          ...(productInfo.purchase_remark && { purchaseRemark: productInfo.purchase_remark }),
          ...(productInfo.product_developer_uid !== undefined && productInfo.product_developer_uid !== null && { productDeveloperUid: String(productInfo.product_developer_uid) }),
          ...(productInfo.product_developer && { productDeveloper: productInfo.product_developer }),
          ...(productInfo.cg_opt_username && { cgOptUsername: productInfo.cg_opt_username }),
          ...(productInfo.global_tags && { globalTags: productInfo.global_tags }),
          ...(productInfo.supplier_quote && { supplierQuote: productInfo.supplier_quote }),
          ...(productInfo.custom_fields && { customFields: productInfo.custom_fields })
        },
        create: {
          productId: productId,
          accountId: accountId,
          productDetail: productInfo, // 保存完整的产品详情
          sku: productInfo.sku || null,
          skuIdentifier: productInfo.sku_identifier || null,
          productName: productInfo.product_name || null,
          picUrl: productInfo.pic_url || null,
            status: productInfo.status !== undefined && productInfo.status !== null ? (typeof productInfo.status === 'string' ? parseInt(productInfo.status) : productInfo.status) : null,
            isCombo: productInfo.is_combo !== undefined && productInfo.is_combo !== null ? (typeof productInfo.is_combo === 'string' ? parseInt(productInfo.is_combo) : productInfo.is_combo) : null,
            cid: productInfo.cid !== undefined && productInfo.cid !== null ? (typeof productInfo.cid === 'string' ? parseInt(productInfo.cid) : productInfo.cid) : null,
            categoryName: productInfo.category_name || null,
            bid: productInfo.bid !== undefined && productInfo.bid !== null ? (typeof productInfo.bid === 'string' ? parseInt(productInfo.bid) : productInfo.bid) : null,
            brandName: productInfo.brand_name || null,
            cgDelivery: productInfo.cg_delivery !== undefined && productInfo.cg_delivery !== null ? (typeof productInfo.cg_delivery === 'string' ? parseInt(productInfo.cg_delivery) : productInfo.cg_delivery) : null,
            cgPrice: productInfo.cg_price ? parseFloat(productInfo.cg_price) : null,
          purchaseRemark: productInfo.purchase_remark || null,
          productDeveloperUid: productInfo.product_developer_uid !== undefined && productInfo.product_developer_uid !== null ? String(productInfo.product_developer_uid) : null,
          productDeveloper: productInfo.product_developer || null,
          cgOptUsername: productInfo.cg_opt_username || null,
          globalTags: productInfo.global_tags || null,
          supplierQuote: productInfo.supplier_quote || null,
          customFields: productInfo.custom_fields || null
        }
      });

      console.log(`产品详情已保存到数据库: productId=${productId}`);
    } catch (error) {
      console.error('保存产品详情到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用返回
    }
  }

  /**
   * 批量查询本地产品详细信息
   * API: POST /erp/sc/routing/data/local_inventory/batchGetProductInfo
   * 对应系统【产品】>【产品管理】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - productIds: 产品id数组，上限100个【产品id、 产品SKU 、SKU识别码 三选一必填】
   *   - skus: 产品SKU数组，上限100个【产品id、 产品SKU 、SKU识别码 三选一必填】
   *   - sku_identifiers: SKU识别码数组，上限100个【产品id、 产品SKU 、SKU识别码 三选一必填】
   * @returns {Promise<Array>} 产品详细信息数组
   */
  async batchGetLocalProductInfo(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证参数：productIds、skus、sku_identifiers 三选一必填
      if (!params.productIds && !params.skus && !params.sku_identifiers) {
        throw new Error('productIds、skus、sku_identifiers 三选一必填');
      }

      // 验证数组长度上限
      if (params.productIds && params.productIds.length > 100) {
        throw new Error('productIds 数组长度不能超过100');
      }
      if (params.skus && params.skus.length > 100) {
        throw new Error('skus 数组长度不能超过100');
      }
      if (params.sku_identifiers && params.sku_identifiers.length > 100) {
        throw new Error('sku_identifiers 数组长度不能超过100');
      }

      // 构建请求参数（这些参数会作为 POST 请求的请求体发送）
      const requestParams = {};
      if (params.productIds && params.productIds.length > 0) {
        requestParams.productIds = params.productIds;
      }
      if (params.skus && params.skus.length > 0) {
        requestParams.skus = params.skus;
      }
      if (params.sku_identifiers && params.sku_identifiers.length > 0) {
        requestParams.sku_identifiers = params.sku_identifiers;
      }

      // 调用API批量获取产品详细信息
      // 注意：requestParams 会作为请求体（body）发送
      // 公共参数（access_token、app_key、timestamp、sign）会自动添加到查询参数中
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/batchGetProductInfo', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '批量获取产品详细信息失败');
      }

      const productInfoList = response.data || [];

      // 如果获取到产品详情列表，批量保存到数据库
      if (productInfoList.length > 0) {
        await this.batchSaveLocalProductDetails(accountId, productInfoList);
      }

      return productInfoList;
    } catch (error) {
      console.error('批量获取本地产品详细信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 批量保存产品详情到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} productInfoList - 产品详细信息数组
   */
  async batchSaveLocalProductDetails(accountId, productInfoList) {
    try {
      const savePromises = productInfoList.map(productInfo => {
        if (!productInfo || !productInfo.id) {
          return Promise.resolve();
        }

        // 确保 productId 是整数类型
        const productId = parseInt(productInfo.id);
        if (isNaN(productId)) {
          console.error(`无效的产品ID: ${productInfo.id}`);
          return Promise.resolve();
        }

        return prisma.lingXingLocalProduct.upsert({
          where: {
            productId: productId
          },
          update: {
            accountId: accountId,
            productDetail: productInfo, // 保存完整的产品详情
            updatedAt: new Date(),
            // 同时更新其他可能变化的字段
            ...(productInfo.sku && { sku: productInfo.sku }),
            ...(productInfo.sku_identifier && { skuIdentifier: productInfo.sku_identifier }),
            ...(productInfo.product_name && { productName: productInfo.product_name }),
            ...(productInfo.pic_url && { picUrl: productInfo.pic_url }),
            ...(productInfo.status !== undefined && { status: typeof productInfo.status === 'string' ? parseInt(productInfo.status) : productInfo.status }),
            ...(productInfo.is_combo !== undefined && { isCombo: typeof productInfo.is_combo === 'string' ? parseInt(productInfo.is_combo) : productInfo.is_combo }),
            ...(productInfo.cid !== undefined && { cid: typeof productInfo.cid === 'string' ? parseInt(productInfo.cid) : productInfo.cid }),
            ...(productInfo.category_name && { categoryName: productInfo.category_name }),
            ...(productInfo.bid !== undefined && { bid: typeof productInfo.bid === 'string' ? parseInt(productInfo.bid) : productInfo.bid }),
            ...(productInfo.brand_name && { brandName: productInfo.brand_name }),
            ...(productInfo.cg_delivery !== undefined && { cgDelivery: typeof productInfo.cg_delivery === 'string' ? parseInt(productInfo.cg_delivery) : productInfo.cg_delivery }),
            ...(productInfo.cg_price && { cgPrice: parseFloat(productInfo.cg_price) }),
            ...(productInfo.purchase_remark && { purchaseRemark: productInfo.purchase_remark }),
            ...(productInfo.product_developer_uid !== undefined && productInfo.product_developer_uid !== null && { productDeveloperUid: String(productInfo.product_developer_uid) }),
            ...(productInfo.product_developer && { productDeveloper: productInfo.product_developer }),
            ...(productInfo.cg_opt_username && { cgOptUsername: productInfo.cg_opt_username }),
            ...(productInfo.global_tags && { globalTags: productInfo.global_tags }),
            ...(productInfo.supplier_quote && { supplierQuote: productInfo.supplier_quote }),
            ...(productInfo.custom_fields && { customFields: productInfo.custom_fields })
          },
          create: {
            productId: productId,
            accountId: accountId,
            productDetail: productInfo, // 保存完整的产品详情
            sku: productInfo.sku || null,
            skuIdentifier: productInfo.sku_identifier || null,
            productName: productInfo.product_name || null,
            picUrl: productInfo.pic_url || null,
            status: productInfo.status !== undefined && productInfo.status !== null ? (typeof productInfo.status === 'string' ? parseInt(productInfo.status) : productInfo.status) : null,
            isCombo: productInfo.is_combo !== undefined && productInfo.is_combo !== null ? (typeof productInfo.is_combo === 'string' ? parseInt(productInfo.is_combo) : productInfo.is_combo) : null,
            cid: productInfo.cid !== undefined && productInfo.cid !== null ? (typeof productInfo.cid === 'string' ? parseInt(productInfo.cid) : productInfo.cid) : null,
            categoryName: productInfo.category_name || null,
            bid: productInfo.bid !== undefined && productInfo.bid !== null ? (typeof productInfo.bid === 'string' ? parseInt(productInfo.bid) : productInfo.bid) : null,
            brandName: productInfo.brand_name || null,
            cgDelivery: productInfo.cg_delivery !== undefined && productInfo.cg_delivery !== null ? (typeof productInfo.cg_delivery === 'string' ? parseInt(productInfo.cg_delivery) : productInfo.cg_delivery) : null,
            cgPrice: productInfo.cg_price ? parseFloat(productInfo.cg_price) : null,
            purchaseRemark: productInfo.purchase_remark || null,
            productDeveloperUid: productInfo.product_developer_uid !== undefined && productInfo.product_developer_uid !== null ? String(productInfo.product_developer_uid) : null,
            productDeveloper: productInfo.product_developer || null,
            cgOptUsername: productInfo.cg_opt_username || null,
            globalTags: productInfo.global_tags || null,
            supplierQuote: productInfo.supplier_quote || null,
            customFields: productInfo.custom_fields || null
          }
        }).catch(error => {
          console.error(`保存产品详情失败: productId=${productId}`, error.message);
          console.error('错误详情:', error);
          // 单个产品保存失败不影响其他产品
          return null;
        });
      });

      await Promise.all(savePromises);
      console.log(`批量保存产品详情完成: 共 ${productInfoList.length} 个产品`);
    } catch (error) {
      console.error('批量保存产品详情到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用返回
    }
  }

  /**
   * 查询本地产品列表并自动批量查询产品详情
   * 先查询产品列表，然后根据产品列表自动批量查询产品详情
   * 注意：批量查询接口令牌桶容量为1，需要分批串行处理
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 产品列表查询参数（同 getLocalProductList）
   * @param {Object} options - 选项
   *   - useCache: 是否优先使用缓存数据（默认false）
   *   - batchSize: 每批查询的产品数量（默认100，最大100）
   *   - delayBetweenBatches: 批次之间的延迟时间（毫秒，默认1000）
   *   - maxRetries: 每批查询的最大重试次数（默认3）
   * @returns {Promise<Object>} { products: [], productDetails: [], total: 0, stats: {} }
   */
  async getLocalProductListWithDetails(accountId, listParams = {}, options = {}) {
    const {
      useCache = false,
      batchSize = 100,
      delayBetweenBatches = 1000,
      maxRetries = 3
    } = options;

    try {
      // 1. 先查询产品列表
      console.log('开始查询产品列表...');
      const listResult = await this.getLocalProductList(accountId, listParams, useCache);
      const products = listResult.data || [];
      const total = listResult.total || 0;

      console.log(`产品列表查询完成，共 ${products.length} 个产品`);

      if (products.length === 0) {
        return {
          products: [],
          productDetails: [],
          total: 0,
          stats: {
            totalProducts: 0,
            batchesProcessed: 0,
            successCount: 0,
            failedCount: 0
          }
        };
      }

      // 2. 提取产品ID（优先使用id，如果没有则使用sku）
      const productIds = products
        .map(p => p.id)
        .filter(id => id !== undefined && id !== null);

      if (productIds.length === 0) {
        console.warn('产品列表中未找到有效的产品ID，无法批量查询详情');
        return {
          products: products,
          productDetails: [],
          total: total,
          stats: {
            totalProducts: products.length,
            batchesProcessed: 0,
            successCount: 0,
            failedCount: 0
          }
        };
      }

      // 3. 分批处理产品ID（每批最多100个）
      const actualBatchSize = Math.min(batchSize, 100);
      const batches = [];
      for (let i = 0; i < productIds.length; i += actualBatchSize) {
        batches.push(productIds.slice(i, i + actualBatchSize));
      }

      console.log(`开始批量查询产品详情，共 ${batches.length} 批，每批最多 ${actualBatchSize} 个产品`);

      // 4. 串行处理每批（因为令牌桶容量为1）
      const allProductDetails = [];
      const stats = {
        totalProducts: productIds.length,
        batchesProcessed: 0,
        successCount: 0,
        failedCount: 0,
        failedBatches: []
      };

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        const batchNumber = i + 1;

        try {
          console.log(`处理第 ${batchNumber}/${batches.length} 批，包含 ${batch.length} 个产品`);

          // 尝试查询这一批，带重试机制
          let batchDetails = [];
          let retryCount = 0;
          let lastError = null;

          while (retryCount <= maxRetries) {
            try {
              batchDetails = await this.batchGetLocalProductInfo(accountId, {
                productIds: batch.map(id => String(id))
              });
              break; // 成功，跳出重试循环
            } catch (error) {
              lastError = error;
              retryCount++;

              // 如果是限流错误，等待后重试
              if (error.code === '3001008' || (error.message && error.message.includes('限流'))) {
                const waitTime = delayBetweenBatches * retryCount; // 递增等待时间
                console.log(`第 ${batchNumber} 批遇到限流，等待 ${waitTime}ms 后重试 (${retryCount}/${maxRetries})`);
                await new Promise(resolve => setTimeout(resolve, waitTime));
              } else {
                // 其他错误，直接抛出
                throw error;
              }
            }
          }

          if (batchDetails.length > 0) {
            allProductDetails.push(...batchDetails);
            stats.successCount += batchDetails.length;
            console.log(`第 ${batchNumber} 批查询成功，获取到 ${batchDetails.length} 个产品详情`);
          } else if (lastError) {
            // 重试失败
            stats.failedCount += batch.length;
            stats.failedBatches.push({
              batchNumber: batchNumber,
              productIds: batch,
              error: lastError.message
            });
            console.error(`第 ${batchNumber} 批查询失败:`, lastError.message);
          }

          stats.batchesProcessed++;

          // 批次之间延迟（除了最后一批）
          if (i < batches.length - 1) {
            await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
          }
        } catch (error) {
          stats.failedCount += batch.length;
          stats.failedBatches.push({
            batchNumber: batchNumber,
            productIds: batch,
            error: error.message
          });
          console.error(`第 ${batchNumber} 批处理异常:`, error.message);
          // 继续处理下一批
        }
      }

      console.log(`批量查询产品详情完成: 成功 ${stats.successCount} 个，失败 ${stats.failedCount} 个`);

      return {
        products: products,
        productDetails: allProductDetails,
        total: total,
        stats: stats
      };
    } catch (error) {
      console.error('查询产品列表并批量获取详情失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有商品列表（自动处理分页）
   * 会自动分页获取所有产品，可选地自动批量查询产品详情
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 产品列表查询参数（同 getLocalProductList）
   *   - update_time_start: 更新时间-开始时间【时间戳，单位：秒】
   *   - update_time_end: 更新时间-结束时间【时间戳，单位：秒】
   *   - create_time_start: 创建时间-开始时间【时间戳，单位：秒】
   *   - create_time_end: 创建时间-结束时间【时间戳，单位：秒】
   *   - sku_list: 本地产品sku数组
   *   - sku_identifier_list: sku识别码列表数组
   * @param {Object} options - 选项
   *   - fetchDetails: 是否自动批量查询产品详情（默认false）
   *   - pageSize: 每页大小（默认1000，最大1000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - batchSize: 批量查询详情时每批数量（默认100，最大100）
   *   - delayBetweenBatches: 批量查询批次之间的延迟时间（毫秒，默认1000）
   *   - maxRetries: 每批查询的最大重试次数（默认3）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { products: [], productDetails: [], total: 0, stats: {} }
   */
  async fetchAllLocalProducts(accountId, listParams = {}, options = {}) {
    const {
      fetchDetails = false,
      pageSize = 1000,
      delayBetweenPages = 500,
      batchSize = 100,
      delayBetweenBatches = 1000,
      maxRetries = 3,
      onProgress = null
    } = options;

    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      console.log('开始自动拉取所有商品列表...');

      const allProducts = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 1000); // 最大1000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 1. 自动分页获取所有产品
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页产品（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getLocalProductList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          }, false); // 不使用缓存，确保获取最新数据

          const pageProducts = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个产品，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allProducts.push(...pageProducts);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProducts.length} 个产品，累计 ${allProducts.length} 个产品`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allProducts.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageProducts.length < actualPageSize || allProducts.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页产品失败:`, error.message);
          // 如果单页失败，继续尝试下一页（避免整个流程中断）
          if (allProducts.length === 0) {
            throw error; // 如果第一页就失败，抛出错误
          }
          hasMore = false; // 停止分页
        }
      }

      console.log(`所有产品列表获取完成，共 ${allProducts.length} 个产品`);

      const stats = {
        totalProducts: allProducts.length,
        pagesFetched: currentPage,
        productDetailsFetched: 0,
        batchesProcessed: 0,
        successCount: 0,
        failedCount: 0
      };

      // 2. 如果启用，自动批量查询产品详情
      let allProductDetails = [];
      if (fetchDetails && allProducts.length > 0) {
        console.log('开始自动批量查询产品详情...');

        // 提取产品ID
        const productIds = allProducts
          .map(p => p.id)
          .filter(id => id !== undefined && id !== null);

        if (productIds.length > 0) {
          // 分批处理产品ID
          const actualBatchSize = Math.min(batchSize, 100);
          const batches = [];
          for (let i = 0; i < productIds.length; i += actualBatchSize) {
            batches.push(productIds.slice(i, i + actualBatchSize));
          }

          console.log(`开始批量查询产品详情，共 ${batches.length} 批，每批最多 ${actualBatchSize} 个产品`);

          // 串行处理每批
          for (let i = 0; i < batches.length; i++) {
            const batch = batches[i];
            const batchNumber = i + 1;

            try {
              console.log(`处理第 ${batchNumber}/${batches.length} 批，包含 ${batch.length} 个产品`);

              let batchDetails = [];
              let retryCount = 0;
              let lastError = null;

              while (retryCount <= maxRetries) {
                try {
                  batchDetails = await this.batchGetLocalProductInfo(accountId, {
                    productIds: batch.map(id => String(id))
                  });
                  break;
                } catch (error) {
                  lastError = error;
                  retryCount++;

                  if (error.code === '3001008' || (error.message && error.message.includes('限流'))) {
                    const waitTime = delayBetweenBatches * retryCount;
                    console.log(`第 ${batchNumber} 批遇到限流，等待 ${waitTime}ms 后重试 (${retryCount}/${maxRetries})`);
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                  } else {
                    throw error;
                  }
                }
              }

              if (batchDetails.length > 0) {
                allProductDetails.push(...batchDetails);
                stats.successCount += batchDetails.length;
                console.log(`第 ${batchNumber} 批查询成功，获取到 ${batchDetails.length} 个产品详情`);
              } else if (lastError) {
                stats.failedCount += batch.length;
                console.error(`第 ${batchNumber} 批查询失败:`, lastError.message);
              }

              stats.batchesProcessed++;

              // 批次之间延迟
              if (i < batches.length - 1 && delayBetweenBatches > 0) {
                await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
              }
            } catch (error) {
              stats.failedCount += batch.length;
              console.error(`第 ${batchNumber} 批处理异常:`, error.message);
            }
          }

          stats.productDetailsFetched = allProductDetails.length;
          console.log(`批量查询产品详情完成: 成功 ${stats.successCount} 个，失败 ${stats.failedCount} 个`);
        }
      }

      return {
        products: allProducts,
        productDetails: allProductDetails,
        total: allProducts.length,
        stats: stats
      };
    } catch (error) {
      console.error('自动拉取所有商品列表失败:', error.message);
      throw error;
    }
  }
}

export default new LingXingProductService();

