import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';

/**
 * 领星ERP VC服务
 * VC相关接口（VC店铺、VC Listing、VC订单、VC发货单等）
 */
class LingXingVcService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询VC店铺列表
   * API: POST /basicOpen/platformAuth/vcSeller/pageList
   * @param {string} accountId - 领星账户ID
   * @param {number} offset - 分页偏移量，默认0
   * @param {number} length - 分页长度，默认20，上限200
   * @param {boolean} useCache - 是否优先使用缓存数据（默认false，因为分页数据）
   * @returns {Promise<Object>} VC店铺列表数据（包含total和data数组）
   */
  async getVcSellerPageList(accountId, offset = 0, length = 20, useCache = false) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 限制length最大值为200
      const pageLength = Math.min(length, 200);
      const pageOffset = Math.max(offset, 0);

      // 调用API获取VC店铺列表（使用通用客户端，成功码为0）
      const response = await this.post(
        account,
        '/basicOpen/platformAuth/vcSeller/pageList',
        {
          offset: pageOffset,
          length: pageLength
        },
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取VC店铺列表失败');
      }

      const vcSellers = response.data || [];

      // 保存到数据库
      if (vcSellers.length > 0) {
        await this.saveVcSellerLists(accountId, vcSellers);
      }

      return {
        total: response.total || 0,
        data: vcSellers,
        code: response.code,
        message: response.message,
        error_details: response.error_details || [],
        request_id: response.request_id,
        response_time: response.response_time
      };
    } catch (error) {
      console.error('获取VC店铺列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存VC店铺列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} vcSellers - VC店铺列表数据
   */
  async saveVcSellerLists(accountId, vcSellers) {
    try {
      for (const vcSeller of vcSellers) {
        await prisma.lingXingVcSeller.upsert({
          where: {
            vcStoreId: vcSeller.vc_store_id
          },
          update: {
            accountId: accountId,
            accountIdLx: vcSeller.account_id,
            sellerId: vcSeller.seller_id,
            accountName: vcSeller.account_name,
            region: vcSeller.region,
            regionName: vcSeller.region_name,
            name: vcSeller.name,
            status: vcSeller.status,
            mid: vcSeller.mid,
            data: vcSeller,
            updatedAt: new Date()
          },
          create: {
            vcStoreId: vcSeller.vc_store_id,
            accountId: accountId,
            accountIdLx: vcSeller.account_id,
            sellerId: vcSeller.seller_id,
            accountName: vcSeller.account_name,
            region: vcSeller.region,
            regionName: vcSeller.region_name,
            name: vcSeller.name,
            status: vcSeller.status,
            mid: vcSeller.mid,
            data: vcSeller
          }
        });
      }
    } catch (error) {
      console.error('保存VC店铺列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取VC店铺列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Array>} VC店铺列表数据
   */
  async getVcSellerListsFromDB(accountId) {
    try {
      const vcSellers = await prisma.lingXingVcSeller.findMany({
        where: { accountId: accountId },
        orderBy: { vcStoreId: 'asc' }
      });

      return vcSellers.map(vs => ({
        account_id: vs.accountIdLx,
        seller_id: vs.sellerId,
        account_name: vs.accountName,
        region: vs.region,
        region_name: vs.regionName,
        vc_store_id: vs.vcStoreId,
        name: vs.name,
        status: vs.status,
        mid: vs.mid,
        ...(vs.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取VC店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 自动拉取所有VC店铺数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认200，最大200）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数（可选）
   * @returns {Promise<Object>} 所有VC店铺数据
   */
  async fetchAllVcSellers(accountId, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有VC店铺数据...');

      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      const allVcSellers = [];
      let currentOffset = 0;
      const actualPageSize = Math.min(pageSize, 200); // 最大200
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有VC店铺数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页VC店铺（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getVcSellerPageList(
            accountId,
            currentOffset,
            actualPageSize,
            false // 不使用缓存，直接调用API
          );

          const pageVcSellers = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条VC店铺数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allVcSellers.push(...pageVcSellers);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageVcSellers.length} 条数据，累计 ${allVcSellers.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allVcSellers.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageVcSellers.length < actualPageSize || allVcSellers.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页VC店铺失败:`, error.message);
          // 如果已经获取了一些数据，继续返回；否则抛出错误
          if (allVcSellers.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有VC店铺数据获取完成，共 ${allVcSellers.length} 条数据`);

      return {
        vcSellers: allVcSellers,
        total: allVcSellers.length,
        stats: {
          totalCount: allVcSellers.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有VC店铺数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询VC Listing列表
   * API: POST /basicOpen/listingManage/vcListing/pageList
   * @param {string} accountId - 领星账户ID
   * @param {number} offset - 分页偏移量，默认0
   * @param {number} length - 分页长度，默认20，上限200
   * @param {Array} vcStoreIds - VC店铺id数组（可选）
   * @returns {Promise<Object>} VC Listing列表数据（包含total和data数组）
   */
  async getVcListingPageList(accountId, offset = 0, length = 20, vcStoreIds = null) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 限制length最大值为200
      const pageLength = Math.min(length, 200);
      const pageOffset = Math.max(offset, 0);

      // 构建请求参数
      const params = {
        offset: pageOffset,
        length: pageLength
      };

      // 如果提供了vc_store_ids，添加到参数中
      if (vcStoreIds && Array.isArray(vcStoreIds) && vcStoreIds.length > 0) {
        params.vc_store_ids = vcStoreIds;
      }

      // 调用API获取VC Listing列表（使用通用客户端，成功码为0）
      const response = await this.post(
        account,
        '/basicOpen/listingManage/vcListing/pageList',
        params,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取VC Listing列表失败');
      }

      const listings = response.data || [];

      // 保存到数据库
      if (listings.length > 0) {
        await this.saveVcListingLists(accountId, listings);
      }

      return {
        total: response.total || 0,
        data: listings,
        code: response.code,
        message: response.message,
        error_details: response.error_details || [],
        request_id: response.request_id,
        response_time: response.response_time
      };
    } catch (error) {
      console.error('获取VC Listing列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存VC Listing列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} listings - VC Listing列表数据
   */
  async saveVcListingLists(accountId, listings) {
    try {
      for (const listing of listings) {
        await prisma.lingXingVcListing.upsert({
          where: {
            accountId_vcStoreId_asin: {
              accountId: accountId,
              vcStoreId: listing.vc_store_id,
              asin: listing.asin
            }
          },
          update: {
            smallMinImageUrl: listing.small_min_image_url,
            asinUrl: listing.asin_url,
            msku: listing.msku,
            upc: listing.upc,
            ean: listing.ean,
            itemName: listing.item_name,
            parentAsin: listing.parent_asin,
            localSku: listing.local_sku,
            localName: listing.local_name,
            categoryName: listing.category_name,
            brandId: listing.brand_id,
            productId: listing.product_id,
            reviewsNum: listing.reviews_num,
            stars: listing.stars,
            remark: listing.remark,
            onSaleTime: listing.on_sale_time,
            status: listing.status,
            price: listing.price != null ? String(listing.price) : null,
            priceCurrencyIcon: listing.price_currency_icon,
            classificationRank: listing.classification_rank || null,
            displayGroupRank: listing.display_group_rank || null,
            principalList: listing.principal_list || null,
            data: listing,
            updatedAt: new Date()
          },
          create: {
            vcStoreId: listing.vc_store_id,
            asin: listing.asin,
            accountId: accountId,
            smallMinImageUrl: listing.small_min_image_url,
            asinUrl: listing.asin_url,
            msku: listing.msku,
            upc: listing.upc,
            ean: listing.ean,
            itemName: listing.item_name,
            parentAsin: listing.parent_asin,
            localSku: listing.local_sku,
            localName: listing.local_name,
            categoryName: listing.category_name,
            brandId: listing.brand_id,
            productId: listing.product_id,
            reviewsNum: listing.reviews_num,
            stars: listing.stars,
            remark: listing.remark,
            onSaleTime: listing.on_sale_time,
            status: listing.status,
            price: listing.price != null ? String(listing.price) : null,
            priceCurrencyIcon: listing.price_currency_icon,
            classificationRank: listing.classification_rank || null,
            displayGroupRank: listing.display_group_rank || null,
            principalList: listing.principal_list || null,
            data: listing
          }
        });
      }
    } catch (error) {
      console.error('保存VC Listing列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取VC Listing列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @param {Array} vcStoreIds - VC店铺id数组（可选，用于筛选）
   * @returns {Promise<Array>} VC Listing列表数据
   */
  async getVcListingListsFromDB(accountId, vcStoreIds = null) {
    try {
      const where = { accountId: accountId };
      
      // 如果提供了vc_store_ids，添加筛选条件
      if (vcStoreIds && Array.isArray(vcStoreIds) && vcStoreIds.length > 0) {
        where.vcStoreId = { in: vcStoreIds };
      }

      const listings = await prisma.lingXingVcListing.findMany({
        where: where,
        orderBy: { updatedAt: 'desc' }
      });

      return listings.map(l => ({
        vc_store_id: l.vcStoreId,
        small_min_image_url: l.smallMinImageUrl,
        asin: l.asin,
        asin_url: l.asinUrl,
        msku: l.msku,
        upc: l.upc,
        ean: l.ean,
        item_name: l.itemName,
        parent_asin: l.parentAsin,
        local_sku: l.localSku,
        local_name: l.localName,
        category_name: l.categoryName,
        brand_id: l.brandId,
        product_id: l.productId,
        reviews_num: l.reviewsNum,
        stars: l.stars,
        remark: l.remark,
        on_sale_time: l.onSaleTime,
        status: l.status,
        price: l.price,
        price_currency_icon: l.priceCurrencyIcon,
        classification_rank: l.classificationRank || [],
        display_group_rank: l.displayGroupRank || [],
        principal_list: l.principalList || [],
        ...(l.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取VC Listing列表失败:', error.message);
      return [];
    }
  }

  /**
   * 自动拉取所有VC Listing数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Array} vcStoreIds - VC店铺id数组（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认200，最大200）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数（可选）
   * @returns {Promise<Object>} 所有VC Listing数据
   */
  async fetchAllVcListings(accountId, vcStoreIds = null, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有VC Listing数据...');

      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      const allListings = [];
      let currentOffset = 0;
      const actualPageSize = Math.min(pageSize, 200); // 最大200
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有VC Listing数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页VC Listing（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getVcListingPageList(
            accountId,
            currentOffset,
            actualPageSize,
            vcStoreIds
          );

          const pageListings = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条VC Listing数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allListings.push(...pageListings);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageListings.length} 条数据，累计 ${allListings.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allListings.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageListings.length < actualPageSize || allListings.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页VC Listing失败:`, error.message);
          // 如果已经获取了一些数据，继续返回；否则抛出错误
          if (allListings.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有VC Listing数据获取完成，共 ${allListings.length} 条数据`);

      return {
        listings: allListings,
        total: allListings.length,
        stats: {
          totalCount: allListings.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有VC Listing数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询VC订单列表
   * API: POST /basicOpen/platformOrder/vcOrder/pageList
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - purchase_order_type: 订单类型数组 ["0"] 或 ["1"]（必填）
   *   - offset: 分页偏移量，默认0
   *   - length: 分页长度，默认20，上限200
   *   - vc_store_ids: VC店铺id数组（可选）
   *   - search_field_time: 查询时间类型（可选）
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - search_field: 搜索类型（可选）
   *   - search_value: 搜索值数组（可选）
   * @returns {Promise<Object>} VC订单列表数据（包含total和data数组）
   */
  async getVcOrderPageList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.purchase_order_type || !Array.isArray(params.purchase_order_type) || params.purchase_order_type.length === 0) {
        throw new Error('purchase_order_type参数必填，且必须为数组');
      }

      // 限制length最大值为200
      const pageLength = Math.min(params.length || 20, 200);
      const pageOffset = Math.max(params.offset || 0, 0);

      // 构建请求参数
      const requestParams = {
        purchase_order_type: params.purchase_order_type,
        offset: pageOffset,
        length: pageLength
      };

      // 添加可选参数
      if (params.vc_store_ids && Array.isArray(params.vc_store_ids) && params.vc_store_ids.length > 0) {
        requestParams.vc_store_ids = params.vc_store_ids;
      }
      if (params.search_field_time) {
        requestParams.search_field_time = params.search_field_time;
      }
      if (params.start_date) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date) {
        requestParams.end_date = params.end_date;
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value && Array.isArray(params.search_value) && params.search_value.length > 0) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取VC订单列表（使用通用客户端，成功码为0）
      const response = await this.post(
        account,
        '/basicOpen/platformOrder/vcOrder/pageList',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取VC订单列表失败');
      }

      const orders = response.data || [];

      // 保存到数据库
      if (orders.length > 0) {
        await this.saveVcOrders(accountId, orders);
        
        // 如果是PO类型订单（purchase_order_type = 1），自动查询并保存订单详情
        const poOrders = orders.filter(order => order.purchase_order_type === 1 || order.purchase_order_type === '1');
        if (poOrders.length > 0) {
          console.log(`发现 ${poOrders.length} 个PO类型订单，开始自动查询订单详情...`);
          
          // 串行查询PO订单详情（避免请求过快触发限流）
          // 每个请求之间延迟，让API客户端的重试机制处理限流错误
          for (let i = 0; i < poOrders.length; i++) {
            const order = poOrders[i];
            if (order.local_po_number) {
              try {
                await this.getVcOrderPoDetail(accountId, order.local_po_number);
                console.log(`[${i + 1}/${poOrders.length}] 已获取PO订单详情: ${order.local_po_number}`);
              } catch (error) {
                // 如果是限流错误，API客户端会自动重试（最多2次，每次延迟1秒）
                // 如果重试后仍然失败，记录错误但继续处理其他订单
                if (error.code === '3001008') {
                  console.warn(`[${i + 1}/${poOrders.length}] PO订单详情查询被限流（已自动重试）: ${order.local_po_number}`);
                } else {
                  console.error(`[${i + 1}/${poOrders.length}] 获取PO订单详情失败 ${order.local_po_number}:`, error.message);
                }
                // 不抛出错误，继续处理其他订单
              }
            }
            
            // 每个请求之间延迟，避免请求过快（除了最后一个）
            if (i < poOrders.length - 1) {
              await new Promise(resolve => setTimeout(resolve, 1000)); // 延迟1秒，避免请求过快
            }
          }
          
          console.log(`PO订单详情查询完成，共处理 ${poOrders.length} 个订单`);
        }
      }

      return {
        total: response.total || 0,
        data: orders,
        code: response.code,
        message: response.message,
        error_details: response.error_details || [],
        request_id: response.request_id,
        response_time: response.response_time
      };
    } catch (error) {
      console.error('获取VC订单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存VC订单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} orders - VC订单列表数据
   */
  async saveVcOrders(accountId, orders) {
    try {
      for (const order of orders) {
        if (!order.local_po_number) {
          console.warn('订单缺少local_po_number，跳过保存:', order);
          continue;
        }

        // 保存订单主表
        // 确保 vcStoreId 存在，如果不存在则使用空字符串作为默认值
        const vcStoreId = order.vc_store_id || '';
        
        const savedOrder = await prisma.lingXingVcOrder.upsert({
          where: {
            localPoNumber_vcStoreId: {
              localPoNumber: order.local_po_number,
              vcStoreId: vcStoreId
            }
          },
          update: {
            orderId: order.id,
            purchaseOrderNumber: order.purchase_order_number,
            customerOrderNumber: order.customer_order_number,
            vcStoreId: order.vc_store_id,
            sellerName: order.seller_name,
            purchaseOrderType: order.purchase_order_type,
            purchaseOrderState: order.purchase_order_state,
            purchaseOrderProcessState: order.purchase_order_process_state,
            purchaseOrderDate: order.purchase_order_date,
            ackStatus: order.ack_status,
            ackStatusDesc: order.ack_status_desc,
            ackUpdateTime: order.ack_update_time,
            focusPartyId: order.focus_party_id,
            erpWarehouseName: order.erp_warehouse_name,
            erpWarehouseId: order.erp_warehouse_id,
            shipWindowTime: order.ship_window_time,
            shipWindowStart: order.ship_window_start,
            shipWindowsEnd: order.ship_windows_end,
            totalPrice: order.total_price,
            currencyCode: order.currency_code,
            currencyIcon: order.currency_icon,
            itemAmount: order.item_amount,
            remark: order.remark,
            shipmentConfirmStatus: order.shipment_confirm_status,
            shipmentLabelStatus: order.shipment_label_status,
            printNum: order.print_num,
            gmtCreate: order.gmt_create,
            gmtModified: order.gmt_modified,
            data: order,
            updatedAt: new Date()
          },
          create: {
            localPoNumber: order.local_po_number,
            vcStoreId: vcStoreId,
            accountId: accountId,
            orderId: order.id,
            purchaseOrderNumber: order.purchase_order_number,
            customerOrderNumber: order.customer_order_number,
            sellerName: order.seller_name,
            purchaseOrderType: order.purchase_order_type,
            purchaseOrderState: order.purchase_order_state,
            purchaseOrderProcessState: order.purchase_order_process_state,
            purchaseOrderDate: order.purchase_order_date,
            ackStatus: order.ack_status,
            ackStatusDesc: order.ack_status_desc,
            ackUpdateTime: order.ack_update_time,
            focusPartyId: order.focus_party_id,
            erpWarehouseName: order.erp_warehouse_name,
            erpWarehouseId: order.erp_warehouse_id,
            shipWindowTime: order.ship_window_time,
            shipWindowStart: order.ship_window_start,
            shipWindowsEnd: order.ship_windows_end,
            totalPrice: order.total_price,
            currencyCode: order.currency_code,
            currencyIcon: order.currency_icon,
            itemAmount: order.item_amount,
            remark: order.remark,
            shipmentConfirmStatus: order.shipment_confirm_status,
            shipmentLabelStatus: order.shipment_label_status,
            printNum: order.print_num,
            gmtCreate: order.gmt_create,
            gmtModified: order.gmt_modified,
            data: order
          }
        });

        // 保存订单明细
        if (order.purchase_order_sku_list && Array.isArray(order.purchase_order_sku_list)) {
          // 先删除旧的订单明细
          await prisma.lingXingVcOrderItem.deleteMany({
            where: { orderId: savedOrder.id }
          });

          // 保存新的订单明细
          for (const item of order.purchase_order_sku_list) {
            await prisma.lingXingVcOrderItem.create({
              data: {
                orderId: savedOrder.id,
                itemId: item.id,
                vcStoreId: item.vc_store_id,
                sellerName: item.seller_name,
                asin: item.asin,
                upc: item.upc,
                ean: item.ean,
                parentAsin: item.parent_asin,
                itemName: item.item_name,
                largeMainImageUrl: item.large_main_image_url,
                mediumMainImageUrl: item.medium_main_image_url,
                smallMainImageUrl: item.small_main_image_url,
                hasPrincipal: item.has_principal,
                purchaseAmount: item.purchase_amount,
                sequenceNumber: item.sequence_number,
                vendorProductId: item.vendor_product_id,
                localPoNumber: item.local_po_number,
                purchaseOrderNumber: item.purchase_order_number,
                unitPrice: item.unit_price,
                netPrice: item.net_price,
                netPriceCurrencyCode: item.net_price_currency_code,
                netPriceCurrencyIcon: item.net_price_currency_icon,
                taxAmount: item.tax_amount,
                taxAmountCurrencyCode: item.tax_amount_currency_code,
                taxAmountCurrencyIcon: item.tax_amount_currency_icon,
                taxRate: item.tax_rate,
                taxRatePercent: item.tax_rate_percent,
                dealTotalPrice: item.deal_total_price,
                dealUnitPrice: item.deal_unit_price,
                isBackOrderAllowed: item.is_back_order_allowed,
                shippedAmount: item.shipped_amount,
                toShipAmount: item.to_ship_amount,
                localName: item.local_name,
                localSku: item.local_sku,
                productId: item.product_id,
                availableAmount: item.available_amount,
                asinUrl: item.asin_url,
                picUrl: item.pic_url,
                acceptedQuantity: item.accepted_quantity,
                rejectedQuantity: item.rejected_quantity,
                receivedQuantity: item.received_quantity,
                data: item
              }
            });
          }
        }
      }
    } catch (error) {
      console.error('保存VC订单列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取VC订单列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filters - 筛选条件（可选）
   * @returns {Promise<Array>} VC订单列表数据
   */
  async getVcOrdersFromDB(accountId, filters = {}) {
    try {
      const where = { accountId: accountId };
      
      // 添加筛选条件
      if (filters.vc_store_ids && Array.isArray(filters.vc_store_ids) && filters.vc_store_ids.length > 0) {
        where.vcStoreId = { in: filters.vc_store_ids };
      }
      if (filters.purchase_order_type !== undefined) {
        where.purchaseOrderType = filters.purchase_order_type;
      }
      if (filters.purchase_order_state) {
        where.purchaseOrderState = filters.purchase_order_state;
      }
      if (filters.start_date && filters.end_date) {
        where.purchaseOrderDate = {
          gte: filters.start_date,
          lte: filters.end_date
        };
      }

      const orders = await prisma.lingXingVcOrder.findMany({
        where: where,
        include: {
          orderItems: true
        },
        orderBy: { updatedAt: 'desc' }
      });

      return orders.map(order => ({
        id: order.orderId,
        gmt_create: order.gmtCreate,
        gmt_modified: order.gmtModified,
        purchase_order_number: order.purchaseOrderNumber,
        customer_order_number: order.customerOrderNumber,
        vc_store_id: order.vcStoreId,
        seller_name: order.sellerName,
        purchase_order_type: order.purchaseOrderType,
        purchase_order_state: order.purchaseOrderState,
        purchase_order_process_state: order.purchaseOrderProcessState,
        purchase_order_date: order.purchaseOrderDate,
        ack_status: order.ackStatus,
        ack_status_desc: order.ackStatusDesc,
        ack_update_time: order.ackUpdateTime,
        focus_party_id: order.focusPartyId,
        erp_warehouse_name: order.erpWarehouseName,
        erp_warehouse_id: order.erpWarehouseId,
        ship_window_time: order.shipWindowTime,
        ship_window_start: order.shipWindowStart,
        ship_windows_end: order.shipWindowsEnd,
        total_price: order.totalPrice,
        currency_code: order.currencyCode,
        currency_icon: order.currencyIcon,
        item_amount: order.itemAmount,
        local_po_number: order.localPoNumber,
        remark: order.remark,
        shipment_confirm_status: order.shipmentConfirmStatus,
        shipment_label_status: order.shipmentLabelStatus,
        print_num: order.printNum,
        purchase_order_sku_list: order.orderItems.map(item => ({
          id: item.itemId,
          vc_store_id: item.vcStoreId,
          seller_name: item.sellerName,
          asin: item.asin,
          upc: item.upc,
          ean: item.ean,
          parent_asin: item.parentAsin,
          item_name: item.itemName,
          large_main_image_url: item.largeMainImageUrl,
          medium_main_image_url: item.mediumMainImageUrl,
          small_main_image_url: item.smallMainImageUrl,
          has_principal: item.hasPrincipal,
          purchase_amount: item.purchaseAmount,
          sequence_number: item.sequenceNumber,
          vendor_product_id: item.vendorProductId,
          local_po_number: item.localPoNumber,
          purchase_order_number: item.purchaseOrderNumber,
          unit_price: item.unitPrice,
          net_price: item.netPrice,
          net_price_currency_code: item.netPriceCurrencyCode,
          net_price_currency_icon: item.netPriceCurrencyIcon,
          tax_amount: item.taxAmount,
          tax_amount_currency_code: item.taxAmountCurrencyCode,
          tax_amount_currency_icon: item.taxAmountCurrencyIcon,
          tax_rate: item.taxRate,
          tax_rate_percent: item.taxRatePercent,
          deal_total_price: item.dealTotalPrice,
          deal_unit_price: item.dealUnitPrice,
          is_back_order_allowed: item.isBackOrderAllowed,
          shipped_amount: item.shippedAmount,
          to_ship_amount: item.toShipAmount,
          local_name: item.localName,
          local_sku: item.localSku,
          product_id: item.productId,
          available_amount: item.availableAmount,
          asin_url: item.asinUrl,
          pic_url: item.picUrl,
          accepted_quantity: item.acceptedQuantity,
          rejected_quantity: item.rejectedQuantity,
          received_quantity: item.receivedQuantity,
          ...(item.data || {})
        })),
        ...(order.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取VC订单列表失败:', error.message);
      return [];
    }
  }

  /**
   * 自动拉取所有VC订单数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认200，最大200）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数（可选）
   * @returns {Promise<Object>} 所有VC订单数据
   */
  async fetchAllVcOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有VC订单数据...');

      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      const allOrders = [];
      let currentOffset = 0;
      const actualPageSize = Math.min(pageSize, 200); // 最大200
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有VC订单数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页VC订单（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getVcOrderPageList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条VC订单数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allOrders.push(...pageOrders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageOrders.length} 条数据，累计 ${allOrders.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allOrders.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageOrders.length < actualPageSize || allOrders.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页VC订单失败:`, error.message);
          // 如果已经获取了一些数据，继续返回；否则抛出错误
          if (allOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有VC订单数据获取完成，共 ${allOrders.length} 条数据`);

      return {
        orders: allOrders,
        total: allOrders.length,
        stats: {
          totalCount: allOrders.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有VC订单数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询VC订单详情（PO类型）
   * API: POST /basicOpen/platformOrder/vcOrderPo/detail
   * @param {string} accountId - 领星账户ID
   * @param {string} localPoNumber - 本地po号
   * @returns {Promise<Object>} VC订单详情数据
   */
  async getVcOrderPoDetail(accountId, localPoNumber) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      if (!localPoNumber) {
        throw new Error('local_po_number参数必填');
      }

      // 调用API获取VC订单详情（使用通用客户端，成功码为0）
      const response = await this.post(
        account,
        '/basicOpen/platformOrder/vcOrderPo/detail',
        {
          local_po_number: localPoNumber
        },
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取VC订单详情失败');
      }

      // 如果返回了订单详情数据，保存到数据库的 orderDetail 字段
      const detailData = response.data || {};
      if (detailData && localPoNumber) {
        try {
          // 先查询订单，获取 vcStoreId（因为唯一键是 localPoNumber + vcStoreId）
          const existingOrder = await prisma.lingXingVcOrder.findFirst({
            where: {
              localPoNumber: localPoNumber,
              accountId: accountId
            }
          });

          if (existingOrder) {
            // 更新订单的 orderDetail 字段
            await prisma.lingXingVcOrder.update({
              where: {
                localPoNumber_vcStoreId: {
                  localPoNumber: localPoNumber,
                  vcStoreId: existingOrder.vcStoreId || ''
                }
              },
              data: {
                orderDetail: detailData,
                updatedAt: new Date()
              }
            });
          } else {
            console.warn(`VC订单不存在，无法保存详情: ${localPoNumber}`);
          }
        } catch (updateError) {
          // 如果订单不存在，记录警告但不影响返回结果
          if (updateError.code === 'P2025') {
            console.warn(`VC订单不存在，无法保存详情: ${localPoNumber}`);
          } else {
            console.error('保存VC订单详情失败:', updateError.message);
            // 不抛出错误，因为详情数据已经获取成功
          }
        }
      }

      return {
        data: detailData,
        code: response.code,
        message: response.message,
        error_details: response.error_details || [],
        request_id: response.request_id,
        response_time: response.response_time
      };
    } catch (error) {
      console.error('获取VC订单详情失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询VC发货单列表
   * API: POST /basicOpen/openapi/getInvoice/page/list
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 偏移量，默认0
   *   - length: 每页条数，默认20，上限200
   *   - sids: 店铺id数组（可选）
   *   - wid: 国家id数组（可选）
   *   - shipmentType: 出库类型，1:DF 2:PO 3:DI（必填）
   *   - status: 订单状态，0:全部 5:待配货 10:待出库 15:已完成 100:已作废（可选，默认0）
   *   - createTimeStartTime: 创建日期-开始（可选）
   *   - createTimeEndTime: 创建日期-结束（可选）
   *   - shipmentTimeStartTime: 出库日期-开始（可选）
   *   - shipmentTimeEndTime: 出库日期-结束（可选）
   * @returns {Promise<Object>} VC发货单列表数据
   */
  async getVcInvoicePageList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.shipmentType) {
        throw new Error('shipmentType参数必填，1:DF 2:PO 3:DI');
      }

      // 限制length最大值为200
      const pageLength = Math.min(params.length || 20, 200);
      const pageOffset = Math.max(params.offset || 0, 0);

      // 构建请求参数
      const requestParams = {
        offset: pageOffset,
        length: pageLength,
        shipmentType: params.shipmentType
      };

      // 添加可选参数
      if (params.sids && Array.isArray(params.sids) && params.sids.length > 0) {
        requestParams.sids = params.sids;
      }
      if (params.wid && Array.isArray(params.wid) && params.wid.length > 0) {
        requestParams.wid = params.wid;
      }
      if (params.status !== undefined && params.status !== null) {
        requestParams.status = params.status;
      }
      if (params.createTimeStartTime) {
        requestParams.createTimeStartTime = params.createTimeStartTime;
      }
      if (params.createTimeEndTime) {
        requestParams.createTimeEndTime = params.createTimeEndTime;
      }
      if (params.shipmentTimeStartTime) {
        requestParams.shipmentTimeStartTime = params.shipmentTimeStartTime;
      }
      if (params.shipmentTimeEndTime) {
        requestParams.shipmentTimeEndTime = params.shipmentTimeEndTime;
      }

      // 调用API获取VC发货单列表（使用通用客户端，成功码为0）
      const response = await this.post(
        account,
        '/basicOpen/openapi/getInvoice/page/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取VC发货单列表失败');
      }

      const invoices = response.data?.list || [];

      // 保存到数据库
      if (invoices.length > 0) {
        await this.saveVcInvoices(accountId, invoices);
        
        // 自动查询并保存发货单详情
        console.log(`发现 ${invoices.length} 个发货单，开始自动查询发货单详情...`);
        
        // 串行查询发货单详情（避免请求过快触发限流）
        // 每个请求之间延迟，让API客户端的重试机制处理限流错误
        for (let i = 0; i < invoices.length; i++) {
          const invoice = invoices[i];
          if (invoice.orderNo) {
            try {
              await this.getVcInvoiceDetail(accountId, invoice.orderNo);
              console.log(`[${i + 1}/${invoices.length}] 已获取发货单详情: ${invoice.orderNo}`);
            } catch (error) {
              // 如果是限流错误，API客户端会自动重试（最多2次，每次延迟1秒）
              // 如果重试后仍然失败，记录错误但继续处理其他发货单
              if (error.code === '3001008') {
                console.warn(`[${i + 1}/${invoices.length}] 发货单详情查询被限流（已自动重试）: ${invoice.orderNo}`);
              } else {
                console.error(`[${i + 1}/${invoices.length}] 获取发货单详情失败 ${invoice.orderNo}:`, error.message);
              }
              // 不抛出错误，继续处理其他发货单
            }
          }
          
          // 每个请求之间延迟，避免请求过快（除了最后一个）
          if (i < invoices.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 1000)); // 延迟1秒，避免请求过快
          }
        }
        
        console.log(`发货单详情查询完成，共处理 ${invoices.length} 个发货单`);
      }

      return {
        total: response.data?.count || 0,
        data: invoices,
        code: response.code,
        message: response.message,
        error_details: response.error_details || [],
        request_id: response.request_id,
        response_time: response.response_time
      };
    } catch (error) {
      console.error('获取VC发货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存VC发货单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} invoices - VC发货单列表数据
   */
  async saveVcInvoices(accountId, invoices) {
    try {
      for (const invoice of invoices) {
        if (!invoice.orderNo) {
          console.warn('发货单缺少orderNo，跳过保存:', invoice);
          continue;
        }

        // 保存发货单主表
        const savedInvoice = await prisma.lingXingVcInvoice.upsert({
          where: {
            orderNo: invoice.orderNo
          },
          update: {
            invoiceId: invoice.id,
            purchaseOrderNumber: invoice.purchaseOrderNumber,
            remark: invoice.remark,
            shippingWid: invoice.shippingWid,
            shippingWarehouseName: invoice.shippingWarehouseName,
            shipmentTime: invoice.shipmentTime,
            shipmentUser: invoice.shipmentUser,
            status: invoice.status,
            createUser: invoice.createUser,
            createTime: invoice.createTime,
            shipmentType: invoice.shipmentType,
            statusName: invoice.statusName,
            totalNum: invoice.totalNum,
            estimatedPickupTime: invoice.estimatedPickupTime,
            shipmentTypeName: invoice.shipmentTypeName,
            sourceType: invoice.sourceType,
            invoiceModel: invoice.invoiceModel,
            outboundDate: invoice.outboundDate,
            gmtCreate: invoice.gmtCreate,
            gmtModified: invoice.gmtModified,
            data: invoice,
            updatedAt: new Date()
          },
          create: {
            orderNo: invoice.orderNo,
            accountId: accountId,
            invoiceId: invoice.id,
            purchaseOrderNumber: invoice.purchaseOrderNumber,
            remark: invoice.remark,
            shippingWid: invoice.shippingWid,
            shippingWarehouseName: invoice.shippingWarehouseName,
            shipmentTime: invoice.shipmentTime,
            shipmentUser: invoice.shipmentUser,
            status: invoice.status,
            createUser: invoice.createUser,
            createTime: invoice.createTime,
            shipmentType: invoice.shipmentType,
            statusName: invoice.statusName,
            totalNum: invoice.totalNum,
            estimatedPickupTime: invoice.estimatedPickupTime,
            shipmentTypeName: invoice.shipmentTypeName,
            sourceType: invoice.sourceType,
            invoiceModel: invoice.invoiceModel,
            outboundDate: invoice.outboundDate,
            gmtCreate: invoice.gmtCreate,
            gmtModified: invoice.gmtModified,
            data: invoice
          }
        });

        // 保存发货单明细
        if (invoice.items && Array.isArray(invoice.items)) {
          // 先删除旧的发货单明细
          await prisma.lingXingVcInvoiceItem.deleteMany({
            where: { invoiceId: savedInvoice.id }
          });

          // 保存新的发货单明细
          for (const item of invoice.items) {
            const savedItem = await prisma.lingXingVcInvoiceItem.create({
              data: {
                invoiceId: savedInvoice.id,
                itemId: item.id,
                sku: item.sku,
                asin: item.asin,
                productId: item.productId,
                productName: item.productName,
                num: item.num,
                storeId: item.storeId,
                storeName: item.storeName,
                warehouseStoreId: item.warehouseStoreId,
                warehouseStoreName: item.warehouseStoreName,
                picUrl: item.picUrl,
                toShipAmount: item.toShipAmount,
                purchasePrice: item.purchasePrice,
                stockCost: item.stockCost,
                orderNo: item.orderNo,
                purchaseOrderNumber: item.purchaseOrderNumber,
                remark: item.remark,
                shippingWid: item.shippingWid,
                shippingWarehouseName: item.shippingWarehouseName,
                shipmentTime: item.shipmentTime,
                shipmentUser: item.shipmentUser,
                isDeleted: item.isDeleted,
                itemStatus: item.status,
                createUser: item.createUser,
                createTime: item.createTime,
                itemShipmentType: item.shipmentType,
                itemRemark: item.itemRemark,
                thirdPartyProductCode: item.thirdPartyProductCode,
                thirdPartyProductName: item.thirdPartyProductName,
                vendorShipmentIdentifier: item.vendorShipmentIdentifier,
                asn: item.asn,
                buyerReferenceNumber: item.buyerReferenceNumber,
                unitPrice: item.unitPrice,
                headStockUnitPrice: item.headStockUnitPrice,
                stockUnitPrice: item.stockUnitPrice,
                gmtCreate: item.gmtCreate,
                gmtModified: item.gmtModified,
                data: item
              }
            });

            // 保存商品箱规
            if (item.itemDimensionsList && Array.isArray(item.itemDimensionsList)) {
              for (const dimension of item.itemDimensionsList) {
                await prisma.lingXingVcInvoiceItemDimension.create({
                  data: {
                    invoiceItemId: savedItem.id,
                    dimensionId: dimension.id,
                    purchaseOrderSkuId: dimension.purchaseOrderSkuId,
                    orderSn: dimension.orderSn,
                    sourceOrderSn: dimension.sourceOrderSn,
                    frontDimensionsId: dimension.frontDimensionsId,
                    boxNum: dimension.boxNum,
                    packedAmount: dimension.packedAmount,
                    dimensionsName: dimension.dimensionsName,
                    weight: dimension.weight,
                    weightUnit: dimension.weightUnit,
                    length: dimension.length,
                    width: dimension.width,
                    height: dimension.height,
                    dimensionsUnit: dimension.dimensionsUnit,
                    thirdPartyOrderNo: dimension.thirdPartyOrderNo,
                    thirdPartyOrderStatus: dimension.thirdPartyOrderStatus,
                    gmtCreate: dimension.gmtCreate,
                    gmtModified: dimension.gmtModified,
                    data: dimension
                  }
                });
              }
            }
          }
        }

        // 保存物流信息
        if (invoice.invoiceTrackingList && Array.isArray(invoice.invoiceTrackingList)) {
          // 先删除旧的物流信息
          await prisma.lingXingVcInvoiceTracking.deleteMany({
            where: { invoiceId: savedInvoice.id }
          });

          // 保存新的物流信息
          for (const tracking of invoice.invoiceTrackingList) {
            await prisma.lingXingVcInvoiceTracking.create({
              data: {
                invoiceId: savedInvoice.id,
                orderSn: tracking.orderSn,
                sourceOrderSn: tracking.sourceOrderSn,
                containerSequenceNumber: tracking.containerSequenceNumber,
                containerIdentificationNumber: tracking.containerIdentificationNumber,
                boxLabelUrl: tracking.boxLabelUrl,
                cartonLabelUrl: tracking.cartonLabelUrl,
                trackingNumber: tracking.trackingNumber,
                data: tracking
              }
            });
          }
        }
      }
    } catch (error) {
      console.error('保存VC发货单列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取VC发货单列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filters - 筛选条件（可选）
   * @returns {Promise<Array>} VC发货单列表数据
   */
  async getVcInvoicesFromDB(accountId, filters = {}) {
    try {
      const where = { accountId: accountId };
      
      // 添加筛选条件
      if (filters.shipmentType) {
        where.shipmentType = filters.shipmentType;
      }
      if (filters.status !== undefined && filters.status !== null) {
        where.status = filters.status;
      }
      if (filters.createTimeStartTime && filters.createTimeEndTime) {
        where.createTime = {
          gte: filters.createTimeStartTime,
          lte: filters.createTimeEndTime
        };
      }
      if (filters.shipmentTimeStartTime && filters.shipmentTimeEndTime) {
        where.shipmentTime = {
          gte: filters.shipmentTimeStartTime,
          lte: filters.shipmentTimeEndTime
        };
      }

      const invoices = await prisma.lingXingVcInvoice.findMany({
        where: where,
        include: {
          invoiceItems: {
            include: {
              itemDimensions: true
            }
          },
          invoiceTrackingList: true
        },
        orderBy: { updatedAt: 'desc' }
      });

      return invoices.map(invoice => ({
        id: invoice.invoiceId,
        gmtCreate: invoice.gmtCreate,
        gmtModified: invoice.gmtModified,
        orderNo: invoice.orderNo,
        purchaseOrderNumber: invoice.purchaseOrderNumber,
        remark: invoice.remark,
        shippingWid: invoice.shippingWid,
        shippingWarehouseName: invoice.shippingWarehouseName,
        shipmentTime: invoice.shipmentTime,
        shipmentUser: invoice.shipmentUser,
        status: invoice.status,
        createUser: invoice.createUser,
        createTime: invoice.createTime,
        shipmentType: invoice.shipmentType,
        statusName: invoice.statusName,
        totalNum: invoice.totalNum,
        estimatedPickupTime: invoice.estimatedPickupTime,
        shipmentTypeName: invoice.shipmentTypeName,
        sourceType: invoice.sourceType,
        invoiceModel: invoice.invoiceModel,
        outboundDate: invoice.outboundDate,
        items: invoice.invoiceItems.map(item => ({
          id: item.itemId,
          gmtCreate: item.gmtCreate,
          gmtModified: item.gmtModified,
          sku: item.sku,
          asin: item.asin,
          productId: item.productId,
          productName: item.productName,
          num: item.num,
          storeId: item.storeId,
          storeName: item.storeName,
          warehouseStoreId: item.warehouseStoreId,
          warehouseStoreName: item.warehouseStoreName,
          picUrl: item.picUrl,
          toShipAmount: item.toShipAmount,
          purchasePrice: item.purchasePrice,
          stockCost: item.stockCost,
          orderNo: item.orderNo,
          purchaseOrderNumber: item.purchaseOrderNumber,
          remark: item.remark,
          shippingWid: item.shippingWid,
          shippingWarehouseName: item.shippingWarehouseName,
          shipmentTime: item.shipmentTime,
          shipmentUser: item.shipmentUser,
          isDeleted: item.isDeleted,
          status: item.itemStatus,
          createUser: item.createUser,
          createTime: item.createTime,
          shipmentType: item.itemShipmentType,
          itemRemark: item.itemRemark,
          thirdPartyProductCode: item.thirdPartyProductCode,
          thirdPartyProductName: item.thirdPartyProductName,
          vendorShipmentIdentifier: item.vendorShipmentIdentifier,
          asn: item.asn,
          buyerReferenceNumber: item.buyerReferenceNumber,
          unitPrice: item.unitPrice,
          headStockUnitPrice: item.headStockUnitPrice,
          stockUnitPrice: item.stockUnitPrice,
          itemDimensionsList: item.itemDimensions.map(dim => ({
            id: dim.dimensionId,
            purchaseOrderSkuId: dim.purchaseOrderSkuId,
            orderSn: dim.orderSn,
            sourceOrderSn: dim.sourceOrderSn,
            frontDimensionsId: dim.frontDimensionsId,
            boxNum: dim.boxNum,
            packedAmount: dim.packedAmount,
            dimensionsName: dim.dimensionsName,
            weight: dim.weight,
            weightUnit: dim.weightUnit,
            length: dim.length,
            width: dim.width,
            height: dim.height,
            dimensionsUnit: dim.dimensionsUnit,
            thirdPartyOrderNo: dim.thirdPartyOrderNo,
            thirdPartyOrderStatus: dim.thirdPartyOrderStatus,
            gmtCreate: dim.gmtCreate,
            gmtModified: dim.gmtModified,
            ...(dim.data || {})
          })),
          ...(item.data || {})
        })),
        invoiceTrackingList: invoice.invoiceTrackingList.map(tracking => ({
          orderSn: tracking.orderSn,
          sourceOrderSn: tracking.sourceOrderSn,
          containerSequenceNumber: tracking.containerSequenceNumber,
          containerIdentificationNumber: tracking.containerIdentificationNumber,
          boxLabelUrl: tracking.boxLabelUrl,
          cartonLabelUrl: tracking.cartonLabelUrl,
          trackingNumber: tracking.trackingNumber,
          ...(tracking.data || {})
        })),
        ...(invoice.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取VC发货单列表失败:', error.message);
      return [];
    }
  }

  /**
   * 自动拉取所有VC发货单数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认200，最大200）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数（可选）
   * @returns {Promise<Object>} 所有VC发货单数据
   */
  async fetchAllVcInvoices(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有VC发货单数据...');

      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      const allInvoices = [];
      let currentOffset = 0;
      const actualPageSize = Math.min(pageSize, 200); // 最大200
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有VC发货单数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页VC发货单（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getVcInvoicePageList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageInvoices = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条VC发货单数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allInvoices.push(...pageInvoices);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageInvoices.length} 条数据，累计 ${allInvoices.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allInvoices.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageInvoices.length < actualPageSize || allInvoices.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页VC发货单失败:`, error.message);
          // 如果已经获取了一些数据，继续返回；否则抛出错误
          if (allInvoices.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有VC发货单数据获取完成，共 ${allInvoices.length} 条数据`);

      return {
        invoices: allInvoices,
        total: allInvoices.length,
        stats: {
          totalCount: allInvoices.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有VC发货单数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询VC发货单详情
   * API: POST /basicOpen/openapi/getInvoice/detail
   * @param {string} accountId - 领星账户ID
   * @param {string} orderNo - 发货单号
   * @returns {Promise<Object>} VC发货单详情数据
   */
  async getVcInvoiceDetail(accountId, orderNo) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      if (!orderNo) {
        throw new Error('orderNo参数必填');
      }

      // 调用API获取VC发货单详情（使用通用客户端，成功码为0）
      const response = await this.post(
        account,
        '/basicOpen/openapi/getInvoice/detail',
        {
          orderNo: orderNo
        },
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取VC发货单详情失败');
      }

      // 如果返回了发货单详情数据，保存到数据库的 invoiceDetail 字段
      const detailData = response.data || {};
      if (detailData && orderNo) {
        try {
          // 更新发货单的 invoiceDetail 字段
          await prisma.lingXingVcInvoice.update({
            where: { orderNo: orderNo },
            data: {
              invoiceDetail: detailData,
              updatedAt: new Date()
            }
          });
          
          // 如果详情中包含完整的发货单数据（包括items和tracking），也更新主表和关联表
          if (detailData.invoice) {
            await this.saveVcInvoices(accountId, [detailData.invoice]);
          }
        } catch (updateError) {
          console.error(`保存VC发货单详情失败 ${orderNo}:`, updateError.message);
          // 不抛出错误，因为保存失败不应该影响API调用
        }
      }

      return {
        data: response.data || {},
        code: response.code,
        message: response.message,
        error_details: response.error_details || [],
        request_id: response.request_id,
        response_time: response.response_time
      };
    } catch (error) {
      console.error('获取VC发货单详情失败:', error.message);
      throw error;
    }
  }
}

export default new LingXingVcService();

