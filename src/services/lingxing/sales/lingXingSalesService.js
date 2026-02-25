import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';
import { runAccountLevelIncrementalSync } from '../sync/lingXingIncrementalRunner.js';

/**
 * 领星ERP销售服务
 * 亚马逊订单相关接口
 */
class LingXingSalesService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询亚马逊订单列表
   * API: POST /erp/sc/data/mws/orders
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id（可选）
   *   - sid_list: 店铺id列表，最大长度20（可选）
   *   - start_date: 查询时间开始，格式：Y-m-d 或 Y-m-d H:i:s（必填）
   *   - end_date: 查询时间结束，格式：Y-m-d 或 Y-m-d H:i:s（必填）
   *   - date_type: 查询日期类型：1 订购时间，2 订单修改时间，3 平台更新时间，10 发货时间（默认1）
   *   - order_status: 订单状态数组，如 ["Pending", "Unshipped"]（可选）
   *   - sort_desc_by_date_type: 是否按查询日期类型排序：0 否，1 降序，2 升序（默认0）
   *   - fulfillment_channel: 配送方式：1 亚马逊订单-AFN，2 自发货-MFN（可选）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认1000，上限5000（可选）
   * @returns {Promise<Object>} 订单列表数据 { data: [], total: 0 }
   */
  async getAmazonOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.start_date || !params.end_date) {
        throw new Error('start_date 和 end_date 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        start_date: params.start_date,
        end_date: params.end_date,
        ...(params.sid !== undefined && { sid: params.sid }),
        ...(params.sid_list && params.sid_list.length > 0 && { sid_list: params.sid_list }),
        ...(params.date_type !== undefined && { date_type: params.date_type }),
        ...(params.order_status && params.order_status.length > 0 && { order_status: params.order_status }),
        ...(params.sort_desc_by_date_type !== undefined && { sort_desc_by_date_type: params.sort_desc_by_date_type }),
        ...(params.fulfillment_channel !== undefined && { fulfillment_channel: params.fulfillment_channel }),
        ...(params.offset !== undefined && { offset: params.offset }),
        ...(params.length !== undefined && { length: params.length })
      };

      // 调用API获取订单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/data/mws/orders', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取订单列表失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      // 保存订单列表到数据库
      if (orders.length > 0) {
        await this.saveAmazonOrders(accountId, orders);
      }

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊订单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊 Listing（【销售】>【Listing】）
   * 唯一键：sid + seller_sku
   * API: POST /erp/sc/data/mws/listing  令牌桶容量 1，需串行调用
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，多个使用英文逗号分隔，对应亚马逊店铺列表的 sid（必填）
   *   - is_pair: 是否配对：1 已配对，2 未配对（可选）
   *   - is_delete: 是否删除：0 未删除，1 已删除（可选）
   *   - pair_update_start_time: 配对更新时间开始，北京时间 Y-m-d H:i:s，需 is_pair=1（可选）
   *   - pair_update_end_time: 配对更新时间结束，北京时间 Y-m-d H:i:s，需 is_pair=1（可选）
   *   - listing_update_start_time: All Listing 报表更新时间开始，零时区 Y-m-d H:i:s（可选）
   *   - listing_update_end_time: All Listing 报表更新时间结束，零时区 Y-m-d H:i:s（可选）
   *   - search_field: 搜索字段 seller_sku / asin / sku（可选）
   *   - search_value: 搜索值数组，上限10个（可选）
   *   - exact_search: 0 模糊搜索，1 精确搜索，默认1（可选）
   *   - store_type: 1 非低价商店，2 低价商店商品（可选）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认1000，上限1000（可选）
   * @returns {Promise<Object>} { data: [], total: 0 }
   */
  async getListingList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      if (params.sid === undefined || params.sid === null || params.sid === '') {
        throw new Error('sid 为必填参数，多个店铺使用英文逗号分隔');
      }

      const sidStr = Array.isArray(params.sid) ? params.sid.join(',') : String(params.sid).trim();
      if (!sidStr) {
        throw new Error('sid 为必填参数，多个店铺使用英文逗号分隔');
      }

      const requestParams = {
        sid: sidStr,
        ...(params.is_pair !== undefined && { is_pair: params.is_pair }),
        ...(params.is_delete !== undefined && { is_delete: params.is_delete }),
        ...(params.pair_update_start_time && { pair_update_start_time: params.pair_update_start_time }),
        ...(params.pair_update_end_time && { pair_update_end_time: params.pair_update_end_time }),
        ...(params.listing_update_start_time && { listing_update_start_time: params.listing_update_start_time }),
        ...(params.listing_update_end_time && { listing_update_end_time: params.listing_update_end_time }),
        ...(params.search_field && { search_field: params.search_field }),
        ...(params.search_value && Array.isArray(params.search_value) && params.search_value.length > 0 && { search_value: params.search_value }),
        ...(params.exact_search !== undefined && { exact_search: params.exact_search }),
        ...(params.store_type !== undefined && { store_type: params.store_type }),
        ...(params.offset !== undefined && { offset: params.offset }),
        ...(params.length !== undefined && { length: Math.min(Number(params.length) || 1000, 1000) })
      };

      const response = await this.post(account, '/erp/sc/data/mws/listing', requestParams, {
        successCode: [0, 200, '200']
      });

      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取 Listing 列表失败');
      }

      const data = response.data || [];
      const total = response.total ?? data.length;

      return {
        data,
        total
      };
    } catch (error) {
      console.error('获取亚马逊 Listing 列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊订单详情
   * API: POST /erp/sc/data/mws/orderDetail
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - order_id: 亚马逊订单号，多个使用英文逗号分隔，上限200（必填）
   * @returns {Promise<Array>} 订单详情数组
   */
  async getAmazonOrderDetail(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.order_id) {
        throw new Error('order_id 为必填参数');
      }

      // 验证订单号数量（上限200）
      const orderIds = params.order_id.split(',').map(id => id.trim()).filter(id => id);
      if (orderIds.length > 200) {
        throw new Error('order_id 最多支持200个订单号');
      }

      // 构建请求参数
      const requestParams = {
        order_id: params.order_id
      };

      // 调用API获取订单详情（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/data/mws/orderDetail', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取订单详情失败');
      }

      const orderDetails = response.data || [];

      // 保存订单详情到数据库
      if (orderDetails.length > 0) {
        await this.saveAmazonOrderDetails(accountId, orderDetails);
      }

      return orderDetails;
    } catch (error) {
      console.error('获取亚马逊订单详情失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存亚马逊订单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} orders - 订单列表数据
   */
  async saveAmazonOrders(accountId, orders) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingAmazonOrder) {
        console.error('Prisma Client 中未找到 lingXingAmazonOrder 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const order of orders) {
        if (!order.amazon_order_id) {
          continue;
        }

        await prisma.lingXingAmazonOrder.upsert({
          where: {
            amazonOrderId: order.amazon_order_id
          },
          update: {
            accountId: accountId,
            sid: order.sid !== undefined && order.sid !== null ? parseInt(order.sid) : null,
            sellerName: order.seller_name || null,
            orderStatus: order.order_status || null,
            orderTotalAmount: order.order_total_amount !== undefined && order.order_total_amount !== null && order.order_total_amount !== '' ? parseFloat(order.order_total_amount) : null,
            orderTotalCurrencyCode: order.order_total_currency_code || null,
            fulfillmentChannel: order.fulfillment_channel || null,
            postalCode: order.postal_code || null,
            isReturn: order.is_return !== undefined ? parseInt(order.is_return) : null,
            isMcfOrder: order.is_mcf_order !== undefined ? parseInt(order.is_mcf_order) : null,
            isAssessed: order.is_assessed !== undefined ? parseInt(order.is_assessed) : null,
            isReplacedOrder: order.is_replaced_order !== undefined ? parseInt(order.is_replaced_order) : null,
            isReplacementOrder: order.is_replacement_order !== undefined ? parseInt(order.is_replacement_order) : null,
            isReturnOrder: order.is_return_order !== undefined ? parseInt(order.is_return_order) : null,
            salesChannel: order.sales_channel || null,
            trackingNumber: order.tracking_number || null,
            refundAmount: order.refund_amount !== undefined && order.refund_amount !== null && order.refund_amount !== '' ? parseFloat(order.refund_amount) : null,
            purchaseDateLocal: order.purchase_date_local || null,
            purchaseDateLocalUtc: order.purchase_date_local_utc || null,
            shipmentDate: order.shipment_date || null,
            shipmentDateUtc: order.shipment_date_utc || null,
            shipmentDateLocal: order.shipment_date_local || null,
            lastUpdateDate: order.last_update_date || null,
            lastUpdateDateUtc: order.last_update_date_utc || null,
            postedDate: order.posted_date || null,
            postedDateUtc: order.posted_date_utc || null,
            purchaseDate: order.purchase_date || null,
            purchaseDateUtc: order.purchase_date_utc || null,
            earliestShipDate: order.earliest_ship_date || null,
            earliestShipDateUtc: order.earliest_ship_date_utc || null,
            gmtModified: order.gmt_modified || null,
            gmtModifiedUtc: order.gmt_modified_utc || null,
            orderData: order, // 保存完整数据
            archived: false,
            updatedAt: new Date()
          },
          create: {
            amazonOrderId: order.amazon_order_id,
            accountId: accountId,
            sid: order.sid !== undefined && order.sid !== null ? parseInt(order.sid) : null,
            sellerName: order.seller_name || null,
            orderStatus: order.order_status || null,
            orderTotalAmount: order.order_total_amount !== undefined && order.order_total_amount !== null && order.order_total_amount !== '' ? parseFloat(order.order_total_amount) : null,
            orderTotalCurrencyCode: order.order_total_currency_code || null,
            fulfillmentChannel: order.fulfillment_channel || null,
            postalCode: order.postal_code || null,
            isReturn: order.is_return !== undefined ? parseInt(order.is_return) : null,
            isMcfOrder: order.is_mcf_order !== undefined ? parseInt(order.is_mcf_order) : null,
            isAssessed: order.is_assessed !== undefined ? parseInt(order.is_assessed) : null,
            isReplacedOrder: order.is_replaced_order !== undefined ? parseInt(order.is_replaced_order) : null,
            isReplacementOrder: order.is_replacement_order !== undefined ? parseInt(order.is_replacement_order) : null,
            isReturnOrder: order.is_return_order !== undefined ? parseInt(order.is_return_order) : null,
            salesChannel: order.sales_channel || null,
            trackingNumber: order.tracking_number || null,
            refundAmount: order.refund_amount !== undefined && order.refund_amount !== null && order.refund_amount !== '' ? parseFloat(order.refund_amount) : null,
            purchaseDateLocal: order.purchase_date_local || null,
            purchaseDateLocalUtc: order.purchase_date_local_utc || null,
            shipmentDate: order.shipment_date || null,
            shipmentDateUtc: order.shipment_date_utc || null,
            shipmentDateLocal: order.shipment_date_local || null,
            lastUpdateDate: order.last_update_date || null,
            lastUpdateDateUtc: order.last_update_date_utc || null,
            postedDate: order.posted_date || null,
            postedDateUtc: order.posted_date_utc || null,
            purchaseDate: order.purchase_date || null,
            purchaseDateUtc: order.purchase_date_utc || null,
            earliestShipDate: order.earliest_ship_date || null,
            earliestShipDateUtc: order.earliest_ship_date_utc || null,
            gmtModified: order.gmt_modified || null,
            gmtModifiedUtc: order.gmt_modified_utc || null,
            orderData: order, // 保存完整数据
            archived: false
          }
        });

        // 保存订单项
        if (order.item_list && order.item_list.length > 0) {
          await this.saveAmazonOrderItems(order.amazon_order_id, order.item_list);
        }
      }

      console.log(`订单列表已保存到数据库: 共 ${orders.length} 个订单`);
    } catch (error) {
      console.error('保存订单列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 保存亚马逊订单项到数据库
   * @param {string} amazonOrderId - 亚马逊订单号
   * @param {Array} items - 订单项数组
   */
  async saveAmazonOrderItems(amazonOrderId, items) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingAmazonOrder || !prisma.lingXingAmazonOrderItem) {
        console.error('Prisma Client 中未找到订单模型，请重新生成 Prisma Client 并重启服务器');
        return;
      }

      // 先查找订单ID
      const order = await prisma.lingXingAmazonOrder.findUnique({
        where: { amazonOrderId: amazonOrderId }
      });

      if (!order) {
        console.warn(`订单不存在，无法保存订单项: ${amazonOrderId}`);
        return;
      }

      for (const item of items) {
        const orderItemId = item.order_item_id || item.seller_sku || item.asin || `item_${Date.now()}_${Math.random()}`;
        
        await prisma.lingXingAmazonOrderItem.upsert({
          where: {
            orderId_orderItemId: {
              orderId: order.id,
              orderItemId: orderItemId
            }
          },
          update: {
            orderItemId: orderItemId,
            asin: item.asin || null,
            quantityOrdered: item.quantity_ordered !== undefined && item.quantity_ordered !== null ? parseInt(item.quantity_ordered) : null,
            quantityShipped: item.quantity_shipped !== undefined && item.quantity_shipped !== null ? parseInt(item.quantity_shipped) : null,
            sellerSku: item.seller_sku || null,
            localSku: item.local_sku || item.sku || null,
            localName: item.local_name || null,
            title: item.title || null,
            productId: item.product_id !== undefined && item.product_id !== null ? parseInt(item.product_id) : null,
            productName: item.product_name || null,
            picUrl: item.pic_url || null,
            asinUrl: item.asin_url || null,
            itemPriceAmount: item.item_price_amount !== undefined && item.item_price_amount !== null && item.item_price_amount !== '' ? parseFloat(item.item_price_amount) : null,
            itemTaxAmount: item.item_tax_amount !== undefined && item.item_tax_amount !== null && item.item_tax_amount !== '' ? parseFloat(item.item_tax_amount) : null,
            shippingPriceAmount: item.shipping_price_amount !== undefined && item.shipping_price_amount !== null && item.shipping_price_amount !== '' ? parseFloat(item.shipping_price_amount) : null,
            shippingTaxAmount: item.shipping_tax_amount !== undefined && item.shipping_tax_amount !== null && item.shipping_tax_amount !== '' ? parseFloat(item.shipping_tax_amount) : null,
            giftWrapPriceAmount: item.gift_wrap_price_amount !== undefined && item.gift_wrap_price_amount !== null && item.gift_wrap_price_amount !== '' ? parseFloat(item.gift_wrap_price_amount) : null,
            giftWrapTaxAmount: item.gift_wrap_tax_amount !== undefined && item.gift_wrap_tax_amount !== null && item.gift_wrap_tax_amount !== '' ? parseFloat(item.gift_wrap_tax_amount) : null,
            shippingDiscountAmount: item.shipping_discount_amount !== undefined && item.shipping_discount_amount !== null && item.shipping_discount_amount !== '' ? parseFloat(item.shipping_discount_amount) : null,
            codFeeAmount: item.cod_fee_amount !== undefined && item.cod_fee_amount !== null && item.cod_fee_amount !== '' ? parseFloat(item.cod_fee_amount) : null,
            promotionIds: item.promotion_ids || null,
            shippingDiscountTaxAmount: item.shipping_discount_tax_amount !== undefined && item.shipping_discount_tax_amount !== null && item.shipping_discount_tax_amount !== '' ? parseFloat(item.shipping_discount_tax_amount) : null,
            promotionDiscountAmount: item.promotion_discount_amount !== undefined && item.promotion_discount_amount !== null && item.promotion_discount_amount !== '' ? parseFloat(item.promotion_discount_amount) : null,
            promotionDiscountTaxAmount: item.promotion_discount_tax_amount !== undefined && item.promotion_discount_tax_amount !== null && item.promotion_discount_tax_amount !== '' ? parseFloat(item.promotion_discount_tax_amount) : null,
            codFeeDiscountAmount: item.cod_fee_discount_amount !== undefined && item.cod_fee_discount_amount !== null && item.cod_fee_discount_amount !== '' ? parseFloat(item.cod_fee_discount_amount) : null,
            giftMessageText: item.gift_message_text || null,
            giftWrapLevel: item.gift_wrap_level || null,
            conditionNote: item.condition_note || null,
            conditionId: item.condition_id || null,
            conditionSubtypeId: item.condition_subtype_id || null,
            scheduledDeliveryStartDate: item.scheduled_delivery_start_date || null,
            scheduledDeliveryEndDate: item.scheduled_delivery_end_date || null,
            priceDesignation: item.price_designation || null,
            cgPrice: item.cg_price !== undefined && item.cg_price !== null && item.cg_price !== '' ? parseFloat(item.cg_price) : null,
            feeName: item.fee_name || null,
            cgTransportCosts: item.cg_transport_costs !== undefined && item.cg_transport_costs !== null && item.cg_transport_costs !== '' ? parseFloat(item.cg_transport_costs) : null,
            fbaShipmentAmount: item.fba_shipment_amount !== undefined && item.fba_shipment_amount !== null && item.fba_shipment_amount !== '' ? parseFloat(item.fba_shipment_amount) : null,
            commissionAmount: item.commission_amount !== undefined && item.commission_amount !== null && item.commission_amount !== '' ? parseFloat(item.commission_amount) : null,
            otherAmount: item.other_amount !== undefined && item.other_amount !== null && item.other_amount !== '' ? parseFloat(item.other_amount) : null,
            feeCurrency: item.fee_currency || null,
            feeIcon: item.fee_icon || null,
            feeCostAmount: item.fee_cost_amount !== undefined && item.fee_cost_amount !== null && item.fee_cost_amount !== '' ? parseFloat(item.fee_cost_amount) : null,
            feeCost: item.fee_cost !== undefined && item.fee_cost !== null && item.fee_cost !== '' ? parseFloat(item.fee_cost) : null,
            salesPriceAmount: item.sales_price_amount !== undefined && item.sales_price_amount !== null && item.sales_price_amount !== '' ? parseFloat(item.sales_price_amount) : null,
            unitPriceAmount: item.unit_price_amount !== undefined && item.unit_price_amount !== null && item.unit_price_amount !== '' ? parseFloat(item.unit_price_amount) : null,
            taxAmount: item.tax_amount !== undefined && item.tax_amount !== null && item.tax_amount !== '' ? parseFloat(item.tax_amount) : null,
            promotionAmount: item.promotion_amount !== undefined && item.promotion_amount !== null && item.promotion_amount !== '' ? parseFloat(item.promotion_amount) : null,
            profit: item.profit !== undefined && item.profit !== null && item.profit !== '' ? parseFloat(item.profit) : null,
            itemDiscount: item.item_discount !== undefined && item.item_discount !== null && item.item_discount !== '' ? parseFloat(item.item_discount) : null,
            customizedJson: item.customized_json ? (typeof item.customized_json === 'string' ? JSON.parse(item.customized_json) : item.customized_json) : null,
            isSettled: item.is_settled !== undefined ? parseInt(item.is_settled) : null,
            itemData: item, // 保存完整数据
            archived: false,
            updatedAt: new Date()
          },
          create: {
            orderId: order.id,
            orderItemId: orderItemId,
            asin: item.asin || null,
            quantityOrdered: item.quantity_ordered !== undefined && item.quantity_ordered !== null ? parseInt(item.quantity_ordered) : null,
            quantityShipped: item.quantity_shipped !== undefined && item.quantity_shipped !== null ? parseInt(item.quantity_shipped) : null,
            sellerSku: item.seller_sku || null,
            localSku: item.local_sku || item.sku || null,
            localName: item.local_name || null,
            title: item.title || null,
            productId: item.product_id !== undefined && item.product_id !== null ? parseInt(item.product_id) : null,
            productName: item.product_name || null,
            picUrl: item.pic_url || null,
            asinUrl: item.asin_url || null,
            itemPriceAmount: item.item_price_amount !== undefined && item.item_price_amount !== null && item.item_price_amount !== '' ? parseFloat(item.item_price_amount) : null,
            itemTaxAmount: item.item_tax_amount !== undefined && item.item_tax_amount !== null && item.item_tax_amount !== '' ? parseFloat(item.item_tax_amount) : null,
            shippingPriceAmount: item.shipping_price_amount !== undefined && item.shipping_price_amount !== null && item.shipping_price_amount !== '' ? parseFloat(item.shipping_price_amount) : null,
            shippingTaxAmount: item.shipping_tax_amount !== undefined && item.shipping_tax_amount !== null && item.shipping_tax_amount !== '' ? parseFloat(item.shipping_tax_amount) : null,
            giftWrapPriceAmount: item.gift_wrap_price_amount !== undefined && item.gift_wrap_price_amount !== null && item.gift_wrap_price_amount !== '' ? parseFloat(item.gift_wrap_price_amount) : null,
            giftWrapTaxAmount: item.gift_wrap_tax_amount !== undefined && item.gift_wrap_tax_amount !== null && item.gift_wrap_tax_amount !== '' ? parseFloat(item.gift_wrap_tax_amount) : null,
            shippingDiscountAmount: item.shipping_discount_amount !== undefined && item.shipping_discount_amount !== null && item.shipping_discount_amount !== '' ? parseFloat(item.shipping_discount_amount) : null,
            codFeeAmount: item.cod_fee_amount !== undefined && item.cod_fee_amount !== null && item.cod_fee_amount !== '' ? parseFloat(item.cod_fee_amount) : null,
            promotionIds: item.promotion_ids || null,
            shippingDiscountTaxAmount: item.shipping_discount_tax_amount !== undefined && item.shipping_discount_tax_amount !== null && item.shipping_discount_tax_amount !== '' ? parseFloat(item.shipping_discount_tax_amount) : null,
            promotionDiscountAmount: item.promotion_discount_amount !== undefined && item.promotion_discount_amount !== null && item.promotion_discount_amount !== '' ? parseFloat(item.promotion_discount_amount) : null,
            promotionDiscountTaxAmount: item.promotion_discount_tax_amount !== undefined && item.promotion_discount_tax_amount !== null && item.promotion_discount_tax_amount !== '' ? parseFloat(item.promotion_discount_tax_amount) : null,
            codFeeDiscountAmount: item.cod_fee_discount_amount !== undefined && item.cod_fee_discount_amount !== null && item.cod_fee_discount_amount !== '' ? parseFloat(item.cod_fee_discount_amount) : null,
            giftMessageText: item.gift_message_text || null,
            giftWrapLevel: item.gift_wrap_level || null,
            conditionNote: item.condition_note || null,
            conditionId: item.condition_id || null,
            conditionSubtypeId: item.condition_subtype_id || null,
            scheduledDeliveryStartDate: item.scheduled_delivery_start_date || null,
            scheduledDeliveryEndDate: item.scheduled_delivery_end_date || null,
            priceDesignation: item.price_designation || null,
            cgPrice: item.cg_price !== undefined && item.cg_price !== null && item.cg_price !== '' ? parseFloat(item.cg_price) : null,
            feeName: item.fee_name || null,
            cgTransportCosts: item.cg_transport_costs !== undefined && item.cg_transport_costs !== null && item.cg_transport_costs !== '' ? parseFloat(item.cg_transport_costs) : null,
            fbaShipmentAmount: item.fba_shipment_amount !== undefined && item.fba_shipment_amount !== null && item.fba_shipment_amount !== '' ? parseFloat(item.fba_shipment_amount) : null,
            commissionAmount: item.commission_amount !== undefined && item.commission_amount !== null && item.commission_amount !== '' ? parseFloat(item.commission_amount) : null,
            otherAmount: item.other_amount !== undefined && item.other_amount !== null && item.other_amount !== '' ? parseFloat(item.other_amount) : null,
            feeCurrency: item.fee_currency || null,
            feeIcon: item.fee_icon || null,
            feeCostAmount: item.fee_cost_amount !== undefined && item.fee_cost_amount !== null && item.fee_cost_amount !== '' ? parseFloat(item.fee_cost_amount) : null,
            feeCost: item.fee_cost !== undefined && item.fee_cost !== null && item.fee_cost !== '' ? parseFloat(item.fee_cost) : null,
            salesPriceAmount: item.sales_price_amount !== undefined && item.sales_price_amount !== null && item.sales_price_amount !== '' ? parseFloat(item.sales_price_amount) : null,
            unitPriceAmount: item.unit_price_amount !== undefined && item.unit_price_amount !== null && item.unit_price_amount !== '' ? parseFloat(item.unit_price_amount) : null,
            taxAmount: item.tax_amount !== undefined && item.tax_amount !== null && item.tax_amount !== '' ? parseFloat(item.tax_amount) : null,
            promotionAmount: item.promotion_amount !== undefined && item.promotion_amount !== null && item.promotion_amount !== '' ? parseFloat(item.promotion_amount) : null,
            profit: item.profit !== undefined && item.profit !== null && item.profit !== '' ? parseFloat(item.profit) : null,
            itemDiscount: item.item_discount !== undefined && item.item_discount !== null && item.item_discount !== '' ? parseFloat(item.item_discount) : null,
            customizedJson: item.customized_json ? (typeof item.customized_json === 'string' ? JSON.parse(item.customized_json) : item.customized_json) : null,
            isSettled: item.is_settled !== undefined ? parseInt(item.is_settled) : null,
            itemData: item, // 保存完整数据
            archived: false
          }
        });
      }
    } catch (error) {
      console.error('保存订单项到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 保存亚马逊订单详情到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} orderDetails - 订单详情数组
   */
  async saveAmazonOrderDetails(accountId, orderDetails) {
    try {
      for (const orderDetail of orderDetails) {
        if (!orderDetail.amazon_order_id) {
          continue;
        }

        // 更新订单详情
        await prisma.lingXingAmazonOrder.updateMany({
          where: {
            amazonOrderId: orderDetail.amazon_order_id,
            accountId: accountId
          },
          data: {
            sid: orderDetail.sid ? parseInt(orderDetail.sid) : undefined,
            orderStatus: orderDetail.order_status || undefined,
            orderTotalAmount: orderDetail.order_total_amount ? parseFloat(orderDetail.order_total_amount) : undefined,
            orderTotalCurrencyCode: orderDetail.currency || undefined,
            fulfillmentChannel: orderDetail.fulfillment_channel || undefined,
            isAssessed: orderDetail.is_assessed !== undefined ? parseInt(orderDetail.is_assessed) : undefined,
            isMcfOrder: orderDetail.is_mcf_order !== undefined ? parseInt(orderDetail.is_mcf_order) : undefined,
            isReturnOrder: orderDetail.is_return_order !== undefined ? parseInt(orderDetail.is_return_order) : undefined,
            isReplacedOrder: orderDetail.is_replaced_order !== undefined ? parseInt(orderDetail.is_replaced_order) : undefined,
            isReplacementOrder: orderDetail.is_replacement_order !== undefined ? parseInt(orderDetail.is_replacement_order) : undefined,
            purchaseDateLocal: orderDetail.purchase_date_local || undefined,
            purchaseDateLocalUtc: orderDetail.purchase_date_local_utc || undefined,
            lastUpdateDate: orderDetail.last_update_date || undefined,
            lastUpdateDateUtc: orderDetail.last_update_date_utc || undefined,
            postedDate: orderDetail.posted_date || undefined,
            shipmentDate: orderDetail.shipment_date || undefined,
            earliestShipDate: orderDetail.earliest_ship_date || undefined,
            earliestShipDateUtc: orderDetail.earliest_ship_date_utc || undefined,
            orderDetail: orderDetail, // 保存完整详情数据
            updatedAt: new Date()
          }
        });

        // 保存订单项详情
        if (orderDetail.item_list && orderDetail.item_list.length > 0) {
          await this.saveAmazonOrderItems(orderDetail.amazon_order_id, orderDetail.item_list);
        }
      }

      console.log(`订单详情已保存到数据库: 共 ${orderDetails.length} 个订单`);
    } catch (error) {
      console.error('保存订单详情到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动同步所有亚马逊订单（自动处理分页）
   * 会自动分页获取所有订单，可选地自动批量查询订单详情
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 订单列表查询参数（同 getAmazonOrderList）
   *   - start_date: 查询时间开始（必填）
   *   - end_date: 查询时间结束（必填）
   *   - sid: 店铺id（可选）
   *   - sid_list: 店铺id列表（可选）
   *   - date_type: 查询日期类型（可选）
   *   - order_status: 订单状态数组（可选）
   *   - 其他查询参数...
   * @param {Object} options - 选项
   *   - fetchDetails: 是否自动批量查询订单详情（默认false）
   *   - pageSize: 每页大小（默认1000，最大5000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - batchSize: 批量查询详情时每批数量（默认200，最大200）
   *   - delayBetweenBatches: 批量查询批次之间的延迟时间（毫秒，默认1000）
   *   - maxRetries: 每批查询的最大重试次数（默认3）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { orders: [], orderDetails: [], total: 0, stats: {} }
   */
  async fetchAllAmazonOrders(accountId, listParams = {}, options = {}) {
    const {
      fetchDetails = false,
      pageSize = 1000,
      delayBetweenPages = 500,
      batchSize = 200,
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

      // 验证必填参数
      if (!listParams.start_date || !listParams.end_date) {
        throw new Error('start_date 和 end_date 为必填参数');
      }

      console.log('开始自动同步所有亚马逊订单...');

      const allOrders = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 5000); // 最大5000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 1. 自动分页获取所有订单
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页订单（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getAmazonOrderList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个订单，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allOrders.push(...pageOrders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageOrders.length} 个订单，累计 ${allOrders.length} 个订单`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allOrders.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageOrders.length < actualPageSize || allOrders.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页订单失败:`, error.message);
          // 如果单页失败，继续尝试下一页（避免整个流程中断）
          if (allOrders.length === 0) {
            throw error; // 如果第一页就失败，抛出错误
          }
          hasMore = false; // 停止分页
        }
      }

      console.log(`所有订单列表获取完成，共 ${allOrders.length} 个订单`);

      // 保存订单列表到数据库
      if (allOrders && allOrders.length > 0) {
        await this.saveAmazonOrders(accountId, allOrders);
      }

      const stats = {
        totalOrders: allOrders.length,
        pagesFetched: currentPage,
        orderDetailsFetched: 0,
        batchesProcessed: 0,
        successCount: 0,
        failedCount: 0
      };

      // 2. 如果启用，自动批量查询订单详情
      let allOrderDetails = [];
      if (fetchDetails && allOrders.length > 0) {
        console.log('开始自动批量查询订单详情...');

        // 提取订单号
        const orderIds = allOrders
          .map(o => o.amazon_order_id)
          .filter(id => id !== undefined && id !== null);

        if (orderIds.length > 0) {
          // 分批处理订单号（每批最多200个）
          const actualBatchSize = Math.min(batchSize, 200);
          const batches = [];
          for (let i = 0; i < orderIds.length; i += actualBatchSize) {
            batches.push(orderIds.slice(i, i + actualBatchSize));
          }

          console.log(`开始批量查询订单详情，共 ${batches.length} 批，每批最多 ${actualBatchSize} 个订单`);

          // 串行处理每批
          for (let i = 0; i < batches.length; i++) {
            const batch = batches[i];
            const batchNumber = i + 1;

            try {
              console.log(`处理第 ${batchNumber}/${batches.length} 批，包含 ${batch.length} 个订单`);

              let batchDetails = [];
              let retryCount = 0;
              let lastError = null;

              while (retryCount <= maxRetries) {
                try {
                  batchDetails = await this.getAmazonOrderDetail(accountId, {
                    order_id: batch.join(',')
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
                allOrderDetails.push(...batchDetails);
                stats.successCount += batchDetails.length;
                console.log(`第 ${batchNumber} 批查询成功，获取到 ${batchDetails.length} 个订单详情`);
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

          stats.orderDetailsFetched = allOrderDetails.length;
          console.log(`批量查询订单详情完成: 成功 ${stats.successCount} 个，失败 ${stats.failedCount} 个`);
        }
      }

      return {
        orders: allOrders,
        orderDetails: allOrderDetails,
        total: allOrders.length,
        stats: stats
      };
    } catch (error) {
      console.error('自动同步所有亚马逊订单失败:', error.message);
      throw error;
    }
  }

  /**
   * 销售-亚马逊订单增量同步（按日期范围）
   * date_type: 1 订购时间，2 订单修改时间，3 平台更新时间，10 发货时间（默认 2 优先使用修改时间）
   */
  async incrementalSyncAmazonOrders(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'salesAmazonOrder',
      { ...options, extraParams: { date_type: options.date_type ?? 2 } },
      async (id, params, opts) => {
        const res = await this.fetchAllAmazonOrders(id, params, opts);
        return { total: res?.total ?? res?.orders?.length ?? 0, data: res?.orders };
      }
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }
}

export default new LingXingSalesService();

