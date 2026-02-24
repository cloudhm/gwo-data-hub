import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';
import lingXingSyncStateService from '../sync/lingXingSyncStateService.js';

const LOG_PREFIX = '[IncrementalSync]';

/**
 * 领星ERP亚马逊原表数据服务
 * 用于查询亚马逊源报表数据
 */
class LingXingAmazonService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询亚马逊源报表-所有订单
   * 查询 All Orders Report By last update 报表
   * API: POST /erp/sc/data/mws_report/allOrders
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - date_type: 时间查询类型（可选，默认1）：1 下单日期，2 亚马逊订单更新时间
   *   - start_date: 亚马逊当地下单时间，左闭区间，格式：Y-m-d（必填）
   *   - end_date: 亚马逊当地下单时间，右开区间，格式：Y-m-d（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 订单数据 { data: [], total: 0 }
   */
  async getAllOrdersReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.date_type !== undefined) {
        requestParams.date_type = parseInt(params.date_type);
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取订单数据（令牌桶容量为10，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/allOrders', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取订单数据失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊所有订单报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-FBA订单
   * 查询 Amazon-Fulfilled Shipments Report 报表
   * API: POST /erp/sc/data/mws_report/fbaOrders
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - date_type: 日期搜索维度（可选，默认1）：1 下单日期，2 配送日期
   *   - start_date: 开始日期，左闭区间，格式：Y-m-d（必填）
   *   - end_date: 结束日期，右开区间，格式：Y-m-d（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} FBA订单数据 { data: [], total: 0 }
   */
  async getFbaOrdersReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.date_type !== undefined) {
        requestParams.date_type = parseInt(params.date_type);
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取FBA订单数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/fbaOrders', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBA订单数据失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (orders && orders.length > 0) {
        await this.saveAmazonReport(accountId, 'fbaOrders', orders, params.sid);
      }

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊FBA订单报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-FBA换货订单
   * 查询 Replacements Report 报表
   * API: POST /erp/sc/routing/data/order/fbaExchangeOrderList
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - start_date: 开始时间，左闭区间，格式：Y-m-d（必填）
   *   - end_date: 结束时间，右开区间，格式：Y-m-d（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} FBA换货订单数据 { data: [], total: 0 }
   */
  async getFbaExchangeOrdersReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取FBA换货订单数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/routing/data/order/fbaExchangeOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBA换货订单数据失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊FBA换货订单报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-FBA退货订单
   * 查询 FBA customer returns 报表
   * API: POST /erp/sc/data/mws_report/refundOrders
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - date_type: 时间查询类型（可选，默认1）：1 退货时间【站点时间】，2 更新时间【北京时间】
   *   - start_date: 开始时间，左闭右开，格式：Y-m-d（必填）
   *   - end_date: 结束时间，左闭右开，格式：Y-m-d（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} FBA退货订单数据 { data: [], total: 0 }
   */
  async getFbaRefundOrdersReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.date_type !== undefined) {
        requestParams.date_type = parseInt(params.date_type);
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取FBA退货订单数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/refundOrders', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBA退货订单数据失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊FBA退货订单报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-FBM退货订单
   * 查询 Returns Reports 报表
   * API: POST /erp/sc/routing/data/order/fbmReturnOrderList
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - start_date: 开始时间，左闭区间，格式：Y-m-d（必填）
   *   - end_date: 结束时间，右开区间，格式：Y-m-d（必填）
   *   - date_type: 时间查询类型（可选，默认1）：1 退货日期，2 下单日期
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} FBM退货订单数据 { data: [], total: 0 }
   */
  async getFbmReturnOrdersReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.date_type !== undefined) {
        requestParams.date_type = parseInt(params.date_type);
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取FBM退货订单数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/routing/data/order/fbmReturnOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBM退货订单数据失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊FBM退货订单报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-移除订单（新）
   * 查询 Reports-Fulfillment-Removal Order Detail 报表
   * 报表为seller_id维度，按sid请求会返回对应seller_id下所有移除订单数据，同一个seller_id授权的店铺任取一个sid请求报表数据即可
   * API: POST /erp/sc/routing/data/order/removalOrderListNew
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - start_date: 查询时间【更新时间】，左闭区间，格式：Y-m-d（必填）
   *   - end_date: 查询时间【更新时间】，右开区间，格式：Y-m-d（必填）
   *   - search_field_time: 搜索时间类型（可选，默认 last_updated_date）：last_updated_date 更新时间，request_date 创建时间
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 移除订单数据 { data: [], total: 0 }
   */
  async getRemovalOrdersReportNew(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        start_date: params.start_date,
        end_date: params.end_date,
        search_field_time: params.search_field_time || 'last_updated_date' // 默认值
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取移除订单数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/routing/data/order/removalOrderListNew', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取移除订单数据失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊移除订单报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-移除货件（新）
   * 查询 Reports-Fulfillment-Removal Shipment Detail 报表
   * 报表为seller_id维度，按sid请求会返回对应seller_id下所有移除订单数据，同一个seller_id授权的店铺任取一个sid请求报表数据即可
   * API: POST /erp/sc/statistic/removalShipment/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id【seller_id同时传值时，以sid为准】（可选，但sid和seller_id至少需要传一个）
   *   - seller_id: 亚马逊店铺id（可选，但sid和seller_id至少需要传一个）
   *   - start_date: 开始日期【发货日期】，左闭右开，格式：Y-m-d（必填）
   *   - end_date: 结束日期【发货日期】，左闭右开，格式：Y-m-d（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 移除货件数据 { data: [], total: 0 }
   */
  async getRemovalShipmentReportNew(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid && !params.seller_id) {
        throw new Error('店铺id (sid) 或 亚马逊店铺id (seller_id) 至少需要传一个');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        start_date: params.start_date,
        end_date: params.end_date
      };

      // sid 和 seller_id 至少传一个（如果同时传，以sid为准）
      if (params.sid !== undefined) {
        requestParams.sid = parseInt(params.sid);
      }
      if (params.seller_id !== undefined) {
        requestParams.seller_id = params.seller_id;
      }

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取移除货件数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/statistic/removalShipment/list', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取移除货件数据失败');
      }

      const shipments = response.data || [];
      const total = response.total || 0;

      return {
        data: shipments,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊移除货件报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-FBA库存
   * 查询 FBA Manage Inventory 报表
   * API: POST /erp/sc/data/mws_report/manageInventory
   * 令牌桶容量: 5
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} FBA库存数据 { data: [], total: 0 }
   */
  async getFbaInventoryReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid)
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取FBA库存数据（令牌桶容量为5，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/manageInventory', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBA库存数据失败');
      }

      const inventory = response.data || [];
      const total = response.total || 0;

      return {
        data: inventory,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊FBA库存报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-每日库存
   * 查询 FBA Daily Inventory History Report 报表
   * 注意：由于亚马逊对应报表下线，2023年12月1日后不再更新此接口数据，获取数据请使用 查询库存分类账summary数据
   * API: POST /erp/sc/data/mws_report/dailyInventory
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id【欧洲传UK下的店铺，美国传US下的店铺】（必填）
   *   - event_date: 报表日期，格式：Y-m-d（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 每日库存数据 { data: [], total: 0 }
   */
  async getDailyInventoryReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.event_date) {
        throw new Error('报表日期 (event_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        event_date: params.event_date
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取每日库存数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/dailyInventory', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取每日库存数据失败');
      }

      const inventory = response.data || [];
      const total = response.total || 0;

      return {
        data: inventory,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊每日库存报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-FBA可售库存
   * 查询 FBA Multi-Country Inventory Report 报表
   * API: POST /erp/sc/data/mws_report/getAfnFulfillableQuantity
   * 令牌桶容量: 5
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} FBA可售库存数据 { data: [], total: 0 }
   */
  async getFbaFulfillableQuantityReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid)
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取FBA可售库存数据（令牌桶容量为5，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/getAfnFulfillableQuantity', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBA可售库存数据失败');
      }

      const inventory = response.data || [];
      const total = response.total || 0;

      return {
        data: inventory,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊FBA可售库存报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-预留库存
   * 查询 FBA Reserved Inventory Report 报表
   * API: POST /erp/sc/data/mws_report/reservedInventory
   * 令牌桶容量: 5
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 预留库存数据 { data: [], total: 0 }
   */
  async getReservedInventoryReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid)
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取预留库存数据（令牌桶容量为5，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/reservedInventory', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取预留库存数据失败');
      }

      const inventory = response.data || [];
      const total = response.total || 0;

      return {
        data: inventory,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊预留库存报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-库龄表
   * 查询 Manage Inventory Health 报表
   * API: POST /erp/sc/routing/fba/fbaStock/getFbaAgeList
   * 令牌桶容量: 3
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，多个使用英文逗号分隔（必填，字符串类型）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20）
   * @returns {Promise<Object>} 库龄表数据 { data: { list: [], total: 0 }, total: 0 }
   */
  async getFbaAgeListReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        sid: String(params.sid) // sid 是字符串类型，可以多个用逗号分隔
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取库龄表数据（令牌桶容量为3，使用默认值）
      const response = await this.post(account, '/erp/sc/routing/fba/fbaStock/getFbaAgeList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取库龄表数据失败');
      }

      // 这个接口的返回结构是 { data: { list: [], total: 0 }, total: 0 }
      const data = response.data || {};
      const list = data.list || [];
      const dataTotal = data.total || 0;
      const total = response.total || dataTotal;

      return {
        data: {
          list: list,
          total: dataTotal
        },
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊库龄表报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-交易明细
   * 查询领星插件下载的 Transaction 报表数据
   * 注意：本接口即将下线，建议使用查询结算中心 - 交易明细
   * API: POST /erp/sc/data/mws_report/transaction
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - event_date: 报表日期，格式：Y-m-d【每月３日后支持查询上月数据】（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 交易明细数据 { data: [], total: 0 }
   */
  async getTransactionReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.event_date) {
        throw new Error('报表日期 (event_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        event_date: params.event_date
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取交易明细数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/transaction', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取交易明细数据失败');
      }

      const transactions = response.data || [];
      const total = response.total || 0;

      return {
        data: transactions,
        total: total
      };
    } catch (error) {
      console.error('获取亚马逊交易明细报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-Amazon Fulfilled Shipments
   * 查询 Amazon Fulfilled Shipments 报表
   * API: POST /erp/sc/data/mws_report/getAmazonFulfilledShipmentsList
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - shipment_date_after: 快照开始时间【shipment_date_locale】，格式：Y-m-d hh-mm-ss，开始结束时间区间支持7天（必填）
   *   - shipment_date_before: 快照结束时间【shipment_date_locale】，格式：Y-m-d hh-mm-ss，开始结束时间区间支持7天（必填）
   *   - amazon_order_id: 订单ID数组（可选）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} Amazon Fulfilled Shipments 数据 { data: [], total: 0 }
   */
  async getAmazonFulfilledShipmentsReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.shipment_date_after) {
        throw new Error('快照开始时间 (shipment_date_after) 是必填参数，格式：Y-m-d hh-mm-ss');
      }
      if (!params.shipment_date_before) {
        throw new Error('快照结束时间 (shipment_date_before) 是必填参数，格式：Y-m-d hh-mm-ss');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        shipment_date_after: params.shipment_date_after,
        shipment_date_before: params.shipment_date_before
      };

      // 可选参数
      if (params.amazon_order_id !== undefined) {
        requestParams.amazon_order_id = Array.isArray(params.amazon_order_id) 
          ? params.amazon_order_id 
          : [params.amazon_order_id];
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取Amazon Fulfilled Shipments数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/getAmazonFulfilledShipmentsList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取Amazon Fulfilled Shipments数据失败');
      }

      const shipments = response.data || [];
      const total = response.total || 0;

      return {
        data: shipments,
        total: total
      };
    } catch (error) {
      console.error('获取Amazon Fulfilled Shipments报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-Inventory Event Detail
   * 查询 FBA Inventory Event Detail 报表
   * 注意：2023年3月后不再更新此接口数据【亚马逊对应报表下线】，获取之后的数据请使用 查询亚马逊库存分类账detail数据
   * API: POST /erp/sc/data/mws_report/getFbaInventoryEventDetailList
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - snapshot_date_after: 快照开始时间【snapshot_date_locale】，格式：Y-m-d，开始结束时间区间支持7天（必填）
   *   - snapshot_date_before: 快照结束时间【snapshot_date_locale】，格式：Y-m-d，开始结束时间区间支持7天（必填）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} Inventory Event Detail 数据 { data: [], total: 0 }
   */
  async getFbaInventoryEventDetailReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.sid) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.snapshot_date_after) {
        throw new Error('快照开始时间 (snapshot_date_after) 是必填参数，格式：Y-m-d');
      }
      if (!params.snapshot_date_before) {
        throw new Error('快照结束时间 (snapshot_date_before) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        snapshot_date_after: params.snapshot_date_after,
        snapshot_date_before: params.snapshot_date_before
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      }

      // 调用API获取Inventory Event Detail数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/erp/sc/data/mws_report/getFbaInventoryEventDetailList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取Inventory Event Detail数据失败');
      }

      const events = response.data || [];
      const total = response.total || 0;

      return {
        data: events,
        total: total
      };
    } catch (error) {
      console.error('获取Inventory Event Detail报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询亚马逊源报表-盘存记录
   * API: POST /basicOpen/openapi/mwsReport/adjustmentList
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（必填，默认0）
   *   - length: 分页长度（必填，默认20，上限10000）
   *   - sids: 店铺id，多个店铺以英文逗号分隔（可选）
   *   - search_field: 搜索的字段（可选）：asin ASIN, msku MSKU, fnsku FNSKU, item_name 标题, transaction_item_id 交易编号
   *   - search_value: 搜索值（可选）
   *   - start_date: 发货日期开始时间【闭区间】，格式Y-m-d【report_date】（必填）
   *   - end_date: 发货日期结束时间【闭区间】，格式Y-m-d【report_date】（必填）
   * @returns {Promise<Object>} 盘存记录数据 { data: [], total: 0 }
   */
  async getAdjustmentListReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.offset === undefined) {
        throw new Error('分页偏移量 (offset) 是必填参数');
      }
      if (params.length === undefined) {
        throw new Error('分页长度 (length) 是必填参数');
      }
      if (!params.start_date) {
        throw new Error('发货日期开始时间 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('发货日期结束时间 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        offset: parseInt(params.offset),
        length: parseInt(params.length),
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.sids !== undefined) {
        requestParams.sids = String(params.sids);
      }
      if (params.search_field !== undefined) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value !== undefined) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取盘存记录数据（令牌桶容量为1，使用默认值）
      const response = await this.post(account, '/basicOpen/openapi/mwsReport/adjustmentList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取盘存记录数据失败');
      }

      const adjustments = response.data || [];
      const total = response.total || 0;

      return {
        data: adjustments,
        total: total
      };
    } catch (error) {
      console.error('获取盘存记录报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有订单数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（必填参数：sid, start_date, end_date）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认1000）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => {}
   * @returns {Promise<Object>} { orders: [], total: 0, stats: {} }
   */
  async fetchAllOrdersReport(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有订单数据...');

      const allOrders = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取订单数据（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getAllOrdersReport(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条订单，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allOrders.push(...pageOrders);
          console.log(`获取完成，本页 ${pageOrders.length} 条订单，累计 ${allOrders.length} 条订单`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allOrders.length, totalCount);
          }

          if (pageOrders.length < pageSize || allOrders.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取订单数据失败:`, error.message);
          if (allOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有订单数据获取完成，共 ${allOrders.length} 条订单`);

      // 保存到数据库
      if (allOrders && allOrders.length > 0) {
        await this.saveAmazonReport(accountId, 'allOrders', allOrders, filterParams.sid);
      }

      return {
        orders: allOrders,
        total: allOrders.length,
        stats: {
          totalOrders: allOrders.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有订单数据失败:', error.message);
      throw error;
    }
  }

  /**
   * getAllOrdersReport 的增量同步方法
   * 按「上次同步结束日期」拉取每个店铺的订单增量，并更新同步状态，便于每日/定时只同步新数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项
   *   - endDate: string Y-m-d，同步截止日期，不传则到「昨天」
   *   - defaultLookbackDays: number 无历史状态时向前取多少天（默认 7）
   *   - timezone: string 计算「昨天」的时区（默认 'Asia/Shanghai'）
   *   - date_type: number 1=下单日期，2=亚马逊订单更新时间（默认 1）
   *   - pageSize: number 每页条数（默认 1000）
   *   - delayBetweenPages: number 分页间延迟 ms（默认 500）
   *   - delayBetweenShops: number 店铺间延迟 ms（默认 500）
   * @returns {Promise<{ results: Array<{ sid, success, recordCount, start_date, end_date, error? }>, summary: { totalShops, successCount, failCount, totalRecords } }>}
   */
  async incrementalSyncAllOrdersReport(accountId, options = {}) {
    const {
      endDate = null,
      defaultLookbackDays = 7,
      timezone = 'Asia/Shanghai',
      date_type = 1,
      pageSize = 1000,
      delayBetweenPages = 500,
      delayBetweenShops = 500
    } = options;

    const syncState = lingXingSyncStateService;
    const taskType = 'allOrders';

    console.log(`${LOG_PREFIX} [allOrders] 开始 accountId=${accountId} endDate=${endDate ?? '(昨天)'} defaultLookbackDays=${defaultLookbackDays} date_type=${date_type}`);

    const sellers = await prisma.lingXingSeller.findMany({
      where: { accountId, status: 1 },
      select: { sid: true },
      orderBy: { sid: 'asc' }
    });
    const sids = sellers.map(s => s.sid).filter(Boolean);

    if (sids.length === 0) {
      console.warn(`${LOG_PREFIX} [allOrders] accountId=${accountId} 无正常状态店铺，跳过`);
      return {
        results: [],
        summary: { totalShops: 0, successCount: 0, failCount: 0, totalRecords: 0, message: '该账户下无正常状态店铺' }
      };
    }

    console.log(`${LOG_PREFIX} [allOrders] accountId=${accountId} 共 ${sids.length} 个店铺: [${sids.join(', ')}]`);

    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (let i = 0; i < sids.length; i++) {
      const sid = sids[i];
      const dateRange = await syncState.getIncrementalDateRange(accountId, taskType, sid, {
        defaultLookbackDays,
        endDate,
        timezone
      });

      if (dateRange.isEmpty) {
        console.log(`${LOG_PREFIX} [allOrders] accountId=${accountId} sid=${sid} 日期范围为空，跳过`);
        results.push({ sid, success: true, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, skipped: true });
        successCount++;
        if (i < sids.length - 1 && delayBetweenShops > 0) {
          await new Promise(r => setTimeout(r, delayBetweenShops));
        }
        continue;
      }

      try {
        console.log(`${LOG_PREFIX} [allOrders] accountId=${accountId} sid=${sid} 拉取 ${dateRange.start_date} ~ ${dateRange.end_date} ...`);
        const fetchResult = await this.fetchAllOrdersReport(accountId, {
          sid,
          start_date: dateRange.start_date,
          end_date: dateRange.end_date,
          date_type
        }, { pageSize, delayBetweenPages });

        const recordCount = fetchResult?.total ?? 0;
        totalRecords += recordCount;

        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: dateRange.end_date,
          lastSyncAt: new Date(),
          lastRecordCount: recordCount,
          lastStatus: 'success',
          lastErrorMessage: null
        });

        console.log(`${LOG_PREFIX} [allOrders] accountId=${accountId} sid=${sid} 成功 拉取${recordCount}条 lastEndDate=${dateRange.end_date}`);
        results.push({
          sid,
          success: true,
          recordCount,
          start_date: dateRange.start_date,
          end_date: dateRange.end_date
        });
        successCount++;
      } catch (err) {
        const message = err?.message || String(err);
        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: null,
          lastSyncAt: new Date(),
          lastRecordCount: null,
          lastStatus: 'failed',
          lastErrorMessage: message
        }).catch(() => {});

        console.error(`${LOG_PREFIX} [allOrders] accountId=${accountId} sid=${sid} 失败 ${dateRange.start_date}~${dateRange.end_date} error=${message}`);
        results.push({
          sid,
          success: false,
          recordCount: 0,
          start_date: dateRange.start_date,
          end_date: dateRange.end_date,
          error: message
        });
        failCount++;
      }

      if (i < sids.length - 1 && delayBetweenShops > 0) {
        await new Promise(r => setTimeout(r, delayBetweenShops));
      }
    }

    console.log(`${LOG_PREFIX} [allOrders] 结束 accountId=${accountId} 店铺=${sids.length} 成功=${successCount} 失败=${failCount} 总条数=${totalRecords}`);
    return {
      results,
      summary: {
        totalShops: sids.length,
        successCount,
        failCount,
        totalRecords
      }
    };
  }

  /**
   * 按店铺维度的增量同步通用方法（内部使用）
   * 适用于所有「sid + start_date + end_date」的报表接口
   * @private
   */
  async _incrementalSyncReportBySid(accountId, taskType, fetchAllFn, options = {}) {
    const {
      endDate = null,
      defaultLookbackDays = 7,
      timezone = 'Asia/Shanghai',
      delayBetweenShops = 500,
      pageSize = 1000,
      delayBetweenPages = 500,
      extraParams = {}
    } = options;

    const syncState = lingXingSyncStateService;
    console.log(`${LOG_PREFIX} [${taskType}] 开始 accountId=${accountId} endDate=${endDate ?? '(昨天)'} defaultLookbackDays=${defaultLookbackDays}`);

    const sellers = await prisma.lingXingSeller.findMany({
      where: { accountId, status: 1 },
      select: { sid: true },
      orderBy: { sid: 'asc' }
    });
    const sids = sellers.map(s => s.sid).filter(Boolean);

    if (sids.length === 0) {
      console.warn(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 无正常状态店铺，跳过`);
      return {
        results: [],
        summary: { totalShops: 0, successCount: 0, failCount: 0, totalRecords: 0, message: '该账户下无正常状态店铺' }
      };
    }

    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 共 ${sids.length} 个店铺: [${sids.join(', ')}]`);

    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (let i = 0; i < sids.length; i++) {
      const sid = sids[i];
      const dateRange = await syncState.getIncrementalDateRange(accountId, taskType, sid, {
        defaultLookbackDays,
        endDate,
        timezone
      });

      if (dateRange.isEmpty) {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 日期范围为空，跳过`);
        results.push({ sid, success: true, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, skipped: true });
        successCount++;
        if (i < sids.length - 1 && delayBetweenShops > 0) {
          await new Promise(r => setTimeout(r, delayBetweenShops));
        }
        continue;
      }

      try {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 拉取 ${dateRange.start_date} ~ ${dateRange.end_date} ...`);
        const filterParams = {
          sid,
          start_date: dateRange.start_date,
          end_date: dateRange.end_date,
          ...extraParams
        };
        const fetchResult = await fetchAllFn(accountId, filterParams, { pageSize, delayBetweenPages });
        const recordCount = fetchResult?.total ?? fetchResult?.data?.length ?? fetchResult?.orders?.length ?? 0;
        totalRecords += recordCount;

        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: dateRange.end_date,
          lastSyncAt: new Date(),
          lastRecordCount: recordCount,
          lastStatus: 'success',
          lastErrorMessage: null
        });

        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 成功 拉取${recordCount}条 lastEndDate=${dateRange.end_date}`);
        results.push({
          sid,
          success: true,
          recordCount,
          start_date: dateRange.start_date,
          end_date: dateRange.end_date
        });
        successCount++;
      } catch (err) {
        const message = err?.message || String(err);
        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: null,
          lastSyncAt: new Date(),
          lastRecordCount: null,
          lastStatus: 'failed',
          lastErrorMessage: message
        }).catch(() => {});

        console.error(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 失败 ${dateRange.start_date}~${dateRange.end_date} error=${message}`);
        results.push({
          sid,
          success: false,
          recordCount: 0,
          start_date: dateRange.start_date,
          end_date: dateRange.end_date,
          error: message
        });
        failCount++;
      }

      if (i < sids.length - 1 && delayBetweenShops > 0) {
        await new Promise(r => setTimeout(r, delayBetweenShops));
      }
    }

    console.log(`${LOG_PREFIX} [${taskType}] 结束 accountId=${accountId} 店铺=${sids.length} 成功=${successCount} 失败=${failCount} 总条数=${totalRecords}`);
    return {
      results,
      summary: {
        totalShops: sids.length,
        successCount,
        failCount,
        totalRecords
      }
    };
  }

  /**
   * FBA 订单报表增量同步
   */
  async incrementalSyncFbaOrdersReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'fbaOrders',
      (id, params, opts) => this.fetchAllFbaOrdersReport(id, params, opts),
      { ...options, extraParams: { date_type: options.date_type ?? 1 } }
    );
  }

  /**
   * FBA 换货订单报表增量同步
   */
  async incrementalSyncFbaExchangeOrdersReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'fbaExchangeOrders',
      (id, params, opts) => this.fetchAllFbaExchangeOrdersReport(id, params, opts),
      options
    );
  }

  /**
   * FBA 退货订单报表增量同步
   */
  async incrementalSyncFbaRefundOrdersReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'fbaRefundOrders',
      (id, params, opts) => this.fetchAllFbaRefundOrdersReport(id, params, opts),
      { ...options, extraParams: { date_type: options.date_type ?? 1 } }
    );
  }

  /**
   * FBM 退货订单报表增量同步
   */
  async incrementalSyncFbmReturnOrdersReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'fbmReturnOrders',
      (id, params, opts) => this.fetchAllFbmReturnOrdersReport(id, params, opts),
      { ...options, extraParams: { date_type: options.date_type ?? 1 } }
    );
  }

  /**
   * 移除订单报表增量同步
   */
  async incrementalSyncRemovalOrdersReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'removalOrders',
      (id, params, opts) => this.fetchAllRemovalOrdersReportNew(id, params, opts),
      options
    );
  }

  /**
   * 移除货件报表增量同步
   */
  async incrementalSyncRemovalShipmentReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'removalShipment',
      (id, params, opts) => this.fetchAllRemovalShipmentReportNew(id, params, opts),
      options
    );
  }

  /**
   * 交易明细报表增量同步（按 event_date 逐日拉取，使用 start_date/end_date 作为日期范围）
   * 注意：接口按单日 event_date 查询，本方法在范围内逐日请求并汇总
   */
  async incrementalSyncTransactionReport(accountId, options = {}) {
    const {
      endDate = null,
      defaultLookbackDays = 7,
      timezone = 'Asia/Shanghai',
      delayBetweenShops = 500,
      pageSize = 1000,
      delayBetweenPages = 500
    } = options;

    const syncState = lingXingSyncStateService;
    const taskType = 'transaction';
    console.log(`${LOG_PREFIX} [${taskType}] 开始 accountId=${accountId} endDate=${endDate ?? '(昨天)'} defaultLookbackDays=${defaultLookbackDays}`);

    const sellers = await prisma.lingXingSeller.findMany({
      where: { accountId, status: 1 },
      select: { sid: true },
      orderBy: { sid: 'asc' }
    });
    const sids = sellers.map(s => s.sid).filter(Boolean);

    if (sids.length === 0) {
      console.warn(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 无正常状态店铺，跳过`);
      return {
        results: [],
        summary: { totalShops: 0, successCount: 0, failCount: 0, totalRecords: 0, message: '该账户下无正常状态店铺' }
      };
    }

    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 共 ${sids.length} 个店铺: [${sids.join(', ')}]`);

    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (let i = 0; i < sids.length; i++) {
      const sid = sids[i];
      const dateRange = await syncState.getIncrementalDateRange(accountId, taskType, sid, {
        defaultLookbackDays,
        endDate,
        timezone
      });

      if (dateRange.isEmpty) {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 日期范围为空，跳过`);
        results.push({ sid, success: true, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, skipped: true });
        successCount++;
        if (i < sids.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
        continue;
      }

      let recordCount = 0;
      try {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 拉取 ${dateRange.start_date} ~ ${dateRange.end_date} (逐日) ...`);
        const start = new Date(dateRange.start_date + 'T00:00:00Z');
        const end = new Date(dateRange.end_date + 'T23:59:59Z');
        for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
          const event_date = d.toISOString().slice(0, 10);
          const pageResult = await this.getTransactionReport(accountId, { sid, event_date, offset: 0, length: pageSize });
          const total = pageResult?.total ?? 0;
          if (total > 0) {
            let offset = 0;
            const dayData = [];
            while (offset < total) {
              const pr = await this.getTransactionReport(accountId, { sid, event_date, offset, length: pageSize });
              dayData.push(...(pr?.data ?? []));
              offset += pageSize;
              if (offset < total && delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
            }
            if (dayData.length > 0) {
              await this.saveAmazonReport(accountId, 'transaction', dayData, sid);
            }
            recordCount += dayData.length;
          }
          if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
        }

        totalRecords += recordCount;
        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: dateRange.end_date,
          lastSyncAt: new Date(),
          lastRecordCount: recordCount,
          lastStatus: 'success',
          lastErrorMessage: null
        });
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 成功 拉取${recordCount}条 lastEndDate=${dateRange.end_date}`);
        results.push({ sid, success: true, recordCount, start_date: dateRange.start_date, end_date: dateRange.end_date });
        successCount++;
      } catch (err) {
        const message = err?.message || String(err);
        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: null,
          lastSyncAt: new Date(),
          lastRecordCount: null,
          lastStatus: 'failed',
          lastErrorMessage: message
        }).catch(() => {});
        console.error(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 失败 ${dateRange.start_date}~${dateRange.end_date} error=${message}`);
        results.push({ sid, success: false, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, error: message });
        failCount++;
      }

      if (i < sids.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
    }

    console.log(`${LOG_PREFIX} [${taskType}] 结束 accountId=${accountId} 店铺=${sids.length} 成功=${successCount} 失败=${failCount} 总条数=${totalRecords}`);
    return {
      results,
      summary: { totalShops: sids.length, successCount, failCount, totalRecords }
    };
  }

  /**
   * Amazon Fulfilled Shipments 报表增量同步（使用 shipment_date_after / shipment_date_before，区间最多 7 天）
   */
  async incrementalSyncAmazonFulfilledShipmentsReport(accountId, options = {}) {
    const {
      endDate = null,
      defaultLookbackDays = 7,
      timezone = 'Asia/Shanghai',
      delayBetweenShops = 500,
      pageSize = 1000,
      delayBetweenPages = 500
    } = options;

    const syncState = lingXingSyncStateService;
    const taskType = 'amazonFulfilledShipments';
    console.log(`${LOG_PREFIX} [${taskType}] 开始 accountId=${accountId} endDate=${endDate ?? '(昨天)'} defaultLookbackDays=${defaultLookbackDays}`);

    const sellers = await prisma.lingXingSeller.findMany({
      where: { accountId, status: 1 },
      select: { sid: true },
      orderBy: { sid: 'asc' }
    });
    const sids = sellers.map(s => s.sid).filter(Boolean);

    if (sids.length === 0) {
      console.warn(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 无正常状态店铺，跳过`);
      return {
        results: [],
        summary: { totalShops: 0, successCount: 0, failCount: 0, totalRecords: 0, message: '该账户下无正常状态店铺' }
      };
    }

    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 共 ${sids.length} 个店铺: [${sids.join(', ')}]`);

    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (let i = 0; i < sids.length; i++) {
      const sid = sids[i];
      const dateRange = await syncState.getIncrementalDateRange(accountId, taskType, sid, {
        defaultLookbackDays,
        endDate,
        timezone
      });

      if (dateRange.isEmpty) {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 日期范围为空，跳过`);
        results.push({ sid, success: true, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, skipped: true });
        successCount++;
        if (i < sids.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
        continue;
      }

      try {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 拉取 ${dateRange.start_date} ~ ${dateRange.end_date} ...`);
        const shipment_date_after = dateRange.start_date + ' 00:00:00';
        const shipment_date_before = dateRange.end_date + ' 23:59:59';
        const fetchResult = await this.fetchAllAmazonFulfilledShipmentsReport(accountId, {
          sid,
          shipment_date_after,
          shipment_date_before
        }, { pageSize, delayBetweenPages });
        const recordCount = fetchResult?.total ?? fetchResult?.data?.length ?? 0;
        totalRecords += recordCount;

        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: dateRange.end_date,
          lastSyncAt: new Date(),
          lastRecordCount: recordCount,
          lastStatus: 'success',
          lastErrorMessage: null
        });
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 成功 拉取${recordCount}条 lastEndDate=${dateRange.end_date}`);
        results.push({ sid, success: true, recordCount, start_date: dateRange.start_date, end_date: dateRange.end_date });
        successCount++;
      } catch (err) {
        const message = err?.message || String(err);
        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: null,
          lastSyncAt: new Date(),
          lastRecordCount: null,
          lastStatus: 'failed',
          lastErrorMessage: message
        }).catch(() => {});
        console.error(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 失败 ${dateRange.start_date}~${dateRange.end_date} error=${message}`);
        results.push({ sid, success: false, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, error: message });
        failCount++;
      }

      if (i < sids.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
    }

    console.log(`${LOG_PREFIX} [${taskType}] 结束 accountId=${accountId} 店铺=${sids.length} 成功=${successCount} 失败=${failCount} 总条数=${totalRecords}`);
    return { results, summary: { totalShops: sids.length, successCount, failCount, totalRecords } };
  }

  /**
   * FBA Inventory Event Detail 报表增量同步（使用 snapshot_date_after / snapshot_date_before，区间最多 7 天）
   */
  async incrementalSyncFbaInventoryEventDetailReport(accountId, options = {}) {
    const {
      endDate = null,
      defaultLookbackDays = 7,
      timezone = 'Asia/Shanghai',
      delayBetweenShops = 500,
      pageSize = 1000,
      delayBetweenPages = 500
    } = options;

    const syncState = lingXingSyncStateService;
    const taskType = 'fbaInventoryEventDetail';
    console.log(`${LOG_PREFIX} [${taskType}] 开始 accountId=${accountId} endDate=${endDate ?? '(昨天)'} defaultLookbackDays=${defaultLookbackDays}`);

    const sellers = await prisma.lingXingSeller.findMany({
      where: { accountId, status: 1 },
      select: { sid: true },
      orderBy: { sid: 'asc' }
    });
    const sids = sellers.map(s => s.sid).filter(Boolean);

    if (sids.length === 0) {
      console.warn(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 无正常状态店铺，跳过`);
      return {
        results: [],
        summary: { totalShops: 0, successCount: 0, failCount: 0, totalRecords: 0, message: '该账户下无正常状态店铺' }
      };
    }

    console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} 共 ${sids.length} 个店铺: [${sids.join(', ')}]`);

    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (let i = 0; i < sids.length; i++) {
      const sid = sids[i];
      const dateRange = await syncState.getIncrementalDateRange(accountId, taskType, sid, {
        defaultLookbackDays,
        endDate,
        timezone
      });

      if (dateRange.isEmpty) {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 日期范围为空，跳过`);
        results.push({ sid, success: true, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, skipped: true });
        successCount++;
        if (i < sids.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
        continue;
      }

      try {
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 拉取 ${dateRange.start_date} ~ ${dateRange.end_date} ...`);
        const fetchResult = await this.fetchAllFbaInventoryEventDetailReport(accountId, {
          sid,
          snapshot_date_after: dateRange.start_date,
          snapshot_date_before: dateRange.end_date
        }, { pageSize, delayBetweenPages });
        const recordCount = fetchResult?.total ?? fetchResult?.data?.length ?? 0;
        totalRecords += recordCount;

        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: dateRange.end_date,
          lastSyncAt: new Date(),
          lastRecordCount: recordCount,
          lastStatus: 'success',
          lastErrorMessage: null
        });
        console.log(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 成功 拉取${recordCount}条 lastEndDate=${dateRange.end_date}`);
        results.push({ sid, success: true, recordCount, start_date: dateRange.start_date, end_date: dateRange.end_date });
        successCount++;
      } catch (err) {
        const message = err?.message || String(err);
        await syncState.upsertSyncState(accountId, taskType, sid, {
          lastEndDate: null,
          lastSyncAt: new Date(),
          lastRecordCount: null,
          lastStatus: 'failed',
          lastErrorMessage: message
        }).catch(() => {});
        console.error(`${LOG_PREFIX} [${taskType}] accountId=${accountId} sid=${sid} 失败 ${dateRange.start_date}~${dateRange.end_date} error=${message}`);
        results.push({ sid, success: false, recordCount: 0, start_date: dateRange.start_date, end_date: dateRange.end_date, error: message });
        failCount++;
      }

      if (i < sids.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
    }

    console.log(`${LOG_PREFIX} [${taskType}] 结束 accountId=${accountId} 店铺=${sids.length} 成功=${successCount} 失败=${failCount} 总条数=${totalRecords}`);
    return { results, summary: { totalShops: sids.length, successCount, failCount, totalRecords } };
  }

  /**
   * 盘存记录（Adjustment List）报表增量同步
   * 接口使用 sids 参数，本方法按单店铺逐店同步并记录每店 lastEndDate
   */
  async incrementalSyncAdjustmentListReport(accountId, options = {}) {
    return this._incrementalSyncReportBySid(
      accountId,
      'adjustmentList',
      (id, params, opts) => this.fetchAllAdjustmentListReport(id, {
        sid: params.sid,
        start_date: params.start_date,
        end_date: params.end_date,
        sids: String(params.sid)
      }, opts),
      options
    );
  }

  /**
   * 自动拉取所有FBA订单数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（必填参数：sid, start_date, end_date）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认1000）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数
   * @returns {Promise<Object>} { orders: [], total: 0, stats: {} }
   */
  async fetchAllFbaOrdersReport(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有FBA订单数据...');

      const allOrders = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取FBA订单数据（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getFbaOrdersReport(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条FBA订单，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allOrders.push(...pageOrders);
          console.log(`获取完成，本页 ${pageOrders.length} 条FBA订单，累计 ${allOrders.length} 条FBA订单`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allOrders.length, totalCount);
          }

          if (pageOrders.length < pageSize || allOrders.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取FBA订单数据失败:`, error.message);
          if (allOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有FBA订单数据获取完成，共 ${allOrders.length} 条FBA订单`);

      // 数据已在 getFbaOrdersReport 中保存，此处无需重复保存

      return {
        orders: allOrders,
        total: allOrders.length,
        stats: {
          totalOrders: allOrders.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有FBA订单数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有FBA库存数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（必填参数：sid）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（可选，默认1000）
   *   - delayBetweenPages: 分页之间延迟（毫秒，可选，默认500）
   *   - onProgress: 进度回调函数
   * @returns {Promise<Object>} { inventory: [], total: 0, stats: {} }
   */
  async fetchAllFbaInventoryReport(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有FBA库存数据...');

      const allInventory = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取FBA库存数据（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getFbaInventoryReport(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageInventory = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条FBA库存，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allInventory.push(...pageInventory);
          console.log(`获取完成，本页 ${pageInventory.length} 条FBA库存，累计 ${allInventory.length} 条FBA库存`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allInventory.length, totalCount);
          }

          if (pageInventory.length < pageSize || allInventory.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取FBA库存数据失败:`, error.message);
          if (allInventory.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有FBA库存数据获取完成，共 ${allInventory.length} 条FBA库存`);

      // 保存到数据库
      if (allInventory && allInventory.length > 0) {
        await this.saveAmazonReport(accountId, 'fbaInventory', allInventory, filterParams.sid);
      }

      return {
        inventory: allInventory,
        total: allInventory.length,
        stats: {
          totalInventory: allInventory.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有FBA库存数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有FBA换货订单数据（自动处理分页）
   */
  async fetchAllFbaExchangeOrdersReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      'FBA换货订单',
      (accountId, params) => this.getFbaExchangeOrdersReport(accountId, params),
      'fbaExchangeOrders'
    );
  }

  /**
   * 自动拉取所有FBA退货订单数据（自动处理分页）
   */
  async fetchAllFbaRefundOrdersReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      'FBA退货订单',
      (accountId, params) => this.getFbaRefundOrdersReport(accountId, params),
      'fbaRefundOrders'
    );
  }

  /**
   * 自动拉取所有FBM退货订单数据（自动处理分页）
   */
  async fetchAllFbmReturnOrdersReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      'FBM退货订单',
      (accountId, params) => this.getFbmReturnOrdersReport(accountId, params),
      'fbmReturnOrders'
    );
  }

  /**
   * 自动拉取所有移除订单数据（自动处理分页）
   */
  async fetchAllRemovalOrdersReportNew(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      '移除订单',
      (accountId, params) => this.getRemovalOrdersReportNew(accountId, params),
      'removalOrders'
    );
  }

  /**
   * 自动拉取所有移除货件数据（自动处理分页）
   */
  async fetchAllRemovalShipmentReportNew(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      '移除货件',
      (accountId, params) => this.getRemovalShipmentReportNew(accountId, params),
      'removalShipment'
    );
  }

  /**
   * 自动拉取所有FBA可售库存数据（自动处理分页）
   */
  async fetchAllFbaFulfillableQuantityReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      'FBA可售库存',
      (accountId, params) => this.getFbaFulfillableQuantityReport(accountId, params),
      'fbaFulfillableQuantity'
    );
  }

  /**
   * 自动拉取所有预留库存数据（自动处理分页）
   */
  async fetchAllReservedInventoryReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      '预留库存',
      (accountId, params) => this.getReservedInventoryReport(accountId, params),
      'reservedInventory'
    );
  }

  /**
   * 自动拉取所有交易明细数据（自动处理分页）
   */
  async fetchAllTransactionReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      '交易明细',
      (accountId, params) => this.getTransactionReport(accountId, params),
      'transaction'
    );
  }

  /**
   * 自动拉取所有Amazon Fulfilled Shipments数据（自动处理分页）
   */
  async fetchAllAmazonFulfilledShipmentsReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      'Amazon Fulfilled Shipments',
      (accountId, params) => this.getAmazonFulfilledShipmentsReport(accountId, params),
      'amazonFulfilledShipments'
    );
  }

  /**
   * 自动拉取所有Inventory Event Detail数据（自动处理分页）
   */
  async fetchAllFbaInventoryEventDetailReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      'Inventory Event Detail',
      (accountId, params) => this.getFbaInventoryEventDetailReport(accountId, params),
      'fbaInventoryEventDetail'
    );
  }

  /**
   * 自动拉取所有盘存记录数据（自动处理分页）
   */
  async fetchAllAdjustmentListReport(accountId, filterParams = {}, options = {}) {
    return this._fetchAllData(
      accountId,
      filterParams,
      options,
      '盘存记录',
      (accountId, params) => this.getAdjustmentListReport(accountId, params),
      'adjustmentList'
    );
  }

  /**
   * 通用的自动拉取所有数据方法（内部使用）
   * @private
   */
  async _fetchAllData(accountId, filterParams, options, dataTypeName, fetchMethod, reportType) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log(`开始自动拉取所有${dataTypeName}数据...`);

      const allData = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取${dataTypeName}数据（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await fetchMethod(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条${dataTypeName}，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`获取完成，本页 ${pageData.length} 条${dataTypeName}，累计 ${allData.length} 条${dataTypeName}`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allData.length, totalCount);
          }

          if (pageData.length < pageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取${dataTypeName}数据失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有${dataTypeName}数据获取完成，共 ${allData.length} 条${dataTypeName}`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveAmazonReport(accountId, reportType, allData, filterParams.sid);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalCount: allData.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error(`自动拉取所有${dataTypeName}数据失败:`, error.message);
      throw error;
    }
  }

  /**
   * 从数据项中提取 amazon_order_id
   * @param {Object} item - 数据项
   * @returns {string|null} amazon_order_id 或 null
   */
  _extractAmazonOrderId(item) {
    // 尝试多种可能的字段名
    return item.amazon_order_id || 
           item['amazon-order-id'] || 
           item.amazonOrderId || 
           item.order_id || 
           item['order-id'] || 
           item.orderId || 
           null;
  }

  /**
   * 保存亚马逊报表数据到数据库
   * @param {string} accountId - 领星账户ID
   * @param {string} reportType - 报表类型
   * @param {Array} reportData - 报表数据数组
   * @param {number} sid - 店铺ID（可选）
   */
  async saveAmazonReport(accountId, reportType, reportData, sid = null) {
    try {
      if (!prisma.lingXingAmazonReport) {
        console.error('Prisma Client 中未找到 lingXingAmazonReport 模型');
        return;
      }

      let createdCount = 0;
      let updatedCount = 0;

      // 提取所有有 amazon_order_id 的数据
      const itemsWithOrderId = reportData
        .map(item => ({
          item,
          amazonOrderId: this._extractAmazonOrderId(item)
        }))
        .filter(({ amazonOrderId }) => amazonOrderId !== null);

      // 批量查询已存在的记录（用于统计）
      let existingOrderIds = new Set();
      if (itemsWithOrderId.length > 0) {
        const amazonOrderIds = itemsWithOrderId.map(({ amazonOrderId }) => amazonOrderId);
        const existingRecords = await prisma.lingXingAmazonReport.findMany({
          where: {
            accountId: accountId,
            reportType: reportType,
            amazonOrderId: {
              in: amazonOrderIds
            }
          },
          select: {
            amazonOrderId: true
          }
        });
        existingRecords.forEach(record => {
          if (record.amazonOrderId) {
            existingOrderIds.add(record.amazonOrderId);
          }
        });
      }

      // 处理每条数据
      for (const reportItem of reportData) {
        const amazonOrderId = this._extractAmazonOrderId(reportItem);
        const itemSid = sid ? parseInt(sid) : (reportItem.sid ? parseInt(reportItem.sid) : null);

        if (amazonOrderId) {
          // 使用 upsert：如果存在则更新，不存在则创建
          await prisma.lingXingAmazonReport.upsert({
            where: {
              accountId_reportType_amazonOrderId: {
                accountId: accountId,
                reportType: reportType,
                amazonOrderId: amazonOrderId
              }
            },
            update: {
              sid: itemSid,
              data: reportItem
            },
            create: {
              accountId: accountId,
              reportType: reportType,
              sid: itemSid,
              amazonOrderId: amazonOrderId,
              data: reportItem
            }
          });

          // 统计：如果之前存在则是更新，否则是创建
          if (existingOrderIds.has(amazonOrderId)) {
            updatedCount++;
          } else {
            createdCount++;
            existingOrderIds.add(amazonOrderId); // 添加到集合中，避免重复统计
          }
        } else {
          // 如果没有 amazon_order_id，则直接创建（可能是其他类型的报表数据）
          await prisma.lingXingAmazonReport.create({
            data: {
              accountId: accountId,
              reportType: reportType,
              sid: itemSid,
              data: reportItem
            }
          });
          createdCount++;
        }
      }

      console.log(`${reportType}报表数据已保存到数据库: 新增 ${createdCount} 条，更新 ${updatedCount} 条，共 ${reportData.length} 条记录`);
    } catch (error) {
      console.error(`保存${reportType}报表数据到数据库失败:`, error.message);
      throw error;
    }
  }
}

// 导出单例
export default new LingXingAmazonService();

