import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';

/**
 * 领星ERP财务管理服务
 * 用于查询财务管理相关数据
 */
class LingXingFinanceService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询费用类型列表
   * API: POST /bd/fee/management/open/feeManagement/otherFee/type
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Array>} 费用类型列表
   */
  async getFeeTypeList(accountId) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 调用API获取费用类型列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/bd/fee/management/open/feeManagement/otherFee/type',
        {},
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取费用类型列表失败');
      }

      const feeTypes = response.data || [];

      return feeTypes;
    } catch (error) {
      console.error('获取费用类型列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有费用类型列表
   * 注意：该接口可能不支持分页，一次性返回所有数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项（预留，用于未来扩展）
   *   - onProgress: 进度回调函数 (currentCount, totalCount) => void
   * @returns {Promise<Object>} { feeTypes: [], total: 0, stats: {} }
   */
  async fetchAllFeeTypes(accountId, options = {}) {
    const {
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有费用类型列表...');

      // 调用API获取费用类型列表
      const feeTypes = await this.getFeeTypeList(accountId);
      const total = feeTypes.length;

      console.log(`费用类型列表获取完成，共 ${total} 个费用类型`);

      // 保存到数据库
      if (feeTypes && feeTypes.length > 0) {
        await this.saveFeeTypes(accountId, feeTypes);
      }

      // 调用进度回调
      if (onProgress) {
        onProgress(total, total);
      }

      return {
        feeTypes: feeTypes,
        total: total,
        stats: {
          totalFeeTypes: total,
          pagesFetched: 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有费用类型列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询费用明细列表
   * API: POST /bd/fee/management/open/feeManagement/otherFee/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（必填）
   *   - length: 分页长度，默认20（必填）
   *   - date_type: 时间类型：gmt_create 创建日期，date 分摊日期（必填）
   *   - start_date: 开始时间，格式：Y-m-d（必填）
   *   - end_date: 结束时间，格式：Y-m-d（必填）
   *   - sids: 店铺id数组（可选）
   *   - other_fee_type_ids: 费用类型id数组（可选）
   *   - status_order: 单据状态：1 待提交，2 待审批，3 已处理，4 已驳回，5 已作废（可选）
   *   - dimensions: 分摊维度id数组：1 msku，2 asin，3 店铺，4 父asin，5 sku，6 企业（可选）
   *   - apportion_status: 分摊状态数组：1 未分摊，2 已分摊-新，3 已分摊-旧，4 已分摊（可选）
   *   - status_merge: 分摊状态：1 未分摊，2 已分摊（可选）
   *   - search_field: 搜索类型（可选）
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 费用明细数据 { data: [], total: 0 }
   */
  async getFeeDetailList(accountId, params = {}) {
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
      if (!params.date_type) {
        throw new Error('时间类型 (date_type) 是必填参数，可选值：gmt_create 或 date');
      }
      if (!params.start_date) {
        throw new Error('开始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        offset: parseInt(params.offset),
        length: parseInt(params.length),
        date_type: params.date_type,
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.other_fee_type_ids && Array.isArray(params.other_fee_type_ids)) {
        requestParams.other_fee_type_ids = params.other_fee_type_ids.map(id => parseInt(id));
      }
      if (params.status_order !== undefined) {
        requestParams.status_order = parseInt(params.status_order);
      }
      if (params.dimensions && Array.isArray(params.dimensions)) {
        requestParams.dimensions = params.dimensions.map(dim => parseInt(dim));
      }
      if (params.apportion_status && Array.isArray(params.apportion_status)) {
        requestParams.apportion_status = params.apportion_status.map(status => parseInt(status));
      }
      if (params.status_merge !== undefined) {
        requestParams.status_merge = parseInt(params.status_merge);
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取费用明细列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/bd/fee/management/open/feeManagement/otherFee/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取费用明细列表失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;

      return {
        data: records,
        total: total
      };
    } catch (error) {
      console.error('获取费用明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有费用明细列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 费用明细列表查询参数（同 getFeeDetailList，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20，上限100）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { feeDetails: [], total: 0, stats: {} }
   */
  async fetchAllFeeDetails(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有费用明细列表...');

      const allFeeDetails = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 100); // 最大100
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有费用明细
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页费用明细（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getFeeDetailList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageFeeDetails = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条费用明细，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allFeeDetails.push(...pageFeeDetails);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageFeeDetails.length} 条费用明细，累计 ${allFeeDetails.length} 条费用明细`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allFeeDetails.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageFeeDetails.length < actualPageSize || allFeeDetails.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页费用明细失败:`, error.message);
          if (allFeeDetails.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有费用明细列表获取完成，共 ${allFeeDetails.length} 条费用明细`);

      // 保存到数据库
      if (allFeeDetails && allFeeDetails.length > 0) {
        await this.saveFeeDetails(accountId, allFeeDetails);
      }

      return {
        feeDetails: allFeeDetails,
        total: allFeeDetails.length,
        stats: {
          totalFeeDetails: allFeeDetails.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有费用明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表-MSKU
   * API: POST /bd/profit/report/open/report/msku/list
   * 令牌桶容量: 10
   * 唯一键说明：dataDate+sid+msku+asin
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度，上限10000（可选，默认1000）
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - monthlyQuery: 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   - startDate: 开始时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - endDate: 结束时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - searchField: 搜索值类型，seller_sku（可选）
   *   - searchValue: 搜索的值数组（可选）
   *   - currencyCode: 币种code【默认原币种】（可选）
   *   - summaryEnabled: 是否按msku汇总返回：false 默认值，true（可选）
   *   - orderStatus: 交易状态（可选）
   *     Deferred 已推迟
   *     Disbursed 已发放【默认】
   *     DisbursedAndPreSettled 已发放（含预结算）
   *     All 全部（不包含已发放预结算数据）
   * @returns {Promise<Object>} 利润报表数据 { data: [], total: 0 }
   */
  async getMskuProfitReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate) {
        throw new Error('开始日期 (startDate) 是必填参数');
      }
      if (!params.endDate) {
        throw new Error('结束日期 (endDate) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 1000; // 默认值
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.monthlyQuery !== undefined) {
        requestParams.monthlyQuery = params.monthlyQuery === true || params.monthlyQuery === 'true';
      }
      if (params.searchField) {
        requestParams.searchField = params.searchField;
      }
      if (params.searchValue && Array.isArray(params.searchValue)) {
        requestParams.searchValue = params.searchValue;
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.summaryEnabled !== undefined) {
        requestParams.summaryEnabled = params.summaryEnabled === true || params.summaryEnabled === 'true';
      }
      if (params.orderStatus) {
        requestParams.orderStatus = params.orderStatus;
      }

      // 调用API获取利润报表-MSKU（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/msku/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润报表-MSKU失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;

      return {
        data: records,
        total: total
      };
    } catch (error) {
      console.error('获取利润报表-MSKU失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有利润报表-MSKU数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 利润报表查询参数（同 getMskuProfitReport，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { profitReports: [], total: 0, stats: {} }
   */
  async fetchAllMskuProfitReport(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有利润报表-MSKU数据...');

      const allProfitReports = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有利润报表数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页利润报表-MSKU（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getMskuProfitReport(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageProfitReports = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条利润报表-MSKU数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allProfitReports.push(...pageProfitReports);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProfitReports.length} 条数据，累计 ${allProfitReports.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allProfitReports.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageProfitReports.length < actualPageSize || allProfitReports.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页利润报表-MSKU失败:`, error.message);
          if (allProfitReports.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有利润报表-MSKU数据获取完成，共 ${allProfitReports.length} 条数据`);

      // 保存到数据库
      if (allProfitReports && allProfitReports.length > 0) {
        await this.saveMskuProfitReports(accountId, allProfitReports);
      }

      return {
        profitReports: allProfitReports,
        total: allProfitReports.length,
        stats: {
          totalProfitReports: allProfitReports.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有利润报表-MSKU数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表-ASIN
   * API: POST /bd/profit/report/open/report/asin/list
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度，上限10000（可选，默认1000）
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - monthlyQuery: 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   - startDate: 开始时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - endDate: 结束时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - searchField: 搜索值类型，ASIN（可选，默认asin）
   *   - searchValue: 搜索的值数组（可选）
   *   - currencyCode: 币种code（可选）
   *   - summaryEnabled: 是否按asin汇总返回：false 默认值，true（可选）
   *   - orderStatus: 交易状态（可选）
   *     Deferred 已推迟
   *     Disbursed 已发放【默认】
   *     DisbursedAndPreSettled 已发放（含预结算）
   *     All 全部
   * @returns {Promise<Object>} 利润报表数据 { data: [], total: 0 }
   */
  async getAsinProfitReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate) {
        throw new Error('开始日期 (startDate) 是必填参数');
      }
      if (!params.endDate) {
        throw new Error('结束日期 (endDate) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 1000; // 默认值
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.monthlyQuery !== undefined) {
        requestParams.monthlyQuery = params.monthlyQuery === true || params.monthlyQuery === 'true';
      }
      if (params.searchField) {
        requestParams.searchField = params.searchField;
      }
      if (params.searchValue && Array.isArray(params.searchValue)) {
        requestParams.searchValue = params.searchValue;
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.summaryEnabled !== undefined) {
        requestParams.summaryEnabled = params.summaryEnabled === true || params.summaryEnabled === 'true';
      }
      if (params.orderStatus) {
        requestParams.orderStatus = params.orderStatus;
      }

      // 调用API获取利润报表-ASIN（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/asin/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润报表-ASIN失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;

      return {
        data: records,
        total: total
      };
    } catch (error) {
      console.error('获取利润报表-ASIN失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有利润报表-ASIN数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 利润报表查询参数（同 getAsinProfitReport，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { profitReports: [], total: 0, stats: {} }
   */
  async fetchAllAsinProfitReport(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有利润报表-ASIN数据...');

      const allProfitReports = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有利润报表数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页利润报表-ASIN（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getAsinProfitReport(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageProfitReports = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条利润报表-ASIN数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allProfitReports.push(...pageProfitReports);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProfitReports.length} 条数据，累计 ${allProfitReports.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allProfitReports.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageProfitReports.length < actualPageSize || allProfitReports.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页利润报表-ASIN失败:`, error.message);
          if (allProfitReports.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有利润报表-ASIN数据获取完成，共 ${allProfitReports.length} 条数据`);

      // 保存到数据库
      if (allProfitReports && allProfitReports.length > 0) {
        await this.saveAsinProfitReports(accountId, allProfitReports);
      }

      return {
        profitReports: allProfitReports,
        total: allProfitReports.length,
        stats: {
          totalProfitReports: allProfitReports.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有利润报表-ASIN数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表-父ASIN
   * API: POST /bd/profit/report/open/report/parent/asin/list
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度，上限10000（可选，默认1000）
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - monthlyQuery: 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   - startDate: 开始时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - endDate: 结束时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - searchField: 搜索值类型，parent_asin（可选，默认parent_asin）
   *   - searchValue: 搜索的值数组（可选）
   *   - currencyCode: 币种code（可选）
   *   - summaryEnabled: 是否按父asin汇总返回：false 默认值，true（可选）
   *   - orderStatus: 交易状态（可选）
   *     Deferred 已推迟
   *     Disbursed 已发放【默认】
   *     DisbursedAndPreSettled 已发放（含预结算）
   *     All 全部
   * @returns {Promise<Object>} 利润报表数据 { data: [], total: 0 }
   */
  async getParentAsinProfitReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate) {
        throw new Error('开始日期 (startDate) 是必填参数');
      }
      if (!params.endDate) {
        throw new Error('结束日期 (endDate) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 1000; // 默认值
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.monthlyQuery !== undefined) {
        requestParams.monthlyQuery = params.monthlyQuery === true || params.monthlyQuery === 'true';
      }
      if (params.searchField) {
        requestParams.searchField = params.searchField;
      }
      if (params.searchValue && Array.isArray(params.searchValue)) {
        requestParams.searchValue = params.searchValue;
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.summaryEnabled !== undefined) {
        requestParams.summaryEnabled = params.summaryEnabled === true || params.summaryEnabled === 'true';
      }
      if (params.orderStatus) {
        requestParams.orderStatus = params.orderStatus;
      }

      // 调用API获取利润报表-父ASIN（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/parent/asin/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润报表-父ASIN失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;

      return {
        data: records,
        total: total
      };
    } catch (error) {
      console.error('获取利润报表-父ASIN失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有利润报表-父ASIN数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 利润报表查询参数（同 getParentAsinProfitReport，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { profitReports: [], total: 0, stats: {} }
   */
  async fetchAllParentAsinProfitReport(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有利润报表-父ASIN数据...');

      const allProfitReports = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有利润报表数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页利润报表-父ASIN（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getParentAsinProfitReport(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageProfitReports = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条利润报表-父ASIN数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allProfitReports.push(...pageProfitReports);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProfitReports.length} 条数据，累计 ${allProfitReports.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allProfitReports.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageProfitReports.length < actualPageSize || allProfitReports.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页利润报表-父ASIN失败:`, error.message);
          if (allProfitReports.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有利润报表-父ASIN数据获取完成，共 ${allProfitReports.length} 条数据`);

      // 保存到数据库
      if (allProfitReports && allProfitReports.length > 0) {
        await this.saveParentAsinProfitReports(accountId, allProfitReports);
      }

      return {
        profitReports: allProfitReports,
        total: allProfitReports.length,
        stats: {
          totalProfitReports: allProfitReports.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有利润报表-父ASIN数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表-SKU
   * API: POST /bd/profit/report/open/report/sku/list
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度，上限10000（可选，默认1000）
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - monthlyQuery: 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   - startDate: 开始时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - endDate: 结束时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - searchField: 搜索值类型，local_sku（可选，默认local_sku）
   *   - searchValue: 搜索的值数组（可选）
   *   - currencyCode: 币种code（可选）
   *   - summaryEnabled: 是否按sku汇总返回：false 默认值，true（可选）
   *   - orderStatus: 交易状态（可选）
   *     Deferred 已推迟
   *     Disbursed 已发放【默认】
   *     DisbursedAndPreSettled 已发放（含预结算）
   *     All 全部
   * @returns {Promise<Object>} 利润报表数据 { data: [], total: 0 }
   */
  async getSkuProfitReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate) {
        throw new Error('开始日期 (startDate) 是必填参数');
      }
      if (!params.endDate) {
        throw new Error('结束日期 (endDate) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 1000; // 默认值
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.monthlyQuery !== undefined) {
        requestParams.monthlyQuery = params.monthlyQuery === true || params.monthlyQuery === 'true';
      }
      if (params.searchField) {
        requestParams.searchField = params.searchField;
      }
      if (params.searchValue && Array.isArray(params.searchValue)) {
        requestParams.searchValue = params.searchValue;
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.summaryEnabled !== undefined) {
        requestParams.summaryEnabled = params.summaryEnabled === true || params.summaryEnabled === 'true';
      }
      if (params.orderStatus) {
        requestParams.orderStatus = params.orderStatus;
      }

      // 调用API获取利润报表-SKU（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/sku/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润报表-SKU失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;

      return {
        data: records,
        total: total
      };
    } catch (error) {
      console.error('获取利润报表-SKU失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有利润报表-SKU数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 利润报表查询参数（同 getSkuProfitReport，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { profitReports: [], total: 0, stats: {} }
   */
  async fetchAllSkuProfitReport(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有利润报表-SKU数据...');

      const allProfitReports = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有利润报表数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页利润报表-SKU（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getSkuProfitReport(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageProfitReports = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条利润报表-SKU数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allProfitReports.push(...pageProfitReports);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProfitReports.length} 条数据，累计 ${allProfitReports.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allProfitReports.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageProfitReports.length < actualPageSize || allProfitReports.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页利润报表-SKU失败:`, error.message);
          if (allProfitReports.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有利润报表-SKU数据获取完成，共 ${allProfitReports.length} 条数据`);

      // 保存到数据库
      if (allProfitReports && allProfitReports.length > 0) {
        await this.saveSkuProfitReports(accountId, allProfitReports);
      }

      return {
        profitReports: allProfitReports,
        total: allProfitReports.length,
        stats: {
          totalProfitReports: allProfitReports.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有利润报表-SKU数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表-店铺
   * API: POST /bd/profit/report/open/report/seller/list
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度，上限10000（可选，默认1000）
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - monthlyQuery: 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   - startDate: 开始时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - endDate: 结束时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - currencyCode: 币种code【默认原币种】（可选）
   *   - summaryEnabled: 是否按店铺汇总返回：false 默认值，true（可选）
   *   - orderStatus: 交易状态（可选）
   *     Deferred 已推迟
   *     Disbursed 已发放【默认】
   *     DisbursedAndPreSettled 已发放（含预结算）
   *     All 全部
   * @returns {Promise<Object>} 利润报表数据 { data: [], total: 0 }
   */
  async getSellerProfitReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate) {
        throw new Error('开始日期 (startDate) 是必填参数');
      }
      if (!params.endDate) {
        throw new Error('结束日期 (endDate) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 1000; // 默认值
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.monthlyQuery !== undefined) {
        requestParams.monthlyQuery = params.monthlyQuery === true || params.monthlyQuery === 'true';
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.summaryEnabled !== undefined) {
        requestParams.summaryEnabled = params.summaryEnabled === true || params.summaryEnabled === 'true';
      }
      if (params.orderStatus) {
        requestParams.orderStatus = params.orderStatus;
      }

      // 调用API获取利润报表-店铺（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/seller/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润报表-店铺失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;

      return {
        data: records,
        total: total
      };
    } catch (error) {
      console.error('获取利润报表-店铺失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有利润报表-店铺数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 利润报表查询参数（同 getSellerProfitReport，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { profitReports: [], total: 0, stats: {} }
   */
  async fetchAllSellerProfitReport(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有利润报表-店铺数据...');

      const allProfitReports = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有利润报表数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页利润报表-店铺（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getSellerProfitReport(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageProfitReports = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条利润报表-店铺数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allProfitReports.push(...pageProfitReports);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProfitReports.length} 条数据，累计 ${allProfitReports.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allProfitReports.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageProfitReports.length < actualPageSize || allProfitReports.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页利润报表-店铺失败:`, error.message);
          if (allProfitReports.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有利润报表-店铺数据获取完成，共 ${allProfitReports.length} 条数据`);

      // 保存到数据库
      if (allProfitReports && allProfitReports.length > 0) {
        await this.saveSellerProfitReports(accountId, allProfitReports);
      }

      return {
        profitReports: allProfitReports,
        total: allProfitReports.length,
        stats: {
          totalProfitReports: allProfitReports.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有利润报表-店铺数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表-店铺月度汇总
   * API: POST /bd/profit/report/open/report/seller/summary/list
   * 令牌桶容量: 10
   * 注意：该接口不支持分页，返回汇总数据数组
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - monthlyQuery: 是否按月查询：false 按天【默认值】，true 按月（可选）
   *   - startDate: 开始时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - endDate: 结束时间【结算时间，双闭区间】（必填）
   *     按天：开始结束时间间隔最长不能跨度 31 天，格式：Y-m-d
   *     按月：开始结束时间年月相同，格式：Y-m
   *   - currencyCode: 币种code（可选）
   *   - orderStatus: 交易状态（可选）
   *     Deferred 已推迟
   *     Disbursed 已发放【默认】
   *     DisbursedAndPreSettled 已发放（含预结算）
   *     All 全部
   * @returns {Promise<Array>} 利润报表汇总数据数组
   */
  async getSellerProfitSummary(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate) {
        throw new Error('开始日期 (startDate) 是必填参数');
      }
      if (!params.endDate) {
        throw new Error('结束日期 (endDate) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate
      };

      // 可选参数
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.monthlyQuery !== undefined) {
        requestParams.monthlyQuery = params.monthlyQuery === true || params.monthlyQuery === 'true';
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.orderStatus) {
        requestParams.orderStatus = params.orderStatus;
      }

      // 调用API获取利润报表-店铺月度汇总（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/seller/summary/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润报表-店铺月度汇总失败');
      }

      // 该接口返回的数据是数组，不是包含records的对象
      const summaryData = Array.isArray(response.data) ? response.data : (response.data?.records || []);

      return summaryData;
    } catch (error) {
      console.error('获取利润报表-店铺月度汇总失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询FBA成本计价流水
   * API: POST /cost/center/api/cost/stream
   * 令牌桶容量: 10
   * 支持查询成本中心FBA成本计价流水
   * 注意：由于亚马逊库存分类账生成数据后5天内会发生变更，领星在获取数据的5天内会继续获取数据并覆盖更新历史版本，因此接口获取的5天内数据是有可能发生变更的。
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认200条（可选）
   *   - wh_names: 仓库名数组（可选）
   *   - shop_names: 店铺名数组（可选）
   *   - skus: sku数组（可选）
   *   - mskus: msku数组（可选）
   *   - disposition_types: 库存属性数组：1 可用在途，2 可用，3 次品（可选）
   *   - business_types: 出入库类型数组（必填）
   *     1 期初库存-FBA上月结存
   *     10 调拨入库-FBA补货入库
   *     11 调拨入库-FBA途损补回
   *     12 调拨入库-FBA超签入库
   *     13 调拨入库-FBA超签入库（close后）
   *     14 调拨入库-FBA补货入库（无发货单）
   *     20 调拨入库-FBA调仓入库
   *     35 调拨入库-FBA发货在途入库
   *     25 盘点入库-FBA盘点入库
   *     30 FBA退货-FBA无源单销售退货
   *     31 FBA退货-FBA有源单销售退货
   *     200 销售出库-FBA补发货销售
   *     201 销售出库-FBA多渠道销售订单
   *     202 销售出库-FBA亚马逊销售订单
   *     205 其他出库-FBA补货出库
   *     220 盘点出库-FBA盘点出库
   *     15 调拨出库-FBA调仓出库
   *     215 调拨出库-FBA移除
   *     225 调拨出库-FBA发货在途出库
   *     226 调拨出库-FBA发货途损
   *     227 调拨出库-后补发货单在途出库
   *     5 调整单- FBA对账差异入库调整
   *     210 调整单-FBA对账差异出库调整
   *     400 调整单-尾差调整
   *     420 调整单-负库存数量调整
   *     405 调整单-期初成本录入
   *   - query_type: 日期查询类型（必填）
   *     01 库存动作日期【对应成本计价详情页面单据日期，即在FBA仓库内发生各项库存动作的日期】
   *     02 结算日期【仅销售、退货场景会存在结算日期，其他库存动作结算日期为空】
   *   - start_date: 起始日期，Y-m-d，不允许跨月（必填）
   *   - end_date: 结束日期，Y-m-d，不允许跨月（必填）
   *   - business_numbers: 业务编号数组（可选）
   *   - origin_accounts: 源头单据号数组（可选）
   * @returns {Promise<Object>} FBA成本计价流水数据 { data: [], total: 0, size: 0, current: 0 }
   */
  async getFbaCostStream(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.business_types || !Array.isArray(params.business_types) || params.business_types.length === 0) {
        throw new Error('出入库类型 (business_types) 是必填参数，且必须是非空数组');
      }
      if (!params.query_type) {
        throw new Error('日期查询类型 (query_type) 是必填参数，可选值：01 或 02');
      }
      if (!params.start_date) {
        throw new Error('起始日期 (start_date) 是必填参数，格式：Y-m-d');
      }
      if (!params.end_date) {
        throw new Error('结束日期 (end_date) 是必填参数，格式：Y-m-d');
      }

      // 构建请求参数
      const requestParams = {
        business_types: params.business_types.map(type => parseInt(type)),
        query_type: params.query_type,
        start_date: params.start_date,
        end_date: params.end_date
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 200; // 默认值
      }
      if (params.wh_names && Array.isArray(params.wh_names)) {
        requestParams.wh_names = params.wh_names;
      }
      if (params.shop_names && Array.isArray(params.shop_names)) {
        requestParams.shop_names = params.shop_names;
      }
      if (params.skus && Array.isArray(params.skus)) {
        requestParams.skus = params.skus;
      }
      if (params.mskus && Array.isArray(params.mskus)) {
        requestParams.mskus = params.mskus;
      }
      if (params.disposition_types && Array.isArray(params.disposition_types)) {
        requestParams.disposition_types = params.disposition_types.map(type => parseInt(type));
      }
      if (params.business_numbers && Array.isArray(params.business_numbers)) {
        requestParams.business_numbers = params.business_numbers;
      }
      if (params.origin_accounts && Array.isArray(params.origin_accounts)) {
        requestParams.origin_accounts = params.origin_accounts;
      }

      // 调用API获取FBA成本计价流水（令牌桶容量为10）
      const response = await this.post(
        account,
        '/cost/center/api/cost/stream',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取FBA成本计价流水失败');
      }

      const records = response.data?.records || [];
      const total = response.data?.total || 0;
      const size = response.data?.size || 0;
      const current = response.data?.current || 0;

      return {
        data: records,
        total: total,
        size: size,
        current: current
      };
    } catch (error) {
      console.error('获取FBA成本计价流水失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有FBA成本计价流水数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - FBA成本计价流水查询参数（同 getFbaCostStream，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { costStreams: [], total: 0, stats: {} }
   */
  async fetchAllFbaCostStream(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有FBA成本计价流水数据...');

      const allCostStreams = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有FBA成本计价流水数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页FBA成本计价流水（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getFbaCostStream(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageCostStreams = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条FBA成本计价流水数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allCostStreams.push(...pageCostStreams);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageCostStreams.length} 条数据，累计 ${allCostStreams.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allCostStreams.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageCostStreams.length < actualPageSize || allCostStreams.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页FBA成本计价流水失败:`, error.message);
          if (allCostStreams.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有FBA成本计价流水数据获取完成，共 ${allCostStreams.length} 条数据`);

      // 保存到数据库
      if (allCostStreams && allCostStreams.length > 0) {
        await this.saveFbaCostStreams(accountId, allCostStreams);
      }

      return {
        costStreams: allCostStreams,
        total: allCostStreams.length,
        stats: {
          totalCostStreams: allCostStreams.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有FBA成本计价流水数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询广告发票列表
   * API: POST /bd/profit/report/open/report/ads/invoice/list
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认值0（可选）
   *   - length: 分页大小，默认20（可选）
   *   - sids: 店铺id数组（可选）
   *   - mids: 国家id数组（可选）
   *   - ads_type: 广告类型数组（可选）
   *     SPONSORED PRODUCTS
   *     SPONSORED DISPLAY
   *     SPONSORED BRANDS
   *     SPONSORED BRANDS VIDEO
   *   - invoice_start_time: 开始时间【发票开具时间】（必填）
   *   - invoice_end_time: 结束时间【发票开具时间】（必填）
   *   - search_type: 搜索类型（可选）
   *     ads_campaign【对应页面广告活动】
   *     invoice_id【对应发票编号】
   *     msku
   *     asin
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 广告发票列表数据 { data: [], total: 0 }
   */
  async getAdsInvoiceList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.invoice_start_time) {
        throw new Error('开始时间 (invoice_start_time) 是必填参数');
      }
      if (!params.invoice_end_time) {
        throw new Error('结束时间 (invoice_end_time) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        invoice_start_time: params.invoice_start_time,
        invoice_end_time: params.invoice_end_time
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(sid => parseInt(sid));
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(mid => parseInt(mid));
      }
      if (params.ads_type && Array.isArray(params.ads_type)) {
        requestParams.ads_type = params.ads_type;
      }
      if (params.search_type) {
        requestParams.search_type = params.search_type;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取广告发票列表（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/ads/invoice/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取广告发票列表失败');
      }

      const invoiceList = response.data || [];
      const total = response.total || 0;

      return {
        data: invoiceList,
        total: total
      };
    } catch (error) {
      console.error('获取广告发票列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询广告发票活动列表
   * API: POST /bd/profit/report/open/report/ads/invoice/campaign/list
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认值0（可选）
   *   - length: 分页大小，默认20（可选）
   *   - invoice_id: 广告发票编号（必填）
   *   - sid: 店铺id（必填）
   *   - ads_type: 广告类型数组（可选）
   *     SPONSORED PRODUCTS
   *     SPONSORED DISPLAY
   *     SPONSORED BRANDS
   *     SPONSORED BRANDS VIDEO
   *   - search_type: 搜索类型（可选）
   *     ads_campaign【对应页面广告活动】
   *     item【对应页面承担商品】
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 广告发票活动列表数据 { data: [], total: 0 }
   */
  async getAdsInvoiceCampaignList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.invoice_id) {
        throw new Error('广告发票编号 (invoice_id) 是必填参数');
      }
      if (params.sid === undefined) {
        throw new Error('店铺id (sid) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        invoice_id: params.invoice_id,
        sid: parseInt(params.sid)
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.ads_type && Array.isArray(params.ads_type)) {
        requestParams.ads_type = params.ads_type;
      }
      if (params.search_type) {
        requestParams.search_type = params.search_type;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取广告发票活动列表（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/ads/invoice/campaign/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取广告发票活动列表失败');
      }

      const campaignList = response.data || [];
      const total = response.total || 0;

      return {
        data: campaignList,
        total: total
      };
    } catch (error) {
      console.error('获取广告发票活动列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询广告发票基本信息
   * API: POST /bd/profit/report/open/report/ads/invoice/detail
   * 令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - invoice_id: 广告发票编号（必填）
   *   - sid: 店铺id（必填）
   * @returns {Promise<Object>} 广告发票基本信息
   */
  async getAdsInvoiceDetail(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.invoice_id) {
        throw new Error('广告发票编号 (invoice_id) 是必填参数');
      }
      if (params.sid === undefined) {
        throw new Error('店铺id (sid) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        invoice_id: params.invoice_id,
        sid: parseInt(params.sid)
      };

      // 调用API获取广告发票基本信息（令牌桶容量为10）
      const response = await this.post(
        account,
        '/bd/profit/report/open/report/ads/invoice/detail',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取广告发票基本信息失败');
      }

      const invoiceDetail = response.data || {};

      return invoiceDetail;
    } catch (error) {
      console.error('获取广告发票基本信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有广告发票列表数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 广告发票列表查询参数（同 getAdsInvoiceList，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20，上限10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { invoices: [], total: 0, stats: {} }
   */
  async fetchAllAdsInvoices(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有广告发票列表数据...');

      const allInvoices = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 10000); // 最大10000
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有广告发票数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页广告发票（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getAdsInvoiceList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageInvoices = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条广告发票数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
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
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页广告发票失败:`, error.message);
          if (allInvoices.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有广告发票列表数据获取完成，共 ${allInvoices.length} 条数据`);

      // 保存到数据库
      if (allInvoices && allInvoices.length > 0) {
        await this.saveAdsInvoices(accountId, allInvoices);
      }

      return {
        invoices: allInvoices,
        total: allInvoices.length,
        stats: {
          totalInvoices: allInvoices.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有广告发票列表数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款单列表
   * API: POST /basicOpen/finance/requestFunds/order/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20，上限200（可选）
   *   - status: 状态（可选）
   *     1 待付款
   *     2 已完成
   *     3 已作废
   *     121 待审批
   *     122 已驳回
   *     124 已作废
   *   - search_field_time: 搜索时间类型（可选）
   *     apply_time 申请时间
   *     real_pay_time 实际付款时间
   *     prepay_time 预计付款时间
   *   - start_date: 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - end_date: 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - search_field: 搜索字段（可选）
   *     purchase_order_sn 关联单据
   *     order_sn 请款单号
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 请款单列表数据 { data: [], total: 0 }
   */
  async getRequestFundsOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {};

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.status !== undefined) {
        requestParams.status = parseInt(params.status);
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
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取请款单列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFunds/order/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款单列表失败');
      }

      const orderList = response.data || [];
      const total = response.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取请款单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款单列表数据（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 请款单列表查询参数（同 getRequestFundsOrderList，但不需要 offset 和 length）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20，上限200）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { requestFundsOrders: [], total: 0, stats: {} }
   */
  async fetchAllRequestFundsOrders(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款单列表数据...');

      const allRequestFundsOrders = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200); // 最大200
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      // 自动分页获取所有请款单数据
      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款单（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsOrderList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条请款单数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allRequestFundsOrders.push(...pageOrders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageOrders.length} 条数据，累计 ${allRequestFundsOrders.length} 条数据`);

          // 调用进度回调
          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allRequestFundsOrders.length, totalCount);
          }

          // 判断是否还有更多数据
          if (pageOrders.length < actualPageSize || allRequestFundsOrders.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            // 分页之间延迟，避免请求过快
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款单失败:`, error.message);
          if (allRequestFundsOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款单列表数据获取完成，共 ${allRequestFundsOrders.length} 条数据`);

      // 保存到数据库
      if (allRequestFundsOrders && allRequestFundsOrders.length > 0) {
        await this.saveRequestFundsOrders(accountId, allRequestFundsOrders);
      }

      return {
        requestFundsOrders: allRequestFundsOrders,
        total: allRequestFundsOrders.length,
        stats: {
          totalRequestFundsOrders: allRequestFundsOrders.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款单列表数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款池 - 货款现结
   * API: POST /basicOpen/finance/requestFundsPool/purchase/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20，上限200（可选）
   *   - pay_status: 支付状态【多个使用英文逗号分隔】（可选）
   *     0 未申请
   *     1 已申请
   *     2 部分付款
   *     3 已付清
   *   - time_field: 时间搜索类型：create_time 创建时间（可选）
   *   - start_time: 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - end_time: 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - search_field: 搜索类型：sku SKU，order_sn 采购单号（可选）
   *   - search_value: 查询值（可选）
   * @returns {Promise<Object>} 请款池-货款现结数据 { data: [], total: 0 }
   */
  async getRequestFundsPoolPurchaseList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {};

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.pay_status !== undefined) {
        requestParams.pay_status = params.pay_status; // 字符串，多个用逗号分隔
      }
      if (params.time_field) {
        requestParams.time_field = params.time_field;
      }
      if (params.start_time) {
        requestParams.start_time = params.start_time;
      }
      if (params.end_time) {
        requestParams.end_time = params.end_time;
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取请款池-货款现结列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFundsPool/purchase/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款池-货款现结列表失败');
      }

      const orderList = response.data || [];
      const total = response.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取请款池-货款现结列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款池 - 货款月结
   * API: POST /basicOpen/finance/requestFundsPool/inbound/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20，上限200（可选）
   *   - pay_status: 状态：0 未申请，10 已申请，20 已付清（可选）
   *   - time_field: 时间搜索类型：create_time 入库时间，prepay_time 应付款日（可选）
   *   - start_time: 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - end_time: 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - search_field: 搜索类型：order_sn 入库单号，purchase_order_sn 采购单号，sku SKU（可选）
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 请款池-货款月结数据 { data: [], total: 0 }
   */
  async getRequestFundsPoolInboundList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {};

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.pay_status !== undefined) {
        requestParams.pay_status = params.pay_status; // 字符串或数字
      }
      if (params.time_field) {
        requestParams.time_field = params.time_field;
      }
      if (params.start_time) {
        requestParams.start_time = params.start_time;
      }
      if (params.end_time) {
        requestParams.end_time = params.end_time;
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取请款池-货款月结列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFundsPool/inbound/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款池-货款月结列表失败');
      }

      const orderList = response.data || [];
      const total = response.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取请款池-货款月结列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款池 - 货款预付款
   * API: POST /basicOpen/finance/requestFundsPool/prepay/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20，上限200（可选）
   *   - pay_status: 支付状态：0 未申请，1 已申请，2 部分付款，3 已付清（可选）
   *   - start_time: 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - end_time: 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - time_field: 时间搜索类型：create_time 创建时间（可选）
   *   - search_field: 搜索类型：purchase_order_sn 采购单号，order_sn 预付款单号（可选）
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 请款池-货款预付款数据 { data: [], total: 0 }
   */
  async getRequestFundsPoolPrepayList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {};

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.pay_status !== undefined) {
        requestParams.pay_status = params.pay_status; // 字符串或数字
      }
      if (params.start_time) {
        requestParams.start_time = params.start_time;
      }
      if (params.end_time) {
        requestParams.end_time = params.end_time;
      }
      if (params.time_field) {
        requestParams.time_field = params.time_field;
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取请款池-货款预付款列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFundsPool/prepay/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款池-货款预付款列表失败');
      }

      const orderList = response.data || [];
      const total = response.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取请款池-货款预付款列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款池-物流请款
   * API: POST /basicOpen/finance/requestFundsPool/logistics/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20，上限200（可选）
   *   - search_field_time: 时间搜索类型（可选）
   *     create_time 费用录入时间
   *     delivery_create_time 发货单创建时间
   *     shipment_time 发货时间
   *     close_time 付清时间
   *   - start_time: 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - end_time: 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - search_field: 搜索类型：order_sn 发货单号，logistics_center_code 物流中心编码（可选）
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 请款池-物流请款数据 { data: [], total: 0 }
   */
  async getRequestFundsPoolLogisticsList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {};

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.search_field_time) {
        requestParams.search_field_time = params.search_field_time;
      }
      if (params.start_time) {
        requestParams.start_time = params.start_time;
      }
      if (params.end_time) {
        requestParams.end_time = params.end_time;
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取请款池-物流请款列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFundsPool/logistics/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款池-物流请款列表失败');
      }

      const orderList = response.data || [];
      const total = response.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取请款池-物流请款列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款池-其他应付款
   * API: POST /basicOpen/finance/requestFundsPool/customFee/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20，上限200（可选）
   *   - pay_status: 支付状态【多个状态用英文逗号分隔】（可选）
   *     0 未申请
   *     1 已申请
   *     2 部分付款
   *     3 已付清
   *   - search_field_time: 时间搜索类型：create_time 创建时间，close_time 付清时间（可选）
   *   - start_time: 开始时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - end_time: 结束时间【时间间隔最长不得超过90天】，闭区间，格式：Y-m-d（可选）
   *   - search_field: 搜索类型：business_sn 费用单号，custom_fee_sn 其他应付单号（可选）
   *   - search_value: 搜索值（可选）
   * @returns {Promise<Object>} 请款池-其他应付款数据 { data: [], total: 0 }
   */
  async getRequestFundsPoolCustomFeeList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {};

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.pay_status !== undefined) {
        requestParams.pay_status = params.pay_status; // 字符串，多个用逗号分隔
      }
      if (params.search_field_time) {
        requestParams.search_field_time = params.search_field_time;
      }
      if (params.start_time) {
        requestParams.start_time = params.start_time;
      }
      if (params.end_time) {
        requestParams.end_time = params.end_time;
      }
      if (params.search_field) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value) {
        requestParams.search_value = params.search_value;
      }

      // 调用API获取请款池-其他应付款列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFundsPool/customFee/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款池-其他应付款列表失败');
      }

      const orderList = response.data || [];
      const total = response.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取请款池-其他应付款列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询请款池-其他费用
   * API: POST /basicOpen/finance/requestFundsPool/otherFee/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选）
   *   - length: 分页长度（可选）
   *   - startTime: 开始时间，必填，格式：yyyy-MM-dd（必填）
   *   - endTime: 结束时间，必填，格式：yyyy-MM-dd（必填）
   *   - searchFieldTime: 时间维度，枚举值：create_time-创建时间, close_time-付清时间，默认create_time（可选）
   *   - searchField: 搜索字段，枚举值：order_sn-采购单号, create_username-采购员（可选）
   *   - searchValue: 搜索值，根据searchField字段进行搜索，支持模糊查询（可选）
   *   - status: 付款状态，枚举值：0-查询未付清, 1-查询已付清，不传默认查询全部（可选）
   *   - purchaserIds: 采购方ID列表，筛选指定采购方的其他费用（可选）
   *   - supplierIds: 应付对象ID列表，筛选指定供应商的其他费用（可选）
   * @returns {Promise<Object>} 请款池-其他费用数据 { data: { list: [], total: 0 }, total: 0 }
   */
  async getRequestFundsPoolOtherFeeList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startTime) {
        throw new Error('开始时间 (startTime) 是必填参数，格式：yyyy-MM-dd');
      }
      if (!params.endTime) {
        throw new Error('结束时间 (endTime) 是必填参数，格式：yyyy-MM-dd');
      }

      // 构建请求参数
      const requestParams = {
        startTime: params.startTime,
        endTime: params.endTime
      };

      // 可选参数
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }
      if (params.searchFieldTime) {
        requestParams.searchFieldTime = params.searchFieldTime;
      }
      if (params.searchField) {
        requestParams.searchField = params.searchField;
      }
      if (params.searchValue) {
        requestParams.searchValue = params.searchValue;
      }
      if (params.status !== undefined) {
        requestParams.status = parseInt(params.status);
      }
      if (params.purchaserIds && Array.isArray(params.purchaserIds)) {
        requestParams.purchaserIds = params.purchaserIds.map(id => parseInt(id));
      }
      if (params.supplierIds && Array.isArray(params.supplierIds)) {
        requestParams.supplierIds = params.supplierIds.map(id => parseInt(id));
      }

      // 调用API获取请款池-其他费用列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/basicOpen/finance/requestFundsPool/otherFee/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取请款池-其他费用列表失败');
      }

      // 该接口返回的数据结构是 { data: { list: [], total: 0 }, total: 0 }
      const list = response.data?.list || [];
      const dataTotal = response.data?.total || 0;
      const total = response.total || dataTotal;

      return {
        data: list,
        total: total
      };
    } catch (error) {
      console.error('获取请款池-其他费用列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款池-货款现结数据（自动处理分页）
   */
  async fetchAllRequestFundsPoolPurchase(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款池-货款现结数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200); // 最大200
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款池-货款现结（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsPoolPurchaseList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款池-货款现结失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款池-货款现结数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveRequestFundsPoolPurchases(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款池-货款现结数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款池-货款月结数据（自动处理分页）
   */
  async fetchAllRequestFundsPoolInbound(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款池-货款月结数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款池-货款月结（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsPoolInboundList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款池-货款月结失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款池-货款月结数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveRequestFundsPoolInbounds(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款池-货款月结数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款池-货款预付款数据（自动处理分页）
   */
  async fetchAllRequestFundsPoolPrepay(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款池-货款预付款数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款池-货款预付款（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsPoolPrepayList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款池-货款预付款失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款池-货款预付款数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveRequestFundsPoolPrepays(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款池-货款预付款数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款池-物流请款数据（自动处理分页）
   */
  async fetchAllRequestFundsPoolLogistics(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款池-物流请款数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款池-物流请款（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsPoolLogisticsList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款池-物流请款失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款池-物流请款数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveRequestFundsPoolLogistics(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款池-物流请款数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款池-其他应付款数据（自动处理分页）
   */
  async fetchAllRequestFundsPoolCustomFee(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款池-其他应付款数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款池-其他应付款（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsPoolCustomFeeList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款池-其他应付款失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款池-其他应付款数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveRequestFundsPoolCustomFees(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款池-其他应付款数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有请款池-其他费用数据（自动处理分页）
   */
  async fetchAllRequestFundsPoolOtherFee(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有请款池-其他费用数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页请款池-其他费用（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getRequestFundsPoolOtherFeeList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页请款池-其他费用失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有请款池-其他费用数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveRequestFundsPoolOtherFees(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有请款池-其他费用数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 应收报告-列表查询
   * API: POST /bd/sp/api/open/monthly/receivable/report/list
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - settleMonth: 结算月，格式：Y-m（必填）
   *   - sids: 店铺id列表（可选）
   *   - mids: 国家id列表（可选）
   *   - currencyCode: 币种code（可选）
   *   - archiveStatus: 对账状态：1 已对账，0 未对账（可选）
   *   - sortField: 排序字段（可选）
   *   - sortType: 排序规则：asc 升序，desc 降序（可选）
   *   - receivedState: 转账/到账金额：0 不相符，1 相符（可选）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认20（可选）
   * @returns {Promise<Object>} 应收报告列表数据 { data: [], total: 0 }
   */
  async getReceivableReportList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.settleMonth) {
        throw new Error('结算月 (settleMonth) 是必填参数，格式：Y-m');
      }

      // 构建请求参数
      const requestParams = {
        settleMonth: params.settleMonth
      };

      // 可选参数
      if (params.sids && Array.isArray(params.sids)) {
        requestParams.sids = params.sids.map(id => parseInt(id));
      }
      if (params.mids && Array.isArray(params.mids)) {
        requestParams.mids = params.mids.map(id => parseInt(id));
      }
      if (params.currencyCode) {
        requestParams.currencyCode = params.currencyCode;
      }
      if (params.archiveStatus !== undefined) {
        requestParams.archiveStatus = parseInt(params.archiveStatus);
      }
      if (params.sortField) {
        requestParams.sortField = params.sortField;
      }
      if (params.sortType) {
        requestParams.sortType = params.sortType;
      }
      if (params.receivedState !== undefined) {
        requestParams.receivedState = parseInt(params.receivedState);
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }

      // 调用API获取应收报告列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/bd/sp/api/open/monthly/receivable/report/list',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取应收报告列表失败');
      }

      const reportList = response.data || [];
      const total = response.total || 0;

      return {
        data: reportList,
        total: total
      };
    } catch (error) {
      console.error('获取应收报告列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 应收报告-详情-列表
   * API: POST /bd/sp/api/open/monthly/receivable/report/list/detail
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id（必填）
   *   - currencyCode: 币种code（必填）
   *   - settleMonth: 结算月（必填）
   *   - searchField: 搜索值类型（可选）
   *   - searchValue: 搜索值（可选）
   *   - offset: 偏移量（可选）
   *   - length: 分页长度，默认20（可选）
   * @returns {Promise<Object>} 应收报告详情列表数据 { data: [], total: 0 }
   */
  async getReceivableReportDetailList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.sid === undefined) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.currencyCode) {
        throw new Error('币种code (currencyCode) 是必填参数');
      }
      if (!params.settleMonth) {
        throw new Error('结算月 (settleMonth) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        currencyCode: params.currencyCode,
        settleMonth: params.settleMonth
      };

      // 可选参数
      if (params.searchField) {
        requestParams.searchField = params.searchField;
      }
      if (params.searchValue) {
        requestParams.searchValue = params.searchValue;
      }
      if (params.offset !== undefined) {
        requestParams.offset = parseInt(params.offset);
      }
      if (params.length !== undefined) {
        requestParams.length = parseInt(params.length);
      } else {
        requestParams.length = 20; // 默认值
      }

      // 调用API获取应收报告详情列表（令牌桶容量为1）
      const response = await this.post(
        account,
        '/bd/sp/api/open/monthly/receivable/report/list/detail',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取应收报告详情列表失败');
      }

      const detailList = response.data || [];
      const total = response.total || 0;

      return {
        data: detailList,
        total: total
      };
    } catch (error) {
      console.error('获取应收报告详情列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 应收报告-详情-基础信息
   * API: POST /bd/sp/api/open/monthly/receivable/report/list/detail/info
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id（必填）
   *   - currencyCode: 币种code（必填）
   *   - settleMonth: 结算月（必填）
   * @returns {Promise<Object>} 应收报告详情基础信息 { data: {} }
   */
  async getReceivableReportDetailInfo(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.sid === undefined) {
        throw new Error('店铺id (sid) 是必填参数');
      }
      if (!params.currencyCode) {
        throw new Error('币种code (currencyCode) 是必填参数');
      }
      if (!params.settleMonth) {
        throw new Error('结算月 (settleMonth) 是必填参数');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        currencyCode: params.currencyCode,
        settleMonth: params.settleMonth
      };

      // 调用API获取应收报告详情基础信息（令牌桶容量为1）
      const response = await this.post(
        account,
        '/bd/sp/api/open/monthly/receivable/report/list/detail/info',
        requestParams,
        {
          successCode: [0, 200, '200']
        }
      );

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取应收报告详情基础信息失败');
      }

      const detailInfo = response.data || {};

      return {
        data: detailInfo
      };
    } catch (error) {
      console.error('获取应收报告详情基础信息失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有应收报告列表数据（自动处理分页）
   */
  async fetchAllReceivableReportList(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有应收报告列表数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页应收报告列表（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getReceivableReportList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页应收报告列表失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有应收报告列表数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveReceivableReports(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有应收报告列表数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有应收报告详情列表数据（自动处理分页）
   */
  async fetchAllReceivableReportDetailList(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有应收报告详情列表数据...');

      const allData = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 200);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页应收报告详情列表（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getReceivableReportDetailList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageData = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条数据，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allData.push(...pageData);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageData.length} 条数据，累计 ${allData.length} 条数据`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allData.length, totalCount);
          }

          if (pageData.length < actualPageSize || allData.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页应收报告详情列表失败:`, error.message);
          if (allData.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有应收报告详情列表数据获取完成，共 ${allData.length} 条数据`);

      // 保存到数据库
      if (allData && allData.length > 0) {
        await this.saveReceivableReportDetails(accountId, allData);
      }

      return {
        data: allData,
        total: allData.length,
        stats: {
          totalRecords: allData.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有应收报告详情列表数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存费用类型列表到数据库
   */
  async saveFeeTypes(accountId, feeTypes) {
    try {
      if (!prisma.lingXingFeeType) {
        console.error('Prisma Client 中未找到 lingXingFeeType 模型');
        return;
      }

      for (const feeType of feeTypes) {
        // 将 feeTypeId 转换为字符串（schema 中定义为 String）
        const feeTypeId = feeType.id || feeType.fee_type_id;
        const feeTypeIdStr = feeTypeId !== undefined && feeTypeId !== null ? String(feeTypeId) : '';

        await prisma.lingXingFeeType.upsert({
          where: {
            accountId_feeTypeId: {
              accountId: accountId,
              feeTypeId: feeTypeIdStr
            }
          },
          update: {
            feeTypeName: feeType.name || feeType.fee_type_name || null,
            data: feeType,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            feeTypeId: feeTypeIdStr,
            feeTypeName: feeType.name || feeType.fee_type_name || null,
            data: feeType
          }
        });
      }

      console.log(`费用类型列表已保存到数据库: 共 ${feeTypes.length} 个费用类型`);
    } catch (error) {
      console.error('保存费用类型列表到数据库失败:', error.message);
    }
  }

  /**
   * 保存费用明细列表到数据库
   */
  async saveFeeDetails(accountId, feeDetails) {
    try {
      if (!prisma.lingXingFeeDetail) {
        console.error('Prisma Client 中未找到 lingXingFeeDetail 模型');
        return;
      }

      for (const feeDetail of feeDetails) {
        await prisma.lingXingFeeDetail.create({
          data: {
            accountId: accountId,
            feeDetailId: feeDetail.id || feeDetail.business_sn || null,
            data: feeDetail
          }
        });
      }

      console.log(`费用明细列表已保存到数据库: 共 ${feeDetails.length} 条费用明细`);
    } catch (error) {
      console.error('保存费用明细列表到数据库失败:', error.message);
    }
  }

  /**
   * 保存MSKU利润报表到数据库
   */
  async saveMskuProfitReports(accountId, reports) {
    try {
      if (!prisma.lingXingMskuProfitReport) {
        console.error('Prisma Client 中未找到 lingXingMskuProfitReport 模型');
        return;
      }

      for (const report of reports) {
        await prisma.lingXingMskuProfitReport.create({
          data: {
            accountId: accountId,
            msku: report.msku || report.seller_sku || null,
            data: report
          }
        });
      }

      console.log(`MSKU利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存MSKU利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存ASIN利润报表到数据库
   */
  async saveAsinProfitReports(accountId, reports) {
    try {
      if (!prisma.lingXingAsinProfitReport) {
        console.error('Prisma Client 中未找到 lingXingAsinProfitReport 模型');
        return;
      }

      for (const report of reports) {
        await prisma.lingXingAsinProfitReport.create({
          data: {
            accountId: accountId,
            asin: report.asin || null,
            data: report
          }
        });
      }

      console.log(`ASIN利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存ASIN利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存父ASIN利润报表到数据库
   */
  async saveParentAsinProfitReports(accountId, reports) {
    try {
      if (!prisma.lingXingParentAsinProfitReport) {
        console.error('Prisma Client 中未找到 lingXingParentAsinProfitReport 模型');
        return;
      }

      for (const report of reports) {
        await prisma.lingXingParentAsinProfitReport.create({
          data: {
            accountId: accountId,
            parentAsin: report.parent_asin || null,
            data: report
          }
        });
      }

      console.log(`父ASIN利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存父ASIN利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存SKU利润报表到数据库
   */
  async saveSkuProfitReports(accountId, reports) {
    try {
      if (!prisma.lingXingSkuProfitReport) {
        console.error('Prisma Client 中未找到 lingXingSkuProfitReport 模型');
        return;
      }

      for (const report of reports) {
        await prisma.lingXingSkuProfitReport.create({
          data: {
            accountId: accountId,
            sku: report.sku || report.local_sku || null,
            data: report
          }
        });
      }

      console.log(`SKU利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存SKU利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存店铺利润报表到数据库
   */
  async saveSellerProfitReports(accountId, reports) {
    try {
      if (!prisma.lingXingSellerProfitReport) {
        console.error('Prisma Client 中未找到 lingXingSellerProfitReport 模型');
        return;
      }

      for (const report of reports) {
        await prisma.lingXingSellerProfitReport.create({
          data: {
            accountId: accountId,
            sid: report.sid ? parseInt(report.sid) : null,
            data: report
          }
        });
      }

      console.log(`店铺利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存店铺利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存FBA成本流到数据库
   */
  async saveFbaCostStreams(accountId, streams) {
    try {
      if (!prisma.lingXingFbaCostStream) {
        console.error('Prisma Client 中未找到 lingXingFbaCostStream 模型');
        return;
      }

      for (const stream of streams) {
        await prisma.lingXingFbaCostStream.create({
          data: {
            accountId: accountId,
            data: stream
          }
        });
      }

      console.log(`FBA成本流已保存到数据库: 共 ${streams.length} 条记录`);
    } catch (error) {
      console.error('保存FBA成本流到数据库失败:', error.message);
    }
  }

  /**
   * 保存广告发票到数据库
   */
  async saveAdsInvoices(accountId, invoices) {
    try {
      if (!prisma.lingXingAdsInvoice) {
        console.error('Prisma Client 中未找到 lingXingAdsInvoice 模型');
        return;
      }

      for (const invoice of invoices) {
        await prisma.lingXingAdsInvoice.create({
          data: {
            accountId: accountId,
            invoiceId: invoice.invoice_id || invoice.id || null,
            data: invoice
          }
        });
      }

      console.log(`广告发票已保存到数据库: 共 ${invoices.length} 条记录`);
    } catch (error) {
      console.error('保存广告发票到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款单到数据库
   */
  async saveRequestFundsOrders(accountId, orders) {
    try {
      if (!prisma.lingXingRequestFundsOrder) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsOrder 模型');
        return;
      }

      for (const order of orders) {
        if (!order.order_sn) continue;

        await prisma.lingXingRequestFundsOrder.upsert({
          where: {
            accountId_orderSn: {
              accountId: accountId,
              orderSn: order.order_sn
            }
          },
          update: {
            data: order,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: order.order_sn,
            data: order
          }
        });
      }

      console.log(`请款单已保存到数据库: 共 ${orders.length} 条记录`);
    } catch (error) {
      console.error('保存请款单到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款池-货款现结到数据库
   */
  async saveRequestFundsPoolPurchases(accountId, purchases) {
    try {
      if (!prisma.lingXingRequestFundsPoolPurchase) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsPoolPurchase 模型');
        return;
      }

      for (const purchase of purchases) {
        if (!purchase.order_sn) continue;

        await prisma.lingXingRequestFundsPoolPurchase.upsert({
          where: {
            accountId_purchase_orderSn: {
              accountId: accountId,
              orderSn: purchase.order_sn
            }
          },
          update: {
            data: purchase,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: purchase.order_sn,
            data: purchase
          }
        });
      }

      console.log(`请款池-货款现结已保存到数据库: 共 ${purchases.length} 条记录`);
    } catch (error) {
      console.error('保存请款池-货款现结到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款池-货款月结到数据库
   */
  async saveRequestFundsPoolInbounds(accountId, inbounds) {
    try {
      if (!prisma.lingXingRequestFundsPoolInbound) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsPoolInbound 模型');
        return;
      }

      for (const inbound of inbounds) {
        if (!inbound.order_sn) continue;

        await prisma.lingXingRequestFundsPoolInbound.upsert({
          where: {
            accountId_inbound_orderSn: {
              accountId: accountId,
              orderSn: inbound.order_sn
            }
          },
          update: {
            data: inbound,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: inbound.order_sn,
            data: inbound
          }
        });
      }

      console.log(`请款池-货款月结已保存到数据库: 共 ${inbounds.length} 条记录`);
    } catch (error) {
      console.error('保存请款池-货款月结到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款池-货款预付款到数据库
   */
  async saveRequestFundsPoolPrepays(accountId, prepays) {
    try {
      if (!prisma.lingXingRequestFundsPoolPrepay) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsPoolPrepay 模型');
        return;
      }

      for (const prepay of prepays) {
        if (!prepay.order_sn) continue;

        await prisma.lingXingRequestFundsPoolPrepay.upsert({
          where: {
            accountId_prepay_orderSn: {
              accountId: accountId,
              orderSn: prepay.order_sn
            }
          },
          update: {
            data: prepay,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: prepay.order_sn,
            data: prepay
          }
        });
      }

      console.log(`请款池-货款预付款已保存到数据库: 共 ${prepays.length} 条记录`);
    } catch (error) {
      console.error('保存请款池-货款预付款到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款池-物流请款到数据库
   */
  async saveRequestFundsPoolLogistics(accountId, logistics) {
    try {
      if (!prisma.lingXingRequestFundsPoolLogistics) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsPoolLogistics 模型');
        return;
      }

      for (const logistic of logistics) {
        if (!logistic.delivery_order_sn) continue;

        await prisma.lingXingRequestFundsPoolLogistics.upsert({
          where: {
            accountId_logistics_deliveryOrderSn: {
              accountId: accountId,
              deliveryOrderSn: logistic.delivery_order_sn
            }
          },
          update: {
            data: logistic,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            deliveryOrderSn: logistic.delivery_order_sn,
            data: logistic
          }
        });
      }

      console.log(`请款池-物流请款已保存到数据库: 共 ${logistics.length} 条记录`);
    } catch (error) {
      console.error('保存请款池-物流请款到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款池-其他应付款到数据库
   */
  async saveRequestFundsPoolCustomFees(accountId, customFees) {
    try {
      if (!prisma.lingXingRequestFundsPoolCustomFee) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsPoolCustomFee 模型');
        return;
      }

      for (const customFee of customFees) {
        if (!customFee.custom_fee_sn) continue;

        await prisma.lingXingRequestFundsPoolCustomFee.upsert({
          where: {
            accountId_customFeeSn: {
              accountId: accountId,
              customFeeSn: customFee.custom_fee_sn
            }
          },
          update: {
            data: customFee,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            customFeeSn: customFee.custom_fee_sn,
            data: customFee
          }
        });
      }

      console.log(`请款池-其他应付款已保存到数据库: 共 ${customFees.length} 条记录`);
    } catch (error) {
      console.error('保存请款池-其他应付款到数据库失败:', error.message);
    }
  }

  /**
   * 保存请款池-其他费用到数据库
   */
  async saveRequestFundsPoolOtherFees(accountId, otherFees) {
    try {
      if (!prisma.lingXingRequestFundsPoolOtherFee) {
        console.error('Prisma Client 中未找到 lingXingRequestFundsPoolOtherFee 模型');
        return;
      }

      for (const otherFee of otherFees) {
        await prisma.lingXingRequestFundsPoolOtherFee.create({
          data: {
            accountId: accountId,
            orderSn: otherFee.orderSn || otherFee.order_sn || null,
            data: otherFee
          }
        });
      }

      console.log(`请款池-其他费用已保存到数据库: 共 ${otherFees.length} 条记录`);
    } catch (error) {
      console.error('保存请款池-其他费用到数据库失败:', error.message);
    }
  }

  /**
   * 保存应收报告到数据库
   */
  async saveReceivableReports(accountId, reports) {
    try {
      if (!prisma.lingXingReceivableReport) {
        console.error('Prisma Client 中未找到 lingXingReceivableReport 模型');
        return;
      }

      for (const report of reports) {
        if (!report.sid || !report.settlementDate) continue;

        await prisma.lingXingReceivableReport.upsert({
          where: {
            accountId_sid_settleMonth: {
              accountId: accountId,
              sid: parseInt(report.sid),
              settleMonth: report.settlementDate || report.settleMonth
            }
          },
          update: {
            data: report,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            sid: parseInt(report.sid),
            settleMonth: report.settlementDate || report.settleMonth,
            data: report
          }
        });
      }

      console.log(`应收报告已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存应收报告到数据库失败:', error.message);
    }
  }

  /**
   * 保存应收报告详情到数据库
   */
  async saveReceivableReportDetails(accountId, details) {
    try {
      if (!prisma.lingXingReceivableReportDetail) {
        console.error('Prisma Client 中未找到 lingXingReceivableReportDetail 模型');
        return;
      }

      for (const detail of details) {
        if (!detail.fid) continue;

        await prisma.lingXingReceivableReportDetail.create({
          data: {
            accountId: accountId,
            sid: detail.sid ? parseInt(detail.sid) : null,
            settleMonth: detail.settleMonth || null,
            fid: detail.fid,
            data: detail
          }
        });
      }

      console.log(`应收报告详情已保存到数据库: 共 ${details.length} 条记录`);
    } catch (error) {
      console.error('保存应收报告详情到数据库失败:', error.message);
    }
  }
}

export default new LingXingFinanceService();

