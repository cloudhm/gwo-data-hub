import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';
import { runAccountLevelIncrementalSync } from '../sync/lingXingIncrementalRunner.js';
import lingXingSyncStateService from '../sync/lingXingSyncStateService.js';

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
   * 按天拉取利润报表-ASIN并按天覆盖保存（startDate=endDate 同一天，多天拆分，默认回退90天）
   */
  async fetchAllAsinProfitReportsByDayRange(accountId, listParams = {}, options = {}) {
    const { pageSize = 1000, delayBetweenPages = 500, onProgress = null } = options;
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    let totalRecords = 0;
    const baseParams = { ...listParams };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;
    for (let i = 0; i < days.length; i++) {
      const day = days[i];
      const allRecords = [];
      let offset = 0;
      let totalCount = 0;
      let hasMore = true;
      const actualPageSize = Math.min(pageSize, 10000);
      while (hasMore) {
        const res = await this.getAsinProfitReport(accountId, { ...baseParams, startDate: day, endDate: day, offset, length: actualPageSize });
        const pageRecords = res.data || [];
        const total = res.total ?? pageRecords.length;
        if (offset === 0) totalCount = total;
        allRecords.push(...pageRecords);
        if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
        else { offset += actualPageSize; if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages)); }
      }
      if (allRecords.length > 0) {
        await this.saveAsinProfitReportsForDay(accountId, allRecords, day);
        totalRecords += allRecords.length;
      }
      if (onProgress) onProgress(i + 1, days.length, day, totalRecords);
    }
    return { total: totalRecords, data: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
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
   * 按天拉取利润报表-父ASIN并按天覆盖保存（startDate=endDate 同一天，多天拆分，默认回退90天）
   */
  async fetchAllParentAsinProfitReportsByDayRange(accountId, listParams = {}, options = {}) {
    const { pageSize = 1000, delayBetweenPages = 500, onProgress = null } = options;
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    let totalRecords = 0;
    const baseParams = { ...listParams };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;
    for (let i = 0; i < days.length; i++) {
      const day = days[i];
      const allRecords = [];
      let offset = 0;
      let totalCount = 0;
      let hasMore = true;
      const actualPageSize = Math.min(pageSize, 10000);
      while (hasMore) {
        const res = await this.getParentAsinProfitReport(accountId, { ...baseParams, startDate: day, endDate: day, offset, length: actualPageSize });
        const pageRecords = res.data || [];
        const total = res.total ?? pageRecords.length;
        if (offset === 0) totalCount = total;
        allRecords.push(...pageRecords);
        if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
        else { offset += actualPageSize; if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages)); }
      }
      if (allRecords.length > 0) {
        await this.saveParentAsinProfitReportsForDay(accountId, allRecords, day);
        totalRecords += allRecords.length;
      }
      if (onProgress) onProgress(i + 1, days.length, day, totalRecords);
    }
    return { total: totalRecords, data: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
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
   * 按天拉取利润报表-店铺并按天覆盖保存（startDate=endDate 同一天，多天拆分，默认回退90天）
   */
  async fetchAllSellerProfitReportsByDayRange(accountId, listParams = {}, options = {}) {
    const { pageSize = 1000, delayBetweenPages = 500, onProgress = null } = options;
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    let totalRecords = 0;
    const baseParams = { ...listParams };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;
    for (let i = 0; i < days.length; i++) {
      const day = days[i];
      const allRecords = [];
      let offset = 0;
      let totalCount = 0;
      let hasMore = true;
      const actualPageSize = Math.min(pageSize, 10000);
      while (hasMore) {
        const res = await this.getSellerProfitReport(accountId, { ...baseParams, startDate: day, endDate: day, offset, length: actualPageSize });
        const pageRecords = res.data || [];
        const total = res.total ?? pageRecords.length;
        if (offset === 0) totalCount = total;
        allRecords.push(...pageRecords);
        if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
        else { offset += actualPageSize; if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages)); }
      }
      if (allRecords.length > 0) {
        await this.saveSellerProfitReportsForDay(accountId, allRecords, day);
        totalRecords += allRecords.length;
      }
      if (onProgress) onProgress(i + 1, days.length, day, totalRecords);
    }
    return { total: totalRecords, data: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
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
   * 拉取某张发票下的全部广告发票活动列表（分页拉全）
   */
  async fetchAllAdsInvoiceCampaignListForInvoice(accountId, params, options = {}) {
    const { delayBetweenPages = 200 } = options;
    const pageSize = 100;
    const list = [];
    let offset = 0;
    let hasMore = true;
    while (hasMore) {
      const res = await this.getAdsInvoiceCampaignList(accountId, { ...params, offset, length: pageSize });
      const page = res.data || [];
      list.push(...page);
      const total = res.total ?? 0;
      if (page.length < pageSize || (total > 0 && list.length >= total)) hasMore = false;
      else {
        offset += pageSize;
        if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
      }
    }
    return list;
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
      // API 要求：search_field_time、start_date、end_date 必须同时填写或都不填
      const hasDateRange = params.start_date || params.end_date;
      if (hasDateRange) {
        requestParams.search_field_time = params.search_field_time || 'apply_time';
        if (params.start_date) requestParams.start_date = params.start_date;
        if (params.end_date) requestParams.end_date = params.end_date;
      } else if (params.search_field_time) {
        requestParams.search_field_time = params.search_field_time;
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
   * 支持增量：listParams 传 start_date/end_date 时映射为 start_time/end_time，并默认 time_field=create_time
   */
  async fetchAllRequestFundsPoolPurchase(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    const apiParams = { ...listParams };
    if (apiParams.start_date != null) { apiParams.start_time = apiParams.start_time ?? apiParams.start_date; delete apiParams.start_date; }
    if (apiParams.end_date != null) { apiParams.end_time = apiParams.end_time ?? apiParams.end_date; delete apiParams.end_date; }
    if ((apiParams.start_time || apiParams.end_time) && !apiParams.time_field) apiParams.time_field = 'create_time';

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
            ...apiParams,
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
   * 支持增量：listParams 传 start_date/end_date 时映射为 start_time/end_time，默认 time_field=create_time
   */
  async fetchAllRequestFundsPoolInbound(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    const apiParams = { ...listParams };
    if (apiParams.start_date != null) { apiParams.start_time = apiParams.start_time ?? apiParams.start_date; delete apiParams.start_date; }
    if (apiParams.end_date != null) { apiParams.end_time = apiParams.end_time ?? apiParams.end_date; delete apiParams.end_date; }
    if ((apiParams.start_time || apiParams.end_time) && !apiParams.time_field) apiParams.time_field = 'create_time';

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
            ...apiParams,
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
   * 支持增量：listParams 传 start_date/end_date 时映射为 start_time/end_time，默认 time_field=create_time
   */
  async fetchAllRequestFundsPoolPrepay(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    const apiParams = { ...listParams };
    if (apiParams.start_date != null) { apiParams.start_time = apiParams.start_time ?? apiParams.start_date; delete apiParams.start_date; }
    if (apiParams.end_date != null) { apiParams.end_time = apiParams.end_time ?? apiParams.end_date; delete apiParams.end_date; }
    if ((apiParams.start_time || apiParams.end_time) && !apiParams.time_field) apiParams.time_field = 'create_time';

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
            ...apiParams,
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
   * 支持增量：listParams 传 start_date/end_date 时映射为 start_time/end_time，默认 search_field_time=create_time
   */
  async fetchAllRequestFundsPoolLogistics(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    const apiParams = { ...listParams };
    if (apiParams.start_date != null) { apiParams.start_time = apiParams.start_time ?? apiParams.start_date; delete apiParams.start_date; }
    if (apiParams.end_date != null) { apiParams.end_time = apiParams.end_time ?? apiParams.end_date; delete apiParams.end_date; }
    if ((apiParams.start_time || apiParams.end_time) && !apiParams.search_field_time) apiParams.search_field_time = 'create_time';

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
            ...apiParams,
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
   * 支持增量：listParams 传 start_date/end_date 时映射为 start_time/end_time，默认 search_field_time=create_time
   */
  async fetchAllRequestFundsPoolCustomFee(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    const apiParams = { ...listParams };
    if (apiParams.start_date != null) { apiParams.start_time = apiParams.start_time ?? apiParams.start_date; delete apiParams.start_date; }
    if (apiParams.end_date != null) { apiParams.end_time = apiParams.end_time ?? apiParams.end_date; delete apiParams.end_date; }
    if ((apiParams.start_time || apiParams.end_time) && !apiParams.search_field_time) apiParams.search_field_time = 'create_time';

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
            ...apiParams,
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
   * 支持增量：listParams 传 start_date/end_date 时映射为 startTime/endTime，默认 searchFieldTime=create_time
   */
  async fetchAllRequestFundsPoolOtherFee(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    const apiParams = { ...listParams };
    if (apiParams.start_date != null) { apiParams.startTime = apiParams.startTime ?? apiParams.start_date; delete apiParams.start_date; }
    if (apiParams.end_date != null) { apiParams.endTime = apiParams.endTime ?? apiParams.end_date; delete apiParams.end_date; }
    if ((apiParams.startTime || apiParams.endTime) && !apiParams.searchFieldTime) apiParams.searchFieldTime = 'create_time';

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
            ...apiParams,
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
   * 保存应收报告详情-基础信息（单条，upsert by accountId + sid + currencyCode + settleMonth）
   */
  async saveReceivableReportDetailInfo(accountId, sid, currencyCode, settleMonth, data) {
    if (!prisma.lingXingReceivableReportDetailInfo) return;
    const sidVal = sid != null ? parseInt(sid) : null;
    await prisma.lingXingReceivableReportDetailInfo.upsert({
      where: {
        accountId_sid_currencyCode_settleMonth: {
          accountId,
          sid: sidVal,
          currencyCode: currencyCode ?? '',
          settleMonth: settleMonth ?? ''
        }
      },
      update: { data: data ?? undefined, archived: false, updatedAt: new Date() },
      create: {
        accountId,
        sid: sidVal,
        currencyCode: currencyCode ?? '',
        settleMonth: settleMonth ?? '',
        data: data ?? undefined,
        archived: false
      }
    });
  }

  /**
   * 获取某账户某月已存在的 (sid, currencyCode) 列表，用于详情列表/详情信息的增量拉取
   * 优先从详情表取，若无则从主报告表取 sid，currencyCode 用默认 USD
   */
  async _getReceivableDetailKeysForMonth(accountId, settleMonth) {
    const details = await prisma.lingXingReceivableReportDetail.findMany({
      where: { accountId, settleMonth, archived: false },
      select: { sid: true, data: true }
    });
    const set = new Set();
    const list = [];
    for (const row of details) {
      const sid = row.sid != null ? row.sid : null;
      const code = (row.data && typeof row.data === 'object' && row.data.currencyCode) ? String(row.data.currencyCode) : 'USD';
      const key = `${sid}\t${code}`;
      if (!set.has(key)) {
        set.add(key);
        list.push({ sid, currencyCode: code });
      }
    }
    if (list.length > 0) return list;

    const reports = await prisma.lingXingReceivableReport.findMany({
      where: { accountId, settleMonth, archived: false },
      select: { sid: true, data: true }
    });
    for (const row of reports) {
      const sid = row.sid != null ? row.sid : null;
      const code = (row.data && typeof row.data === 'object' && row.data.currencyCode) ? String(row.data.currencyCode) : 'USD';
      const key = `${sid}\t${code}`;
      if (!set.has(key)) {
        set.add(key);
        list.push({ sid, currencyCode: code });
      }
    }
    return list;
  }

  /**
   * 结算中心-结算汇总列表
   * API: POST /bd/sp/api/open/settlement/summary/list  令牌桶容量: 10
   * @param {Object} params - dateType(必填 0/1/2), startDate, endDate(必填，间隔≤90天), offset, length, countryCodes, sids, currencyCode, searchField, searchValue
   * @returns {Promise<{ records: [], total: number }>}
   */
  async getSettlementSummaryList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      if (params.dateType === undefined || params.dateType === null) throw new Error('dateType 为必填（0 结算开始 1 结算结束 2 转账时间）');
      if (!params.startDate || !params.endDate) throw new Error('startDate、endDate 为必填，时间间隔最长 90 天');

      const requestParams = {
        dateType: Number(params.dateType),
        startDate: params.startDate,
        endDate: params.endDate,
        offset: params.offset !== undefined ? parseInt(params.offset) : 0,
        length: params.length !== undefined ? Math.min(parseInt(params.length), 100) : 20
      };
      if (params.countryCodes && Array.isArray(params.countryCodes)) requestParams.countryCodes = params.countryCodes.map(Number);
      if (params.sids && Array.isArray(params.sids)) requestParams.sids = params.sids.map(sid => parseInt(sid));
      if (params.currencyCode) requestParams.currencyCode = params.currencyCode;
      if (params.searchField) requestParams.searchField = params.searchField;
      if (params.searchValue && Array.isArray(params.searchValue)) requestParams.searchValue = params.searchValue;

      const response = await this.post(account, '/bd/sp/api/open/settlement/summary/list', requestParams, { successCode: [0, 200, '200'] });
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取结算汇总列表失败');
      }
      const data = response.data || {};
      const records = data.records || [];
      const total = data.total !== undefined ? data.total : records.length;
      return { records, total };
    } catch (error) {
      console.error('获取结算中心-结算汇总列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 按日拉取结算汇总并保存（startDate=endDate 同一天逐天查询，时间间隔最长 90 天）
   * listParams: start_date/end_date 或 startDate/endDate, dateType 默认 1（结算结束时间）
   */
  async fetchAllSettlementSummaryList(accountId, listParams = {}, options = {}) {
    const { pageSize = 20, delayBetweenPages = 500, onProgress = null } = options;
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const dateType = listParams.dateType !== undefined ? Number(listParams.dateType) : 1;
    const days = this._getDaysBetween(start, end);
    if (days.length > 90) throw new Error('结算汇总时间间隔最长不得超过 90 天');
    const baseParams = { ...listParams, dateType };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;

    let totalRecords = 0;
    for (let i = 0; i < days.length; i++) {
      const day = days[i];
      const allRecords = [];
      let offset = 0;
      let totalCount = 0;
      let hasMore = true;
      const actualPageSize = Math.min(pageSize, 100);
      while (hasMore) {
        const res = await this.getSettlementSummaryList(accountId, {
          ...baseParams,
          startDate: day,
          endDate: day,
          offset,
          length: actualPageSize
        });
        const pageRecords = res.records || [];
        const total = res.total ?? pageRecords.length;
        if (offset === 0) totalCount = total;
        allRecords.push(...pageRecords);
        if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
        else {
          offset += actualPageSize;
          if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
        }
      }
      if (allRecords.length > 0) {
        await this.saveSettlementSummariesForDay(accountId, allRecords, day, dateType);
        totalRecords += allRecords.length;
      }
      if (onProgress) onProgress(i + 1, days.length, day, totalRecords);
    }
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
  }

  /**
   * 按日保存结算汇总（先软删该 accountId+dateType+settleDate 再 upsert）
   */
  async saveSettlementSummariesForDay(accountId, records, settleDate, dateType) {
    try {
      if (!prisma.lingXingSettlementSummary) return;
      const day = String(settleDate).trim().slice(0, 10);
      const dateTypeNum = Number(dateType);
      await prisma.lingXingSettlementSummary.updateMany({
        where: { accountId, settleDate: day, dateType: dateTypeNum },
        data: { archived: true, updatedAt: new Date() }
      });
      for (const row of records) {
        const recordId = row.id != null ? String(row.id) : '';
        if (!recordId) continue;
        const sidVal = row.sid != null ? parseInt(row.sid) : null;
        await prisma.lingXingSettlementSummary.upsert({
          where: {
            accountId_recordId_dateType_settleDate: {
              accountId,
              recordId,
              dateType: dateTypeNum,
              settleDate: day
            }
          },
          update: { sid: sidVal, data: row, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            recordId,
            dateType: dateTypeNum,
            settleDate: day,
            sid: sidVal,
            data: row,
            archived: false
          }
        });
      }
      if (records.length > 0) console.log(`结算汇总已按日保存: settleDate=${day} dateType=${dateTypeNum} 共 ${records.length} 条`);
    } catch (error) {
      console.error('保存结算汇总到数据库失败:', error.message);
    }
  }

  /**
   * 结算中心-交易明细列表
   * API: POST /bd/sp/api/open/settlement/transaction/detail/list  令牌桶容量: 10
   * 无搜索值时，结算时间(startDate+endDate)与修改时间(gmtModifiedStart+gmtModifiedEnd)二选一必填；时间间隔不得超过 7 天；length 上限 10000
   * @returns {Promise<{ records: [], total: number }>}
   */
  async getSettlementTransactionDetailList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      const hasSettle = params.startDate && params.endDate;
      const hasModified = params.gmtModifiedStart && params.gmtModifiedEnd;
      if (!hasSettle && !hasModified) throw new Error('startDate+endDate 或 gmtModifiedStart+gmtModifiedEnd 二选一必填');
      if (hasSettle) {
        const a = new Date(params.startDate);
        const b = new Date(params.endDate);
        if (Math.ceil((b - a) / (24 * 3600 * 1000)) > 7) throw new Error('结算时间间隔不得超过 7 天');
      }

      const requestParams = {
        offset: params.offset !== undefined ? parseInt(params.offset) : 0,
        length: params.length !== undefined ? Math.min(parseInt(params.length), 10000) : 20
      };
      if (params.startDate) requestParams.startDate = params.startDate;
      if (params.endDate) requestParams.endDate = params.endDate;
      if (params.gmtModifiedStart) requestParams.gmtModifiedStart = params.gmtModifiedStart;
      if (params.gmtModifiedEnd) requestParams.gmtModifiedEnd = params.gmtModifiedEnd;
      if (params.countryCodes && Array.isArray(params.countryCodes)) requestParams.countryCodes = params.countryCodes.map(Number);
      if (params.sids && Array.isArray(params.sids)) requestParams.sids = params.sids.map(sid => parseInt(sid));
      if (params.eventType) requestParams.eventType = params.eventType;
      if (params.type) requestParams.type = params.type;
      if (params.searchField) requestParams.searchField = params.searchField;
      if (params.searchValue && Array.isArray(params.searchValue)) requestParams.searchValue = params.searchValue;

      const response = await this.post(account, '/bd/sp/api/open/settlement/transaction/detail/list', requestParams, { successCode: [0, 200, '200'] });
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取结算中心-交易明细列表失败');
      }
      const data = response.data || {};
      const records = data.records || [];
      const total = data.total !== undefined ? data.total : records.length;
      return { records, total };
    } catch (error) {
      console.error('获取结算中心-交易明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 从数据库获取账户下店铺 sid 列表（用于按 sid 遍历查询）
   */
  async _getSidsForAccount(accountId) {
    const rows = await prisma.lingXingSeller.findMany({
      where: { accountId, archived: false },
      select: { sid: true }
    });
    return rows.map(r => r.sid);
  }

  /**
   * 从数据库获取账户下亚马逊 sellerId 列表（用于按 sellerId 遍历查询，过滤空值）
   */
  async _getSellerIdsForAccount(accountId) {
    const rows = await prisma.lingXingSeller.findMany({
      where: { accountId, archived: false },
      select: { sellerId: true }
    });
    return rows.map(r => r.sellerId).filter(Boolean);
  }

  /**
   * 从数据库获取账户下店铺列表（sid + sellerId），用于需同时传 sid 与 sellerId 的接口
   */
  async _getSellersForAccount(accountId) {
    const rows = await prisma.lingXingSeller.findMany({
      where: { accountId, archived: false },
      select: { sid: true, sellerId: true }
    });
    return rows.filter(r => r.sellerId != null && r.sellerId !== '');
  }

  /**
   * 从数据库获取账户下店铺名列表（用于 FBA 成本流水等按 shop_names 遍历的接口，过滤空值）
   */
  async _getShopNamesForAccount(accountId) {
    const rows = await prisma.lingXingSeller.findMany({
      where: { accountId, archived: false },
      select: { name: true }
    });
    return rows.map(r => (r.name != null && String(r.name).trim() !== '') ? String(r.name).trim() : null).filter(Boolean);
  }

  /**
   * 按日拉取结算中心-交易明细并保存：从 DB 遍历 sid，每天 startDate=endDate=day 查询并分页，按日保存；eventType=ServiceFeeEventList 按日先删后插
   * 若日期范围超过 7 天则自动按 7 天一段拆分多段拉取
   */
  async fetchAllSettlementTransactionDetailList(accountId, listParams = {}, options = {}) {
    const { pageSize = 5000, delayBetweenPages = 500, onProgress = null } = options;
    const actualPageSize = Math.min(pageSize, 10000);
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const allDays = this._getDaysBetween(start, end);

    const sids = await this._getSidsForAccount(accountId);
    if (sids.length === 0) {
      console.log(`[settlementTransactionDetail] accountId=${accountId} 无店铺 sid，跳过`);
      return { total: 0, data: 0, records: 0, stats: { daysProcessed: 0, totalRecords: 0 } };
    }

    const baseParams = { ...listParams };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;

    let totalRecords = 0;
    let done = 0;
    const totalSteps = sids.length * allDays.length;

    for (let chunkStart = 0; chunkStart < allDays.length; chunkStart += 7) {
      const chunkDays = allDays.slice(chunkStart, chunkStart + 7);
      for (const sid of sids) {
        for (const day of chunkDays) {
          const allRecords = [];
          let offset = 0;
          let totalCount = 0;
          let hasMore = true;
          while (hasMore) {
            const res = await this.getSettlementTransactionDetailList(accountId, {
              ...baseParams,
              sids: [sid],
              startDate: day,
              endDate: day,
              offset,
              length: actualPageSize
            });
            const pageRecords = res.records || [];
            const total = res.total ?? pageRecords.length;
            if (offset === 0) totalCount = total;
            allRecords.push(...pageRecords);
            if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
            else {
              offset += actualPageSize;
              if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
            }
          }
          if (allRecords.length > 0) {
            await this.saveSettlementTransactionDetailsForDay(accountId, sid, allRecords, day);
            totalRecords += allRecords.length;
          }
          done++;
          if (onProgress) onProgress(done, totalSteps, day, totalRecords);
        }
      }
    }

    console.log(`结算中心-交易明细拉取完成: ${start}~${end} sids=${sids.length} 共 ${totalRecords} 条`);
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { daysProcessed: allDays.length, totalRecords } };
  }

  /**
   * 按日保存结算中心-交易明细：eventType=ServiceFeeEventList 时先软删该 (accountId,sid,settleDate,eventType) 再插入；其余按 (accountId,sid,settleDate,uniqueKey) upsert
   */
  async saveSettlementTransactionDetailsForDay(accountId, sid, records, settleDate) {
    try {
      if (!prisma.lingXingSettlementTransactionDetail) return;
      const day = String(settleDate).trim().slice(0, 10);
      const sidVal = parseInt(sid);

      const serviceFeeList = [];
      const others = [];
      for (const r of records) {
        const et = (r.eventType || '').toLowerCase();
        if (et === 'servicefeeeventlist') serviceFeeList.push(r);
        else others.push(r);
      }

      if (serviceFeeList.length > 0) {
        const eventTypeVal = serviceFeeList[0].eventType || 'ServiceFeeEventList';
        await prisma.lingXingSettlementTransactionDetail.updateMany({
          where: { accountId, sid: sidVal, settleDate: day, eventType: eventTypeVal },
          data: { archived: true, updatedAt: new Date() }
        });
        for (let i = 0; i < serviceFeeList.length; i++) {
          const row = serviceFeeList[i];
          const uniqueKey = (row.uniqueKey != null && String(row.uniqueKey).trim()) ? String(row.uniqueKey).trim() : `sf_${row.id ?? i}_${i}`;
          await prisma.lingXingSettlementTransactionDetail.create({
            data: {
              accountId,
              sid: sidVal,
              settleDate: day,
              uniqueKey,
              eventType: row.eventType || 'ServiceFeeEventList',
              data: row,
              archived: false
            }
          });
        }
      }

      for (let i = 0; i < others.length; i++) {
        const row = others[i];
        const uniqueKey = (row.uniqueKey != null && String(row.uniqueKey).trim()) ? String(row.uniqueKey).trim() : (row.id != null ? String(row.id) : `u_${day}_${sidVal}_${i}`);
        await prisma.lingXingSettlementTransactionDetail.upsert({
          where: {
            accountId_sid_settleDate_uniqueKey: {
              accountId,
              sid: sidVal,
              settleDate: day,
              uniqueKey
            }
          },
          update: { data: row, eventType: row.eventType ?? null, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            sid: sidVal,
            settleDate: day,
            uniqueKey,
            eventType: row.eventType ?? null,
            data: row,
            archived: false
          }
        });
      }

      if (records.length > 0) console.log(`结算中心-交易明细已按日保存: sid=${sidVal} settleDate=${day} 共 ${records.length} 条`);
    } catch (error) {
      console.error('保存结算中心-交易明细到数据库失败:', error.message);
    }
  }

  /**
   * 库存分类账 detail 列表（GET_LEDGER_DETAIL_VIEW_DATA）
   * API: POST /cost/center/ods/detail/query  令牌桶: 10  code 1 成功
   * @param params.sellerIds 必填 [string], startDate/endDate 必填 Y-m-d, offset/length(上限1000), fnskus, asins, mskus, eventTypes, disposition, locations, referenceId
   */
  async getInventoryLedgerDetailList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      if (!params.sellerIds || !Array.isArray(params.sellerIds) || params.sellerIds.length === 0) throw new Error('sellerIds 为必填且非空数组');
      if (!params.startDate || !params.endDate) throw new Error('startDate、endDate 为必填');

      const requestParams = {
        sellerIds: params.sellerIds,
        startDate: params.startDate,
        endDate: params.endDate,
        offset: params.offset !== undefined ? parseInt(params.offset) : 0,
        length: params.length !== undefined ? Math.min(parseInt(params.length), 1000) : 20
      };
      if (params.fnskus && Array.isArray(params.fnskus)) requestParams.fnskus = params.fnskus;
      if (params.asins && Array.isArray(params.asins)) requestParams.asins = params.asins;
      if (params.mskus && Array.isArray(params.mskus)) requestParams.mskus = params.mskus;
      if (params.eventTypes && Array.isArray(params.eventTypes)) requestParams.eventTypes = params.eventTypes;
      if (params.disposition) requestParams.disposition = params.disposition;
      if (params.locations && Array.isArray(params.locations)) requestParams.locations = params.locations;
      if (params.referenceId) requestParams.referenceId = params.referenceId;

      const response = await this.post(account, '/cost/center/ods/detail/query', requestParams, { successCode: [1] });
      if (response.code !== 1) throw new Error(response.msg || response.message || '获取库存分类账detail失败');
      const data = response.data || {};
      const records = data.records || [];
      const total = data.total !== undefined ? data.total : records.length;
      return { records, total };
    } catch (error) {
      console.error('获取库存分类账detail失败:', error.message);
      throw error;
    }
  }

  /**
   * 按日拉取库存分类账 detail：遍历 sellerId，每天 startDate=endDate=day 查询并分页保存
   */
  async fetchAllInventoryLedgerDetailList(accountId, listParams = {}, options = {}) {
    const { pageSize = 500, delayBetweenPages = 500, onProgress = null } = options;
    const actualPageSize = Math.min(pageSize, 1000);
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    const sellerIds = await this._getSellerIdsForAccount(accountId);
    if (sellerIds.length === 0) {
      console.log(`[inventoryLedgerDetail] accountId=${accountId} 无 sellerId，跳过`);
      return { total: 0, data: 0, records: 0, stats: { daysProcessed: 0, totalRecords: 0 } };
    }

    const baseParams = { ...listParams };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;

    let totalRecords = 0;
    let done = 0;
    const totalSteps = sellerIds.length * days.length;
    for (const sellerId of sellerIds) {
      for (const day of days) {
        const allRecords = [];
        let offset = 0;
        let totalCount = 0;
        let hasMore = true;
        while (hasMore) {
          const res = await this.getInventoryLedgerDetailList(accountId, {
            ...baseParams,
            sellerIds: [sellerId],
            startDate: day,
            endDate: day,
            offset,
            length: actualPageSize
          });
          const pageRecords = res.records || [];
          const total = res.total ?? pageRecords.length;
          if (offset === 0) totalCount = total;
          allRecords.push(...pageRecords);
          if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
          else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
          }
        }
        if (allRecords.length > 0) {
          await this.saveInventoryLedgerDetailsForDay(accountId, sellerId, allRecords, day);
          totalRecords += allRecords.length;
        }
        done++;
        if (onProgress) onProgress(done, totalSteps, day, totalRecords);
      }
    }
    console.log(`库存分类账detail拉取完成: ${start}~${end} sellerIds=${sellerIds.length} 共 ${totalRecords} 条`);
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
  }

  /**
   * 按日保存库存分类账 detail（唯一键 sellerId+settleDate+uniqueMd5Idx+uniqueMd5）
   */
  async saveInventoryLedgerDetailsForDay(accountId, sellerId, records, settleDate) {
    try {
      if (!prisma.lingXingInventoryLedgerDetail) return;
      const day = String(settleDate).trim().slice(0, 10);
      for (const row of records) {
        const idx = (row.uniqueMd5Idx != null && String(row.uniqueMd5Idx).trim()) ? String(row.uniqueMd5Idx).trim() : '0';
        const md5 = (row.uniqueMd5 != null && String(row.uniqueMd5).trim()) ? String(row.uniqueMd5).trim() : '0';
        await prisma.lingXingInventoryLedgerDetail.upsert({
          where: {
            inv_ledger_detail_account_seller_date_md5: {
              accountId,
              sellerId: String(sellerId),
              settleDate: day,
              uniqueMd5Idx: idx,
              uniqueMd5: md5
            }
          },
          update: { data: row, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            sellerId: String(sellerId),
            settleDate: day,
            uniqueMd5Idx: idx,
            uniqueMd5: md5,
            data: row,
            archived: false
          }
        });
      }
      if (records.length > 0) console.log(`库存分类账detail已按日保存: sellerId=${sellerId} settleDate=${day} 共 ${records.length} 条`);
    } catch (error) {
      console.error('保存库存分类账detail失败:', error.message);
    }
  }

  /**
   * 库存分类账 summary 列表（queryType=2 按天）
   * API: POST /cost/center/ods/summary/query  code 1 成功
   */
  async getInventoryLedgerSummaryList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      if (!params.sellerIds || !Array.isArray(params.sellerIds) || params.sellerIds.length === 0) throw new Error('sellerIds 为必填且非空数组');
      if (!params.startDate || !params.endDate) throw new Error('startDate、endDate 为必填');
      const requestParams = {
        sellerIds: params.sellerIds,
        queryType: params.queryType !== undefined ? Number(params.queryType) : 2,
        startDate: params.startDate,
        endDate: params.endDate,
        offset: params.offset !== undefined ? parseInt(params.offset) : 0,
        length: params.length !== undefined ? Math.min(parseInt(params.length), 1000) : 20
      };
      if (params.fnskus && Array.isArray(params.fnskus)) requestParams.fnskus = params.fnskus;
      if (params.asins && Array.isArray(params.asins)) requestParams.asins = params.asins;
      if (params.mskus && Array.isArray(params.mskus)) requestParams.mskus = params.mskus;
      if (params.disposition) requestParams.disposition = params.disposition;
      if (params.locations && Array.isArray(params.locations)) requestParams.locations = params.locations;

      const response = await this.post(account, '/cost/center/ods/summary/query', requestParams, { successCode: [1] });
      if (response.code !== 1) throw new Error(response.msg || response.message || '获取库存分类账summary失败');
      const data = response.data || {};
      const records = data.records || [];
      const total = data.total !== undefined ? data.total : records.length;
      return { records, total };
    } catch (error) {
      console.error('获取库存分类账summary失败:', error.message);
      throw error;
    }
  }

  /**
   * 按日拉取库存分类账 summary（queryType=2 按天），遍历 sellerId，每天查询并保存
   */
  async fetchAllInventoryLedgerSummaryList(accountId, listParams = {}, options = {}) {
    const { pageSize = 500, delayBetweenPages = 500, onProgress = null } = options;
    const actualPageSize = Math.min(pageSize, 1000);
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    const sellerIds = await this._getSellerIdsForAccount(accountId);
    if (sellerIds.length === 0) {
      console.log(`[inventoryLedgerSummary] accountId=${accountId} 无 sellerId，跳过`);
      return { total: 0, data: 0, records: 0, stats: { daysProcessed: 0, totalRecords: 0 } };
    }

    const baseParams = { ...listParams, queryType: 2 };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;

    let totalRecords = 0;
    let done = 0;
    const totalSteps = sellerIds.length * days.length;
    for (const sellerId of sellerIds) {
      for (const day of days) {
        const allRecords = [];
        let offset = 0;
        let totalCount = 0;
        let hasMore = true;
        while (hasMore) {
          const res = await this.getInventoryLedgerSummaryList(accountId, {
            ...baseParams,
            sellerIds: [sellerId],
            startDate: day,
            endDate: day,
            offset,
            length: actualPageSize
          });
          const pageRecords = res.records || [];
          const total = res.total ?? pageRecords.length;
          if (offset === 0) totalCount = total;
          allRecords.push(...pageRecords);
          if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
          else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
          }
        }
        if (allRecords.length > 0) {
          await this.saveInventoryLedgerSummaryForDay(accountId, sellerId, allRecords, day);
          totalRecords += allRecords.length;
        }
        done++;
        if (onProgress) onProgress(done, totalSteps, day, totalRecords);
      }
    }
    console.log(`库存分类账summary拉取完成: ${start}~${end} sellerIds=${sellerIds.length} 共 ${totalRecords} 条`);
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
  }

  /**
   * 按日保存库存分类账 summary（rowKey 用 date+fnsku+msku+disposition+location）
   */
  async saveInventoryLedgerSummaryForDay(accountId, sellerId, records, settleDate) {
    try {
      if (!prisma.lingXingInventoryLedgerSummary) return;
      const day = String(settleDate).trim().slice(0, 10);
      for (const row of records) {
        const rowKey = `${row.date || day}_${row.fnsku || ''}_${row.msku || ''}_${row.disposition || ''}_${row.location || ''}`;
        await prisma.lingXingInventoryLedgerSummary.upsert({
          where: {
            inv_ledger_summary_account_seller_date_key: {
              accountId,
              sellerId: String(sellerId),
              settleDate: day,
              rowKey
            }
          },
          update: { data: row, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            sellerId: String(sellerId),
            settleDate: day,
            rowKey,
            data: row,
            archived: false
          }
        });
      }
      if (records.length > 0) console.log(`库存分类账summary已按日保存: sellerId=${sellerId} settleDate=${day} 共 ${records.length} 条`);
    } catch (error) {
      console.error('保存库存分类账summary失败:', error.message);
    }
  }

  /**
   * 发货结算报告列表
   * API: POST /cost/center/api/settlement/report  令牌桶: 3  code 1 成功；timeType 06 更新时间
   */
  async getSettlementReportList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      if (!params.amazonSellerIds || !Array.isArray(params.amazonSellerIds) || params.amazonSellerIds.length === 0) throw new Error('amazonSellerIds 为必填且非空数组');
      if (!params.sids || !Array.isArray(params.sids) || params.sids.length === 0) throw new Error('sids 为必填且非空数组');
      if (!params.filterBeginDate || !params.filterEndDate) throw new Error('filterBeginDate、filterEndDate 为必填');

      const requestParams = {
        amazonSellerIds: params.amazonSellerIds,
        sids: params.sids.map(sid => parseInt(sid)),
        timeType: params.timeType || '06',
        filterBeginDate: params.filterBeginDate,
        filterEndDate: params.filterEndDate,
        offset: params.offset !== undefined ? parseInt(params.offset) : 0,
        length: params.length !== undefined ? Math.min(parseInt(params.length), 1000) : 20
      };
      if (params.countryCodes && Array.isArray(params.countryCodes)) requestParams.countryCodes = params.countryCodes;
      if (params.orderNumbers && Array.isArray(params.orderNumbers)) requestParams.orderNumbers = params.orderNumbers;
      if (params.shipmentNumbers && Array.isArray(params.shipmentNumbers)) requestParams.shipmentNumbers = params.shipmentNumbers;
      if (params.mskus && Array.isArray(params.mskus)) requestParams.mskus = params.mskus;
      if (params.fulfillmentType) requestParams.fulfillmentType = params.fulfillmentType;

      const response = await this.post(account, '/cost/center/api/settlement/report', requestParams, { successCode: [1] });
      if (response.code !== 1) throw new Error(response.msg || response.message || '获取发货结算报告失败');
      const data = response.data || {};
      const records = data.records || [];
      const total = data.total !== undefined ? data.total : records.length;
      return { records, total };
    } catch (error) {
      console.error('获取发货结算报告失败:', error.message);
      throw error;
    }
  }

  /**
   * 按日拉取发货结算报告：遍历 sid+amazonSellerId（从 DB），timeType=06 更新时间，每天查询并保存
   */
  async fetchAllSettlementReportList(accountId, listParams = {}, options = {}) {
    const { pageSize = 500, delayBetweenPages = 500, onProgress = null } = options;
    const actualPageSize = Math.min(pageSize, 1000);
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    const sellers = await this._getSellersForAccount(accountId);
    if (sellers.length === 0) {
      console.log(`[settlementReport] accountId=${accountId} 无店铺，跳过`);
      return { total: 0, data: 0, records: 0, stats: { daysProcessed: 0, totalRecords: 0 } };
    }

    const baseParams = { ...listParams, timeType: listParams.timeType || '06' };
    delete baseParams.start_date;
    delete baseParams.end_date;
    delete baseParams.startDate;
    delete baseParams.endDate;

    let totalRecords = 0;
    let done = 0;
    const totalSteps = sellers.length * days.length;
    for (const seller of sellers) {
      const amazonSellerIds = [seller.sellerId];
      const sids = [seller.sid];
      for (const day of days) {
        const allRecords = [];
        let offset = 0;
        let totalCount = 0;
        let hasMore = true;
        while (hasMore) {
          const res = await this.getSettlementReportList(accountId, {
            ...baseParams,
            amazonSellerIds,
            sids,
            filterBeginDate: day,
            filterEndDate: day,
            offset,
            length: actualPageSize
          });
          const pageRecords = res.records || [];
          const total = res.total ?? pageRecords.length;
          if (offset === 0) totalCount = total;
          allRecords.push(...pageRecords);
          if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
          else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
          }
        }
        if (allRecords.length > 0) {
          await this.saveSettlementReportsForDay(accountId, seller.sid, allRecords, day);
          totalRecords += allRecords.length;
        }
        done++;
        if (onProgress) onProgress(done, totalSteps, day, totalRecords);
      }
    }
    console.log(`发货结算报告拉取完成: ${start}~${end} 店铺数=${sellers.length} 共 ${totalRecords} 条`);
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
  }

  /**
   * 按日保存发货结算报告（唯一键 sid+settleDate+amazonOrderId+shipmentId+shipmentItemId+msku，空用 ''）
   */
  async saveSettlementReportsForDay(accountId, sid, records, settleDate) {
    try {
      if (!prisma.lingXingSettlementReport) return;
      const day = String(settleDate).trim().slice(0, 10);
      const sidVal = parseInt(sid);
      for (const row of records) {
        const amazonOrderId = (row.amazonOrderId != null && String(row.amazonOrderId).trim()) ? String(row.amazonOrderId).trim() : '';
        const shipmentId = (row.shipmentId != null && String(row.shipmentId).trim()) ? String(row.shipmentId).trim() : '';
        const shipmentItemId = (row.shipmentItemId != null && String(row.shipmentItemId).trim()) ? String(row.shipmentItemId).trim() : '';
        const msku = (row.msku != null && String(row.msku).trim()) ? String(row.msku).trim() : '';
        await prisma.lingXingSettlementReport.upsert({
          where: {
            settlement_report_unique: {
              accountId,
              sid: sidVal,
              settleDate: day,
              amazonOrderId,
              shipmentId,
              shipmentItemId,
              msku
            }
          },
          update: { data: row, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            sid: sidVal,
            settleDate: day,
            amazonOrderId,
            shipmentId,
            shipmentItemId,
            msku,
            data: row,
            archived: false
          }
        });
      }
      if (records.length > 0) console.log(`发货结算报告已按日保存: sid=${sidVal} settleDate=${day} 共 ${records.length} 条`);
    } catch (error) {
      console.error('保存发货结算报告失败:', error.message);
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
   * 返回 [startYyyyMm, endYyyyMm] 之间的月份列表（含首尾），格式 'YYYY-MM'
   */
  _getMonthsBetween(startYyyyMm, endYyyyMm) {
    const list = [];
    const [sy, sm] = startYyyyMm.split('-').map(Number);
    const [ey, em] = endYyyyMm.split('-').map(Number);
    for (let y = sy, m = sm; y < ey || (y === ey && m <= em); m++) {
      if (m > 12) { m = 1; y++; }
      list.push(`${y}-${String(m).padStart(2, '0')}`);
    }
    return list;
  }

  /**
   * 应收报告增量同步：按 settleMonth 逐月拉取并保存，当月数据支持重复查询（同月多次拉取会 upsert 覆盖）。
   * 每月数据有分页，内部已按页拉全后保存。
   * @param {Object} options - defaultLookbackMonths 无历史状态时回溯月数（默认 12），endMonth 结束月 'YYYY-MM'（默认当前月）
   */
  async incrementalSyncReceivableReport(accountId, options = {}) {
    const taskType = 'receivableReport';
    const defaultLookbackMonths = Math.min(Math.max(1, options.defaultLookbackMonths ?? 12), 60);
    const now = new Date();
    const endMonth = options.endMonth ?? `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    const state = await lingXingSyncStateService.getSyncState(accountId, taskType, null);
    let startMonth;
    if (state?.lastEndTimestamp) {
      const d = new Date(state.lastEndTimestamp);
      const lastYyyyMm = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const [y, m] = lastYyyyMm.split('-').map(Number);
      const nextM = m === 12 ? 1 : m + 1;
      const nextY = m === 12 ? y + 1 : y;
      startMonth = `${nextY}-${String(nextM).padStart(2, '0')}`;
    } else {
      const d = new Date(now.getFullYear(), now.getMonth() - defaultLookbackMonths + 1, 1);
      startMonth = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      if (startMonth > endMonth) startMonth = endMonth;
    }

    const months = this._getMonthsBetween(startMonth, endMonth);
    if (months.length === 0) {
      return { results: [{ success: true, recordCount: 0, start_date: startMonth, end_date: endMonth, skipped: true }], summary: { successCount: 1, failCount: 0, totalRecords: 0 } };
    }

    let totalRecords = 0;
    let lastProcessedMonth = null;
    const errors = [];

    for (const settleMonth of months) {
      try {
        const result = await this.fetchAllReceivableReportList(accountId, { settleMonth }, options);
        const count = result?.total ?? result?.data?.length ?? 0;
        totalRecords += count;
        lastProcessedMonth = settleMonth;
      } catch (err) {
        const msg = err?.message ?? String(err);
        errors.push(`${settleMonth}: ${msg}`);
        console.error(`[receivableReport] accountId=${accountId} settleMonth=${settleMonth} 失败:`, msg);
      }
    }

    const lastMonthDate = lastProcessedMonth ? (() => {
      const [y, m] = lastProcessedMonth.split('-').map(Number);
      return new Date(y, m, 0, 23, 59, 59, 999); // 该月最后一天
    })() : null;

    if (lastMonthDate) {
      await lingXingSyncStateService.upsertSyncState(accountId, taskType, null, {
        lastEndTimestamp: lastMonthDate,
        lastSyncAt: new Date(),
        lastRecordCount: totalRecords,
        lastStatus: errors.length > 0 ? 'partial' : 'success',
        lastErrorMessage: errors.length > 0 ? errors.join('; ') : null
      });
    }

    const success = errors.length === 0;
    return {
      results: [{
        success,
        recordCount: totalRecords,
        start_date: months[0],
        end_date: months[months.length - 1],
        error: errors.length > 0 ? errors.join('; ') : undefined
      }],
      summary: { successCount: success ? 1 : 0, failCount: success ? 0 : 1, totalRecords }
    };
  }

  /**
   * 应收报告-详情-列表 增量同步：按 settleMonth 逐月、按 (sid, currencyCode) 拉取并分页保存。
   * 依赖同月已有主报告或详情数据以得到 (sid, currencyCode)；若某月无则先拉取该月主报告再拉详情。
   */
  async incrementalSyncReceivableReportDetailList(accountId, options = {}) {
    const taskType = 'receivableReportDetail';
    const defaultLookbackMonths = Math.min(Math.max(1, options.defaultLookbackMonths ?? 12), 60);
    const now = new Date();
    const endMonth = options.endMonth ?? `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    const state = await lingXingSyncStateService.getSyncState(accountId, taskType, null);
    let startMonth;
    if (state?.lastEndTimestamp) {
      const d = new Date(state.lastEndTimestamp);
      const lastYyyyMm = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const [y, m] = lastYyyyMm.split('-').map(Number);
      const nextM = m === 12 ? 1 : m + 1;
      const nextY = m === 12 ? y + 1 : y;
      startMonth = `${nextY}-${String(nextM).padStart(2, '0')}`;
    } else {
      const d = new Date(now.getFullYear(), now.getMonth() - defaultLookbackMonths + 1, 1);
      startMonth = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      if (startMonth > endMonth) startMonth = endMonth;
    }

    const months = this._getMonthsBetween(startMonth, endMonth);
    if (months.length === 0) {
      return { results: [{ success: true, recordCount: 0, skipped: true }], summary: { successCount: 1, failCount: 0, totalRecords: 0 } };
    }

    let totalRecords = 0;
    let lastProcessedMonth = null;
    const errors = [];

    for (const settleMonth of months) {
      let keys = await this._getReceivableDetailKeysForMonth(accountId, settleMonth);
      if (keys.length === 0) {
        try {
          await this.fetchAllReceivableReportList(accountId, { settleMonth }, options);
          keys = await this._getReceivableDetailKeysForMonth(accountId, settleMonth);
        } catch (e) {
          errors.push(`${settleMonth}(先拉主报告): ${e?.message ?? e}`);
          continue;
        }
      }
      for (const { sid, currencyCode } of keys) {
        try {
          const result = await this.fetchAllReceivableReportDetailList(accountId, { sid, currencyCode, settleMonth }, options);
          totalRecords += result?.total ?? result?.data?.length ?? 0;
        } catch (err) {
          errors.push(`${settleMonth}/${sid}/${currencyCode}: ${err?.message ?? err}`);
        }
      }
      lastProcessedMonth = settleMonth;
    }

    const lastMonthDate = lastProcessedMonth ? (() => {
      const [y, m] = lastProcessedMonth.split('-').map(Number);
      return new Date(y, m, 0, 23, 59, 59, 999);
    })() : null;
    if (lastMonthDate) {
      await lingXingSyncStateService.upsertSyncState(accountId, taskType, null, {
        lastEndTimestamp: lastMonthDate,
        lastSyncAt: new Date(),
        lastRecordCount: totalRecords,
        lastStatus: errors.length > 0 ? 'partial' : 'success',
        lastErrorMessage: errors.length > 0 ? errors.join('; ') : null
      });
    }

    const success = errors.length === 0;
    return {
      results: [{ success, recordCount: totalRecords, error: errors.length > 0 ? errors.join('; ') : undefined }],
      summary: { successCount: success ? 1 : 0, failCount: success ? 0 : 1, totalRecords }
    };
  }

  /**
   * 应收报告-详情-基础信息 增量同步：按 settleMonth 逐月、按 (sid, currencyCode) 拉取并保存单条基础信息。
   */
  async incrementalSyncReceivableReportDetailInfo(accountId, options = {}) {
    const taskType = 'receivableReportDetailInfo';
    const defaultLookbackMonths = Math.min(Math.max(1, options.defaultLookbackMonths ?? 12), 60);
    const now = new Date();
    const endMonth = options.endMonth ?? `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    const state = await lingXingSyncStateService.getSyncState(accountId, taskType, null);
    let startMonth;
    if (state?.lastEndTimestamp) {
      const d = new Date(state.lastEndTimestamp);
      const lastYyyyMm = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const [y, m] = lastYyyyMm.split('-').map(Number);
      const nextM = m === 12 ? 1 : m + 1;
      const nextY = m === 12 ? y + 1 : y;
      startMonth = `${nextY}-${String(nextM).padStart(2, '0')}`;
    } else {
      const d = new Date(now.getFullYear(), now.getMonth() - defaultLookbackMonths + 1, 1);
      startMonth = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      if (startMonth > endMonth) startMonth = endMonth;
    }

    const months = this._getMonthsBetween(startMonth, endMonth);
    if (months.length === 0) {
      return { results: [{ success: true, recordCount: 0, skipped: true }], summary: { successCount: 1, failCount: 0, totalRecords: 0 } };
    }

    let totalRecords = 0;
    let lastProcessedMonth = null;
    const errors = [];

    for (const settleMonth of months) {
      let keys = await this._getReceivableDetailKeysForMonth(accountId, settleMonth);
      if (keys.length === 0) {
        try {
          await this.fetchAllReceivableReportList(accountId, { settleMonth }, options);
          keys = await this._getReceivableDetailKeysForMonth(accountId, settleMonth);
        } catch (e) {
          errors.push(`${settleMonth}(先拉主报告): ${e?.message ?? e}`);
          continue;
        }
      }
      for (const { sid, currencyCode } of keys) {
        try {
          const result = await this.getReceivableReportDetailInfo(accountId, { sid, currencyCode, settleMonth });
          const info = result?.data;
          if (info != null) {
            await this.saveReceivableReportDetailInfo(accountId, sid, currencyCode, settleMonth, info);
            totalRecords += 1;
          }
        } catch (err) {
          errors.push(`${settleMonth}/${sid}/${currencyCode}: ${err?.message ?? err}`);
        }
      }
      lastProcessedMonth = settleMonth;
    }

    const lastMonthDate = lastProcessedMonth ? (() => {
      const [y, m] = lastProcessedMonth.split('-').map(Number);
      return new Date(y, m, 0, 23, 59, 59, 999);
    })() : null;
    if (lastMonthDate) {
      await lingXingSyncStateService.upsertSyncState(accountId, taskType, null, {
        lastEndTimestamp: lastMonthDate,
        lastSyncAt: new Date(),
        lastRecordCount: totalRecords,
        lastStatus: errors.length > 0 ? 'partial' : 'success',
        lastErrorMessage: errors.length > 0 ? errors.join('; ') : null
      });
    }

    const success = errors.length === 0;
    return {
      results: [{ success, recordCount: totalRecords, error: errors.length > 0 ? errors.join('; ') : undefined }],
      summary: { successCount: success ? 1 : 0, failCount: success ? 0 : 1, totalRecords }
    };
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
          if (listParams.settleMonth) {
            pageData.forEach(d => { if (d.settleMonth == null) d.settleMonth = listParams.settleMonth; });
          }

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

      // 保存到数据库（传入当前查询的 sid/settleMonth，接口未返回 sid 时用其补全，避免唯一键为 null）
      if (allData && allData.length > 0) {
        await this.saveReceivableReportDetails(accountId, allData, { sid: listParams.sid, settleMonth: listParams.settleMonth });
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
   * 查询利润报表-订单（本接口即将下线，建议使用 transaction 视图）
   * API: POST /bd/profit/report/open/report/order/list  令牌桶容量: 10
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，上限10000（可选）
   *   - search_date_field: 时间类型，必填。posted_date_locale 结算时间 / fund_transfer_datetime_locale 转账时间 / shipment_datetime_locale 发货时间
   *   - start_date: 开始时间，必填
   *   - end_date: 结束时间，必填
   *   - mids: 站点id数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - search_field, search_value, currency_code, account_type, settlement_status, fund_transfer_status, event_source, description（可选）
   * @returns {Promise<Object>} { data: [], total: 0 }
   */
  async getProfitReportOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });
      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }
      if (!params.search_date_field || !params.start_date || !params.end_date) {
        throw new Error('search_date_field、start_date、end_date 为必填参数');
      }
      const requestParams = {
        search_date_field: params.search_date_field,
        start_date: params.start_date,
        end_date: params.end_date,
        offset: params.offset !== undefined ? params.offset : 0,
        length: Math.min(Number(params.length) || 20, 10000)
      };
      if (params.mids && Array.isArray(params.mids)) requestParams.mids = params.mids;
      if (params.sids && Array.isArray(params.sids)) requestParams.sids = params.sids;
      if (params.search_field) requestParams.search_field = params.search_field;
      if (params.search_value && Array.isArray(params.search_value)) requestParams.search_value = params.search_value;
      if (params.currency_code) requestParams.currency_code = params.currency_code;
      if (params.account_type) requestParams.account_type = params.account_type;
      if (params.settlement_status && Array.isArray(params.settlement_status)) requestParams.settlement_status = params.settlement_status;
      if (params.fund_transfer_status && Array.isArray(params.fund_transfer_status)) requestParams.fund_transfer_status = params.fund_transfer_status;
      if (params.event_source && Array.isArray(params.event_source)) requestParams.event_source = params.event_source;
      if (params.description && Array.isArray(params.description)) requestParams.description = params.description;

      const response = await this.post(account, '/bd/profit/report/open/report/order/list', requestParams, { successCode: [0, 200, '200'] });
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取利润报表-订单列表失败');
      }
      const data = response.data || [];
      const total = response.total ?? data.length;
      return { data, total };
    } catch (error) {
      console.error('获取利润报表-订单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存利润报表-订单到数据库（唯一键：sid + posted_datetime_locale + fid + order_id + event_source + seller_sku/msku）
   */
  async saveProfitReportOrders(accountId, orders) {
    try {
      if (!prisma.lingXingProfitReportOrder) {
        console.error('Prisma Client 中未找到 lingXingProfitReportOrder 模型');
        return;
      }
      for (const row of orders) {
        const sid = row.sid !== undefined && row.sid !== null ? parseInt(row.sid) : 0;
        const posted = (row.posted_datetime_locale != null ? String(row.posted_datetime_locale) : '').trim();
        const fid = (row.fid != null ? String(row.fid) : '').trim();
        const orderId = (row.order_id != null ? String(row.order_id) : '').trim();
        const eventSource = (row.event_source != null ? String(row.event_source) : '').trim();
        const sellerSku = (row.msku != null ? String(row.msku) : (row.seller_sku != null ? String(row.seller_sku) : '')).trim();
        await prisma.lingXingProfitReportOrder.upsert({
          where: {
            accountId_sid_posted_fid_order_event_seller: {
              accountId,
              sid,
              postedDatetimeLocale: posted,
              fid,
              orderId,
              eventSource,
              sellerSku
            }
          },
          update: { data: row, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            sid,
            postedDatetimeLocale: posted,
            fid,
            orderId,
            eventSource,
            sellerSku,
            data: row,
            archived: false
          }
        });
      }
      console.log(`利润报表-订单已保存到数据库: 共 ${orders.length} 条`);
    } catch (error) {
      console.error('保存利润报表-订单到数据库失败:', error.message);
    }
  }

  /**
   * 自动拉取所有利润报表-订单（分页，length 上限 10000）
   */
  async fetchAllProfitReportOrders(accountId, listParams = {}, options = {}) {
    const { pageSize = 5000, delayBetweenPages = 500, onProgress = null } = options;
    const actualPageSize = Math.min(pageSize, 10000);
    try {
      console.log('开始自动拉取所有利润报表-订单数据...');
      const allData = [];
      let offset = 0;
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;
      while (hasMore) {
        currentPage++;
        const pageResult = await this.getProfitReportOrderList(accountId, { ...listParams, offset, length: actualPageSize });
        const pageData = pageResult.data || [];
        const total = pageResult.total ?? pageData.length;
        if (currentPage === 1) totalCount = total;
        allData.push(...pageData);
        if (onProgress) onProgress(currentPage, totalCount ? Math.ceil(totalCount / actualPageSize) : 1, allData.length, totalCount);
        if (pageData.length < actualPageSize || (totalCount > 0 && allData.length >= totalCount)) hasMore = false;
        else {
          offset += actualPageSize;
          if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
        }
      }
      if (allData.length > 0) await this.saveProfitReportOrders(accountId, allData);
      console.log(`利润报表-订单拉取完成，共 ${allData.length} 条`);
      return { data: allData, total: allData.length, stats: { totalRecords: allData.length, pagesFetched: currentPage } };
    } catch (error) {
      console.error('自动拉取利润报表-订单失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询利润报表 - 订单维度 transaction 视图
   * API: POST /basicOpen/finance/profitReport/order/transcation/list  令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（驼峰命名，与接口一致）
   *   - startDate, endDate: 必填，格式 YYYY-MM-DD
   *   - offset, length: 分页，length 上限 1000
   *   - searchDateField: posted_date_locale | fund_transfer_datetime_locale | shipment_datetime_locale | order_datetime_locale | accounting_time
   *   - mids, sids, currencyCode, searchField, searchValue, sortField, sortType, settlementStatus, fundTransferStatus, accountType, eventSource, fulfillment, orderStatus, gmtModifiedStartDate, gmtModifiedEndDate 等
   * @returns {Promise<Object>} { records: [], total: 0 }
   */
  async getProfitReportOrderTransactionList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      if (!params.startDate || !params.endDate) throw new Error('startDate、endDate 为必填参数');

      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate,
        offset: params.offset !== undefined ? params.offset : 0,
        length: Math.min(Number(params.length) || 20, 1000)
      };
      if (params.searchDateField) requestParams.searchDateField = params.searchDateField;
      if (params.mids && Array.isArray(params.mids)) requestParams.mids = params.mids;
      if (params.sids && Array.isArray(params.sids)) requestParams.sids = params.sids;
      if (params.currencyCode) requestParams.currencyCode = params.currencyCode;
      if (params.searchField) requestParams.searchField = params.searchField;
      if (params.searchValue && Array.isArray(params.searchValue)) requestParams.searchValue = params.searchValue;
      if (params.sortField) requestParams.sortField = params.sortField;
      if (params.sortType) requestParams.sortType = params.sortType;
      if (params.settlementStatus && Array.isArray(params.settlementStatus)) requestParams.settlementStatus = params.settlementStatus;
      if (params.fundTransferStatus && Array.isArray(params.fundTransferStatus)) requestParams.fundTransferStatus = params.fundTransferStatus;
      if (params.accountType && Array.isArray(params.accountType)) requestParams.accountType = params.accountType;
      if (params.eventSource && Array.isArray(params.eventSource)) requestParams.eventSource = params.eventSource;
      if (params.fulfillment && Array.isArray(params.fulfillment)) requestParams.fulfillment = params.fulfillment;
      if (params.principalUids && Array.isArray(params.principalUids)) requestParams.principalUids = params.principalUids;
      if (params.productDeveloperUids && Array.isArray(params.productDeveloperUids)) requestParams.productDeveloperUids = params.productDeveloperUids;
      if (params.gmtModifiedStartDate) requestParams.gmtModifiedStartDate = params.gmtModifiedStartDate;
      if (params.gmtModifiedEndDate) requestParams.gmtModifiedEndDate = params.gmtModifiedEndDate;
      if (params.orderStatus) requestParams.orderStatus = params.orderStatus;
      if (params.transactionStatus && Array.isArray(params.transactionStatus)) requestParams.transactionStatus = params.transactionStatus;

      const response = await this.post(account, '/basicOpen/finance/profitReport/order/transcation/list', requestParams, { successCode: [0, 200, '200'] });
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取利润报表-订单transaction列表失败');
      }
      const data = response.data || {};
      const records = data.records || [];
      const total = data.total !== undefined ? data.total : (response.total !== undefined ? response.total : records.length);
      return { records, total };
    } catch (error) {
      console.error('获取利润报表-订单transaction列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 按天覆盖保存利润报表-订单 transaction：先删除该 accountId+postedDate 的旧数据，再写入新记录
   */
  async saveProfitReportOrderTransactions(accountId, records, postedDate) {
    try {
      if (!prisma.lingXingProfitReportOrderTransaction) {
        console.error('Prisma Client 中未找到 lingXingProfitReportOrderTransaction 模型');
        return;
      }
      const day = String(postedDate).trim().slice(0, 10);
      await prisma.lingXingProfitReportOrderTransaction.updateMany({
        where: { accountId, postedDate: day },
        data: { archived: true, updatedAt: new Date() }
      });
      for (const row of records) {
        const recordId = (row.id != null ? String(row.id) : '').trim();
        if (!recordId) continue;
        await prisma.lingXingProfitReportOrderTransaction.upsert({
          where: {
            accountId_recordId_profit_tx: { accountId, recordId }
          },
          update: {
            postedDate: day,
            data: row,
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId,
            recordId,
            postedDate: day,
            data: row,
            archived: false
          }
        });
      }
      if (records.length > 0) {
        console.log(`利润报表-订单transaction已按天覆盖保存: postedDate=${day} 共 ${records.length} 条`);
      }
    } catch (error) {
      console.error('保存利润报表-订单transaction失败:', error.message);
    }
  }

  /**
   * 将 start_date~end_date 拆分为日期列表（按天）
   */
  _getDaysBetween(startDateStr, endDateStr) {
    const start = new Date(startDateStr);
    const end = new Date(endDateStr);
    const days = [];
    const d = new Date(start);
    while (d <= end) {
      days.push(d.toISOString().slice(0, 10));
      d.setDate(d.getDate() + 1);
    }
    return days;
  }

  /**
   * 自动拉取利润报表-订单 transaction（按天拆分：若区间>1天则按天查询并逐天覆盖保存，默认最早前90天）
   */
  async fetchAllProfitReportOrdersTransaction(accountId, listParams = {}, options = {}) {
    const { pageSize = 1000, delayBetweenPages = 500, onProgress = null, defaultLookbackDays = 90 } = options;
    const actualPageSize = Math.min(pageSize, 1000);
    try {
      const startDate = listParams.start_date || listParams.startDate;
      const endDate = listParams.end_date || listParams.endDate;
      if (!startDate || !endDate) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
      const days = this._getDaysBetween(startDate, endDate);
      const searchDateField = listParams.searchDateField || listParams.search_date_field || 'posted_date_locale';
      let totalRecords = 0;

      for (let i = 0; i < days.length; i++) {
        const day = days[i];
        const dayParams = {
          ...listParams,
          startDate: day,
          endDate: day,
          searchDateField
        };
        const allRecords = [];
        let offset = 0;
        let totalCount = 0;
        let hasMore = true;
        while (hasMore) {
          const pageResult = await this.getProfitReportOrderTransactionList(accountId, {
            ...dayParams,
            offset,
            length: actualPageSize
          });
          const pageRecords = pageResult.records || [];
          const total = pageResult.total ?? pageRecords.length;
          if (offset === 0) totalCount = total;
          allRecords.push(...pageRecords);
          if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
          else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
          }
        }
        if (allRecords.length > 0) {
          await this.saveProfitReportOrderTransactions(accountId, allRecords, day);
          totalRecords += allRecords.length;
        }
        if (onProgress) onProgress(i + 1, days.length, day, totalRecords);
      }

      console.log(`利润报表-订单transaction拉取完成: ${startDate}~${endDate} 共 ${days.length} 天 ${totalRecords} 条`);
      return { data: totalRecords, total: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
    } catch (error) {
      console.error('自动拉取利润报表-订单transaction失败:', error.message);
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            feeTypeId: feeTypeIdStr,
            feeTypeName: feeType.name || feeType.fee_type_name || null,
            data: feeType,
            archived: false
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
            data: feeDetail,
            archived: false
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
            data: report,
            archived: false
          }
        });
      }

      console.log(`MSKU利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存MSKU利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存ASIN利润报表到数据库（按天覆盖：先删该日再插入）
   */
  async saveAsinProfitReportsForDay(accountId, reports, dataDate) {
    try {
      if (!prisma.lingXingAsinProfitReport) return;
      const day = String(dataDate).trim().slice(0, 10);
      await prisma.lingXingAsinProfitReport.updateMany({
        where: { accountId, dataDate: day },
        data: { archived: true, updatedAt: new Date() }
      });
      for (const report of reports) {
        await prisma.lingXingAsinProfitReport.create({
          data: {
            accountId,
            asin: report.asin || null,
            dataDate: day,
            data: report,
            archived: false
          }
        });
      }
      if (reports.length > 0) console.log(`ASIN利润报表已按天保存: dataDate=${day} 共 ${reports.length} 条`);
    } catch (error) {
      console.error('保存ASIN利润报表到数据库失败:', error.message);
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
            data: report,
            archived: false
          }
        });
      }

      console.log(`ASIN利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存ASIN利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存父ASIN利润报表到数据库（按天覆盖：先删该日再插入）
   */
  async saveParentAsinProfitReportsForDay(accountId, reports, dataDate) {
    try {
      if (!prisma.lingXingParentAsinProfitReport) return;
      const day = String(dataDate).trim().slice(0, 10);
      await prisma.lingXingParentAsinProfitReport.updateMany({
        where: { accountId, dataDate: day },
        data: { archived: true, updatedAt: new Date() }
      });
      for (const report of reports) {
        await prisma.lingXingParentAsinProfitReport.create({
          data: {
            accountId,
            parentAsin: report.parent_asin || null,
            dataDate: day,
            data: report,
            archived: false
          }
        });
      }
      if (reports.length > 0) console.log(`父ASIN利润报表已按天保存: dataDate=${day} 共 ${reports.length} 条`);
    } catch (error) {
      console.error('保存父ASIN利润报表到数据库失败:', error.message);
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
            data: report,
            archived: false
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
            data: report,
            archived: false
          }
        });
      }

      console.log(`SKU利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存SKU利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存店铺利润报表到数据库（按天覆盖：先删该日再插入）
   */
  async saveSellerProfitReportsForDay(accountId, reports, dataDate) {
    try {
      if (!prisma.lingXingSellerProfitReport) return;
      const day = String(dataDate).trim().slice(0, 10);
      await prisma.lingXingSellerProfitReport.updateMany({
        where: { accountId, dataDate: day },
        data: { archived: true, updatedAt: new Date() }
      });
      for (const report of reports) {
        await prisma.lingXingSellerProfitReport.create({
          data: {
            accountId,
            sid: report.sid != null ? parseInt(report.sid) : null,
            dataDate: day,
            data: report,
            archived: false
          }
        });
      }
      if (reports.length > 0) console.log(`店铺利润报表已按天保存: dataDate=${day} 共 ${reports.length} 条`);
    } catch (error) {
      console.error('保存店铺利润报表到数据库失败:', error.message);
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
            data: report,
            archived: false
          }
        });
      }

      console.log(`店铺利润报表已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存店铺利润报表到数据库失败:', error.message);
    }
  }

  /**
   * 保存FBA成本流到数据库（兼容全量拉取：从 record 取 shop_name/stream_date/unique_key）
   */
  async saveFbaCostStreams(accountId, streams) {
    try {
      if (!prisma.lingXingFbaCostStream) {
        console.error('Prisma Client 中未找到 lingXingFbaCostStream 模型');
        return;
      }

      let idx = 0;
      for (const stream of streams) {
        const shopName = (stream.shop_name != null && String(stream.shop_name).trim() !== '') ? String(stream.shop_name).trim() : '';
        const streamDate = (stream.stream_date || stream.action_date || stream.date || '').toString().trim().slice(0, 10);
        const uniqueKey = (stream.unique_key != null && String(stream.unique_key).trim() !== '') ? String(stream.unique_key).trim() : (stream.business_number != null ? String(stream.business_number) : '') || `gen_${idx}`;
        idx += 1;
        await prisma.lingXingFbaCostStream.upsert({
          where: {
            fba_cost_stream_account_shop_date_key: {
              accountId,
              shopName,
              streamDate,
              uniqueKey
            }
          },
          update: { data: stream, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            shopName: shopName || '',
            streamDate: streamDate || '',
            uniqueKey: uniqueKey || '',
            data: stream,
            archived: false
          }
        });
      }

      console.log(`FBA成本流已保存到数据库: 共 ${streams.length} 条记录`);
    } catch (error) {
      console.error('保存FBA成本流到数据库失败:', error.message);
      throw error;
    }
  }

  /**
   * 按日保存 FBA 成本流水（按 accountId + shopName + streamDate + uniqueKey upsert）
   */
  async saveFbaCostStreamForDay(accountId, shopName, streamDate, records) {
    if (!prisma.lingXingFbaCostStream || !records.length) return;
    const day = String(streamDate).trim().slice(0, 10);
    const shop = (shopName != null && String(shopName).trim() !== '') ? String(shopName).trim() : '';
    for (let i = 0; i < records.length; i++) {
      const row = records[i];
      const uniqueKey = (row.unique_key != null && String(row.unique_key).trim() !== '') ? String(row.unique_key).trim() : (row.business_number != null ? String(row.business_number) : '') || `row_${i}`;
      await prisma.lingXingFbaCostStream.upsert({
        where: {
          fba_cost_stream_account_shop_date_key: {
            accountId,
            shopName: shop,
            streamDate: day,
            uniqueKey
          }
        },
        update: { data: row, archived: false, updatedAt: new Date() },
        create: {
          accountId,
          shopName: shop || '',
          streamDate: day || '',
          uniqueKey: uniqueKey || '',
          data: row,
          archived: false
        }
      });
    }
  }

  /** FBA 成本流水接口必填的 business_types 默认全量类型（库存动作日期） */
  static getDefaultFbaCostStreamBusinessTypes() {
    return [1, 10, 11, 12, 13, 14, 20, 35, 25, 30, 31, 200, 201, 202, 205, 220, 15, 215, 225, 226, 227, 5, 210, 400, 420, 405];
  }

  /**
   * 按日拉取 FBA 成本流水：从 DB 遍历 shop_names，每天 start_date=end_date=day 查询并分页，按日保存（不跨月，按天即不跨月）
   */
  async fetchAllFbaCostStreamByDay(accountId, listParams = {}, options = {}) {
    const { pageSize = 200, delayBetweenPages = 500, onProgress = null } = options;
    const actualPageSize = Math.min(pageSize, 10000);
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const days = this._getDaysBetween(start, end);
    const shopNames = await this._getShopNamesForAccount(accountId);
    if (shopNames.length === 0) {
      console.log(`[fbaCostStream] accountId=${accountId} 无店铺名，跳过`);
      return { total: 0, data: 0, records: 0, stats: { daysProcessed: 0, totalRecords: 0 } };
    }

    const businessTypes = listParams.business_types || LingXingFinanceService.getDefaultFbaCostStreamBusinessTypes();
    const queryType = listParams.query_type || '01';
    let totalRecords = 0;
    let done = 0;
    const totalSteps = shopNames.length * days.length;

    for (const shopName of shopNames) {
      for (const day of days) {
        const allRecords = [];
        let offset = 0;
        let totalCount = 0;
        let hasMore = true;
        while (hasMore) {
          const res = await this.getFbaCostStream(accountId, {
            shop_names: [shopName],
            start_date: day,
            end_date: day,
            business_types: businessTypes,
            query_type: queryType,
            offset,
            length: actualPageSize
          });
          const pageRecords = res.data || [];
          const total = res.total ?? 0;
          if (offset === 0) totalCount = total;
          allRecords.push(...pageRecords);
          if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
          else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
          }
        }
        if (allRecords.length > 0) {
          await this.saveFbaCostStreamForDay(accountId, shopName, day, allRecords);
          totalRecords += allRecords.length;
        }
        done++;
        if (onProgress) onProgress(done, totalSteps, day, totalRecords);
      }
    }
    console.log(`FBA成本流水拉取完成: ${start}~${end} 店铺数=${shopNames.length} 共 ${totalRecords} 条`);
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
  }
  /**
   * 保存广告发票到数据库（唯一键：sid + invoice_id；可选拉取活动列表与基本信息并写入当前记录）
   * @param {string} accountId
   * @param {Array} invoices - 列表项需含 invoice_id 或 id、可选 sid
   * @param {Object} opts - sid: 本批统一 sid；fetchEnrichment: 是否拉取活动列表+基本信息并写入 data（默认 true）；delayBetweenEnrichment: 每条补充请求间隔 ms
   */
  async saveAdsInvoices(accountId, invoices, opts = {}) {
    try {
      if (!prisma.lingXingAdsInvoice) {
        console.error('Prisma Client 中未找到 lingXingAdsInvoice 模型');
        return;
      }

      const sidOverride = opts.sid !== undefined && opts.sid !== null ? parseInt(opts.sid) : null;
      const fetchEnrichment = opts.fetchEnrichment !== false;
      const delayBetweenEnrichment = opts.delayBetweenEnrichment ?? 150;

      for (let i = 0; i < invoices.length; i++) {
        const invoice = invoices[i];
        const sid = sidOverride !== null ? sidOverride : (invoice.sid !== undefined && invoice.sid !== null ? parseInt(invoice.sid) : 0);
        const invoiceId = (invoice.invoice_id != null ? String(invoice.invoice_id) : (invoice.id != null ? String(invoice.id) : '')).trim() || '';
        let dataToSave = { ...invoice };

        if (fetchEnrichment && invoiceId && (sid !== undefined && sid !== null)) {
          try {
            const [campaignList, detail] = await Promise.all([
              this.fetchAllAdsInvoiceCampaignListForInvoice(accountId, { invoice_id: invoiceId, sid }, { delayBetweenPages: delayBetweenEnrichment }),
              this.getAdsInvoiceDetail(accountId, { invoice_id: invoiceId, sid })
            ]);
            dataToSave = { ...invoice, campaignList: campaignList || [], detail: detail || null };
          } catch (err) {
            console.warn(`[adsInvoice] 拉取活动/详情失败 sid=${sid} invoice_id=${invoiceId}:`, err?.message || err);
          }
          if (delayBetweenEnrichment > 0 && i < invoices.length - 1) {
            await new Promise(r => setTimeout(r, delayBetweenEnrichment));
          }
        }

        await prisma.lingXingAdsInvoice.upsert({
          where: {
            accountId_sid_invoiceId: {
              accountId,
              sid,
              invoiceId
            }
          },
          update: { data: dataToSave, archived: false, updatedAt: new Date() },
          create: {
            accountId,
            sid,
            invoiceId,
            data: dataToSave,
            archived: false
          }
        });
      }

      console.log(`广告发票已保存到数据库: 共 ${invoices.length} 条记录`);
    } catch (error) {
      console.error('保存广告发票到数据库失败:', error.message);
      throw error;
    }
  }

  /**
   * 按日期范围拉取广告发票列表：支持多天查询，按 sid 遍历，每个 sid 用 invoice_start_time～invoice_end_time 一次查并分页；
   * 存储以 sid + invoice_id 为唯一键；保存时拉取广告发票活动列表和基本信息写入当前记录
   */
  async fetchAllAdsInvoiceListByDay(accountId, listParams = {}, options = {}) {
    const { pageSize = 100, delayBetweenPages = 500, onProgress = null, fetchEnrichment = true } = options;
    const actualPageSize = Math.min(pageSize, 10000);
    const start = listParams.start_date || listParams.startDate;
    const end = listParams.end_date || listParams.endDate;
    if (!start || !end) throw new Error('start_date/startDate 与 end_date/endDate 为必填');
    const sids = await this._getSidsForAccount(accountId);
    if (sids.length === 0) {
      console.log(`[adsInvoice] accountId=${accountId} 无店铺 sid，跳过`);
      return { total: 0, data: 0, records: 0, stats: { sidsProcessed: 0, totalRecords: 0 } };
    }

    let totalRecords = 0;
    let done = 0;

    for (const sid of sids) {
      const allRecords = [];
      let offset = 0;
      let totalCount = 0;
      let hasMore = true;
      while (hasMore) {
        const res = await this.getAdsInvoiceList(accountId, {
          invoice_start_time: start,
          invoice_end_time: end,
          sids: [sid],
          offset,
          length: actualPageSize
        });
        const pageRecords = res.data || [];
        const total = res.total ?? 0;
        if (offset === 0) totalCount = total;
        allRecords.push(...pageRecords);
        if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
        else {
          offset += actualPageSize;
          if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages));
        }
      }
      if (allRecords.length > 0) {
        await this.saveAdsInvoices(accountId, allRecords, { sid, fetchEnrichment });
        totalRecords += allRecords.length;
      }
      done++;
      if (onProgress) onProgress(done, sids.length, totalRecords);
    }
    console.log(`广告发票拉取完成: ${start}~${end} sid数=${sids.length} 共 ${totalRecords} 条`);
    return { total: totalRecords, data: totalRecords, records: totalRecords, stats: { sidsProcessed: sids.length, totalRecords } };
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: order.order_sn,
            data: order,
            archived: false
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: purchase.order_sn,
            data: purchase,
            archived: false
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: inbound.order_sn,
            data: inbound,
            archived: false
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: prepay.order_sn,
            data: prepay,
            archived: false
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            deliveryOrderSn: logistic.delivery_order_sn,
            data: logistic,
            archived: false
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            customFeeSn: customFee.custom_fee_sn,
            data: customFee,
            archived: false
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
            data: otherFee,
            archived: false
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
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            sid: parseInt(report.sid),
            settleMonth: report.settlementDate || report.settleMonth,
            data: report,
            archived: false
          }
        });
      }

      console.log(`应收报告已保存到数据库: 共 ${reports.length} 条记录`);
    } catch (error) {
      console.error('保存应收报告到数据库失败:', error.message);
    }
  }

  /**
   * 保存应收报告详情到数据库（upsert，支持增量重复写入同 (accountId,sid,settleMonth,fid) 覆盖）
   * @param {string} accountId
   * @param {Array} details - 详情列表，若接口未返回 sid 则用 context.sid（按 sid 遍历查询时传入）
   * @param {Object} [context] - 可选，{ sid, settleMonth } 当前查询维度，用于补全记录中缺失的 sid/settleMonth；sid 缺省时用 0 占位以满足唯一键非空
   */
  async saveReceivableReportDetails(accountId, details, context = {}) {
    try {
      if (!prisma.lingXingReceivableReportDetail) {
        console.error('Prisma Client 中未找到 lingXingReceivableReportDetail 模型');
        return;
      }

      for (const detail of details) {
        if (!detail.fid) continue;

        const sidVal = detail.sid != null ? parseInt(detail.sid) : (context.sid != null ? parseInt(context.sid) : 0);
        const settleMonthVal = (detail.settleMonth ?? context.settleMonth ?? '').toString().trim() || null;

        await prisma.lingXingReceivableReportDetail.upsert({
          where: {
            accountId_sid_settleMonth_fid: {
              accountId,
              sid: sidVal,
              settleMonth: settleMonthVal,
              fid: String(detail.fid)
            }
          },
          update: {
            data: detail,
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId,
            sid: sidVal,
            settleMonth: settleMonthVal,
            fid: String(detail.fid),
            data: detail,
            archived: false
          }
        });
      }

      console.log(`应收报告详情已保存到数据库: 共 ${details.length} 条记录`);
    } catch (error) {
      console.error('保存应收报告详情到数据库失败:', error.message);
    }
  }

  /**
   * 费用明细增量同步（优先按创建时间 date_type: gmt_create）
   */
  async incrementalSyncFeeDetails(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'feeDetail',
      { ...options, extraParams: { date_type: options.date_type || 'gmt_create' } },
      async (id, params, opts) => this.fetchAllFeeDetails(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 请款单增量同步（支持 search_field_time，不传则用接口默认；时间间隔最长 90 天）
   */
  async incrementalSyncRequestFundsOrders(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsOrder',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options, extraParams: { search_field_time: options.search_field_time } },
      async (id, params, opts) => this.fetchAllRequestFundsOrders(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 请款池-货款现结 增量同步（按 start_date/end_date，默认回退 90 天） */
  async incrementalSyncRequestFundsPoolPurchase(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsPoolPurchase',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllRequestFundsPoolPurchase(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 请款池-货款月结 增量同步 */
  async incrementalSyncRequestFundsPoolInbound(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsPoolInbound',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllRequestFundsPoolInbound(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 请款池-货款预付款 增量同步 */
  async incrementalSyncRequestFundsPoolPrepay(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsPoolPrepay',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllRequestFundsPoolPrepay(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 请款池-物流请款 增量同步 */
  async incrementalSyncRequestFundsPoolLogistics(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsPoolLogistics',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllRequestFundsPoolLogistics(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 请款池-其他应付款 增量同步 */
  async incrementalSyncRequestFundsPoolCustomFee(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsPoolCustomFee',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllRequestFundsPoolCustomFee(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 请款池-其他费用 增量同步 */
  async incrementalSyncRequestFundsPoolOtherFee(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'requestFundsPoolOtherFee',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllRequestFundsPoolOtherFee(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 结算中心-结算汇总 增量同步（按日保存，dateType 默认 1 结算结束时间，时间间隔最长 90 天） */
  async incrementalSyncSettlementSummary(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'settlementSummary',
      {
        defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90),
        ...options,
        extraParams: { dateType: options.dateType !== undefined ? Number(options.dateType) : 1 }
      },
      async (id, params, opts) => this.fetchAllSettlementSummaryList(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 结算中心-交易明细 增量同步（sid 从 DB 遍历，按日保存，每段最多 7 天） */
  async incrementalSyncSettlementTransactionDetail(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'settlementTransactionDetail',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 7, 90), ...options },
      async (id, params, opts) => this.fetchAllSettlementTransactionDetailList(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 库存分类账 detail 增量同步（sellerId 从 DB 遍历，按日查存） */
  async incrementalSyncInventoryLedgerDetail(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'inventoryLedgerDetail',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllInventoryLedgerDetailList(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 库存分类账 summary 增量同步（sellerId 从 DB 遍历，按日查存） */
  async incrementalSyncInventoryLedgerSummary(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'inventoryLedgerSummary',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllInventoryLedgerSummaryList(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 发货结算报告 增量同步（sid+amazonSellerId 从 DB 遍历，timeType=06 更新时间，按日查存） */
  async incrementalSyncSettlementReport(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'settlementReport',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options, extraParams: { timeType: '06' } },
      async (id, params, opts) => this.fetchAllSettlementReportList(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** FBA 成本流水 增量同步（shopName 从 DB 遍历，按日查存） */
  async incrementalSyncFbaCostStream(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'fbaCostStream',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllFbaCostStreamByDay(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /** 广告发票 增量同步（sid 从 DB 遍历，按日查存） */
  async incrementalSyncAdsInvoice(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'adsInvoice',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllAdsInvoiceListByDay(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润报表-订单增量同步（按 search_date_field 时间维度，默认 posted_date_locale 结算时间）
   */
  async incrementalSyncProfitReportOrders(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'profitReportOrder',
      {
        defaultLookbackDays: options.defaultLookbackDays ?? 31,
        ...options,
        extraParams: {
          search_date_field: options.search_date_field || 'posted_date_locale'
        }
      },
      async (id, params, opts) => this.fetchAllProfitReportOrders(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润报表-订单维度 transaction 增量同步（按天拆分、按天覆盖，默认最早前90天）
   */
  async incrementalSyncProfitReportOrderTransaction(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'profitReportOrderTransaction',
      {
        defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90),
        ...options,
        extraParams: {
          searchDateField: options.searchDateField || 'posted_date_locale'
        }
      },
      async (id, params, opts) => this.fetchAllProfitReportOrdersTransaction(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润报表-ASIN 增量同步（按天拆分、按天覆盖，默认回退90天）
   */
  async incrementalSyncAsinProfitReport(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'asinProfitReport',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllAsinProfitReportsByDayRange(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润报表-父ASIN 增量同步（按天拆分、按天覆盖，默认回退90天）
   */
  async incrementalSyncParentAsinProfitReport(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'parentAsinProfitReport',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllParentAsinProfitReportsByDayRange(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润报表-店铺 增量同步（按天拆分、按天覆盖，默认回退90天）
   */
  async incrementalSyncSellerProfitReport(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'sellerProfitReport',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllSellerProfitReportsByDayRange(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

}

export default new LingXingFinanceService();

