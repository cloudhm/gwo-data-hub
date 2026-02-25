import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';
import lingxingBasicDataService from '../basic/lingxingBasicDataService.js';
import { runAccountLevelIncrementalSync } from '../sync/lingXingIncrementalRunner.js';

/**
 * 领星ERP报表服务
 * 销量、订单量、销售额报表相关接口
 */
class LingXingReportService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询销量、订单量、销售额报表（按ASIN或MSKU）
   * API: POST /erp/sc/data/sales_report/asinDailyLists
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id，对应查询亚马逊店铺列表接口对应字段【sid】（必填）
   *   - event_date: 报表时间【站点时间】，格式：Y-m-d（必填）
   *   - asin_type: 查询维度：1=asin, 2=msku（可选，默认1）
   *   - type: 类型：1=销售额, 2=销量, 3=订单量（可选，默认1）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认1000（可选）
   * @returns {Promise<Object>} 报表数据 { data: [], total: 0 }
   */
  async getSalesReport(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.sid === undefined || params.sid === null) {
        throw new Error('sid 为必填参数');
      }
      if (!params.event_date) {
        throw new Error('event_date 为必填参数');
      }

      // 验证日期格式（Y-m-d）
      const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRegex.test(params.event_date)) {
        throw new Error('event_date 格式错误，应为 Y-m-d 格式，例如：2024-08-05');
      }

      // 构建请求参数
      const requestParams = {
        sid: parseInt(params.sid),
        event_date: params.event_date,
        ...(params.asin_type !== undefined && { asin_type: parseInt(params.asin_type) }),
        ...(params.type !== undefined && { type: parseInt(params.type) }),
        ...(params.offset !== undefined && { offset: parseInt(params.offset) }),
        ...(params.length !== undefined && { length: parseInt(params.length) })
      };

      // 调用API获取报表数据（使用通用客户端，成功码为0，令牌桶容量为5）
      const response = await this.post(account, '/erp/sc/data/sales_report/asinDailyLists', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取销量报表失败');
      }

      const reportData = response.data || [];
      const total = response.total || reportData.length;

      // 保存报表数据到数据库
      if (reportData.length > 0) {
        await this.saveSalesReport(accountId, {
          sid: requestParams.sid,
          event_date: requestParams.event_date,
          asin_type: requestParams.asin_type || 1,
          type: requestParams.type || 1
        }, reportData);
      }

      return {
        data: reportData,
        total: total
      };
    } catch (error) {
      console.error('获取销量报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 批量查询多天的销量报表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id（可选，如果不传则查询所有店铺）
   *   - start_date: 开始日期，格式：Y-m-d（必填）
   *   - end_date: 结束日期，格式：Y-m-d（必填）
   *   - asin_type: 查询维度：1=asin, 2=msku（可选，不传则遍历1和2）
   *   - type: 类型：1=销售额, 2=销量, 3=订单量（可选，不传则遍历1、2、3）
   *   - pageSize: 每页大小（可选，默认1000）
   *   - delayBetweenDays: 每天之间的延迟时间（毫秒，默认500）
   *   - delayBetweenShops: 店铺之间的延迟时间（毫秒，默认1000）
   *   - delayBetweenTypes: 类型组合之间的延迟时间（毫秒，默认500）
   * @returns {Promise<Object>} 报表数据 { data: [], total: 0, stats: {} }
   */
  async getSalesReportByDateRange(accountId, params = {}) {
    const {
      pageSize = 1000,
      delayBetweenDays = 500,
      delayBetweenShops = 1000,
      delayBetweenTypes = 500
    } = params;

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

      // 验证日期格式
      const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRegex.test(params.start_date) || !dateRegex.test(params.end_date)) {
        throw new Error('日期格式错误，应为 Y-m-d 格式，例如：2024-08-05');
      }

      // 解析日期范围
      const startDate = new Date(params.start_date);
      const endDate = new Date(params.end_date);

      if (startDate > endDate) {
        throw new Error('start_date 不能大于 end_date');
      }

      // 如果 sid 未传，获取所有店铺
      let shopList = [];
      if (params.sid === undefined || params.sid === null) {
        console.log('sid 未传，自动获取所有店铺列表...');
        try {
          const sellers = await lingxingBasicDataService.getSellerLists(accountId, true);
          shopList = sellers.map(seller => ({
            sid: seller.sid,
            name: seller.name || seller.account_name || `店铺${seller.sid}`
          }));
          console.log(`获取到 ${shopList.length} 个店铺，将依次同步`);
        } catch (error) {
          console.error('获取店铺列表失败:', error.message);
          throw new Error(`获取店铺列表失败: ${error.message}`);
        }
      } else {
        // 如果传了 sid，只查询该店铺
        shopList = [{
          sid: params.sid,
          name: `店铺${params.sid}`
        }];
      }

      // 确定需要遍历的 asin_type 和 type 组合
      const asinTypes = params.asin_type !== undefined && params.asin_type !== null 
        ? [parseInt(params.asin_type)] 
        : [1, 2]; // 1=asin, 2=msku
      
      const types = params.type !== undefined && params.type !== null 
        ? [parseInt(params.type)] 
        : [1, 2, 3]; // 1=销售额, 2=销量, 3=订单量

      // 生成所有组合
      const typeCombinations = [];
      for (const asinType of asinTypes) {
        for (const type of types) {
          typeCombinations.push({
            asin_type: asinType,
            type: type,
            name: `asin_type=${asinType}(${asinType === 1 ? 'asin' : 'msku'}), type=${type}(${type === 1 ? '销售额' : type === 2 ? '销量' : '订单量'})`
          });
        }
      }

      console.log(`\n========== 开始批量同步 ==========`);
      console.log(`店铺数量: ${shopList.length}`);
      console.log(`类型组合数量: ${typeCombinations.length}`);
      console.log(`日期范围: ${params.start_date} 至 ${params.end_date}`);
      console.log(`预计总查询次数: ${shopList.length} 店铺 × ${typeCombinations.length} 组合 × ${Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1} 天`);

      const allReportData = [];
      const stats = {
        totalShops: shopList.length,
        successShops: 0,
        failedShops: 0,
        totalCombinations: typeCombinations.length,
        successCombinations: 0,
        failedCombinations: 0,
        totalDays: 0,
        successDays: 0,
        failedDays: 0,
        totalRecords: 0
      };

      // 遍历每个店铺
      for (let shopIndex = 0; shopIndex < shopList.length; shopIndex++) {
        const shop = shopList[shopIndex];
        const shopSid = shop.sid;
        const shopName = shop.name;

        console.log(`\n========== 开始同步店铺 ${shopIndex + 1}/${shopList.length}: ${shopName} (sid: ${shopSid}) ==========`);

        try {
          // 遍历每个类型组合
          for (let comboIndex = 0; comboIndex < typeCombinations.length; comboIndex++) {
            const combo = typeCombinations[comboIndex];
            
            console.log(`\n[${shopName}] 开始同步组合 ${comboIndex + 1}/${typeCombinations.length}: ${combo.name}`);

            try {
              // 遍历每一天
              const currentDate = new Date(startDate);
              let comboTotalRecords = 0;
              let comboSuccessDays = 0;
              let comboFailedDays = 0;

              while (currentDate <= endDate) {
                const dateStr = currentDate.toISOString().split('T')[0]; // Y-m-d 格式
                stats.totalDays++;

                try {
                  console.log(`[${shopName}][${combo.name}] 正在查询日期：${dateStr}`);

                  // 获取当天的数据（自动处理分页）
                  let offset = 0;
                  let hasMore = true;
                  const dayData = [];

                  while (hasMore) {
                    const pageResult = await this.getSalesReport(accountId, {
                      sid: shopSid,
                      event_date: dateStr,
                      asin_type: combo.asin_type,
                      type: combo.type,
                      offset: offset,
                      length: pageSize
                    });

                    const pageData = pageResult.data || [];
                    dayData.push(...pageData);

                    // 判断是否还有更多数据
                    if (pageData.length < pageSize) {
                      hasMore = false;
                    } else {
                      offset += pageSize;
                      // 分页之间延迟
                      if (delayBetweenDays > 0) {
                        await new Promise(resolve => setTimeout(resolve, 100));
                      }
                    }
                  }

                  allReportData.push(...dayData);
                  comboTotalRecords += dayData.length;
                  comboSuccessDays++;
                  stats.successDays++;
                  stats.totalRecords += dayData.length;
                  console.log(`[${shopName}][${combo.name}] 日期 ${dateStr} 查询完成，获取 ${dayData.length} 条记录`);

                  // 立即保存当天的数据
                  if (dayData.length > 0) {
                    await this.saveSalesReport(accountId, {
                      sid: shopSid,
                      event_date: dateStr,
                      asin_type: combo.asin_type,
                      type: combo.type
                    }, dayData);
                  }

                  // 每天之间延迟
                  if (currentDate < endDate && delayBetweenDays > 0) {
                    await new Promise(resolve => setTimeout(resolve, delayBetweenDays));
                  }
                } catch (error) {
                  comboFailedDays++;
                  stats.failedDays++;
                  console.error(`[${shopName}][${combo.name}] 日期 ${dateStr} 查询失败:`, error.message);
                }

                // 移动到下一天
                currentDate.setDate(currentDate.getDate() + 1);
              }

              console.log(`[${shopName}][${combo.name}] 同步完成：成功 ${comboSuccessDays} 天，失败 ${comboFailedDays} 天，共 ${comboTotalRecords} 条记录`);
              stats.successCombinations++;

              // 类型组合之间延迟
              if (comboIndex < typeCombinations.length - 1 && delayBetweenTypes > 0) {
                await new Promise(resolve => setTimeout(resolve, delayBetweenTypes));
              }
            } catch (error) {
              stats.failedCombinations++;
              console.error(`[${shopName}][${combo.name}] 同步失败:`, error.message);
            }
          }

          console.log(`[${shopName}] 所有组合同步完成`);
          stats.successShops++;

          // 店铺之间延迟
          if (shopIndex < shopList.length - 1 && delayBetweenShops > 0) {
            console.log(`等待 ${delayBetweenShops}ms 后继续下一个店铺...`);
            await new Promise(resolve => setTimeout(resolve, delayBetweenShops));
          }
        } catch (error) {
          stats.failedShops++;
          console.error(`[${shopName}] 同步失败:`, error.message);
        }
      }

      console.log(`\n========== 批量查询完成 ==========`);
      console.log(`店铺统计：成功 ${stats.successShops}/${stats.totalShops} 个，失败 ${stats.failedShops} 个`);
      console.log(`类型组合统计：成功 ${stats.successCombinations}/${stats.totalCombinations} 个，失败 ${stats.failedCombinations} 个`);
      console.log(`日期统计：成功 ${stats.successDays} 天，失败 ${stats.failedDays} 天`);
      console.log(`总记录数：${stats.totalRecords} 条`);

      return {
        data: allReportData,
        total: allReportData.length,
        stats: stats
      };
    } catch (error) {
      console.error('批量查询销量报表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存销量报表数据到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Object} reportParams - 报表参数
   *   - sid: 店铺ID
   *   - event_date: 报表日期
   *   - asin_type: 查询维度
   *   - type: 类型
   * @param {Array} reportData - 报表数据数组
   */
  async saveSalesReport(accountId, reportParams, reportData) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingSalesReport || !prisma.lingXingSalesReportItem) {
        console.error('Prisma Client 中未找到销量报表模型，请重新生成 Prisma Client 并重启服务器');
        return;
      }

      const { sid, event_date, asin_type, type } = reportParams;

      // 创建或更新主表记录
      const report = await prisma.lingXingSalesReport.upsert({
        where: {
          sid_eventDate_asinType_type: {
            sid: sid,
            eventDate: event_date,
            asinType: asin_type || 1,
            type: type || 1
          }
        },
        update: {
          totalRecords: reportData.length,
          archived: false,
          updatedAt: new Date()
        },
        create: {
          accountId: accountId,
          sid: sid,
          eventDate: event_date,
          asinType: asin_type || 1,
          type: type || 1,
          totalRecords: reportData.length,
          archived: false
        }
      });

      console.log(`销量报表主表已保存/更新: sid=${sid}, event_date=${event_date}, asin_type=${asin_type}, type=${type}`);

      // 保存明细数据
      for (const item of reportData) {
        const asin = item.asin || null;
        const sellerSku = item.seller_sku || null;

        // 如果 asin 和 seller_sku 都为空，跳过
        if (!asin && !sellerSku) {
          console.warn('报表明细数据缺少 asin 和 seller_sku，跳过保存:', item);
          continue;
        }

        // 对于唯一键，如果值为 null，使用空字符串（Prisma 唯一键不支持 null）
        // 但根据业务逻辑，在同一个报表中，asin 和 seller_sku 至少有一个有值
        // 为了保持一致性，在存储时也使用空字符串代替 null
        const asinValue = asin || '';
        const sellerSkuValue = sellerSku || '';
        
        await prisma.lingXingSalesReportItem.upsert({
          where: {
            reportId_asin_sellerSku: {
              reportId: report.id,
              asin: asinValue,
              sellerSku: sellerSkuValue
            }
          },
          update: {
            sid: item.sid !== undefined && item.sid !== null ? parseInt(item.sid) : sid,
            rDate: item.r_date || event_date,
            currencyCode: item.currency_code || null,
            sellerSku: sellerSkuValue, // 使用处理后的值，保持与唯一键一致
            asin: asinValue, // 使用处理后的值，保持与唯一键一致
            productName: item.product_name || null,
            mapValue: item.map_value !== undefined && item.map_value !== null && item.map_value !== '' 
              ? parseFloat(item.map_value) 
              : null,
            data: item, // 保存完整数据
            archived: false,
            updatedAt: new Date()
          },
          create: {
            reportId: report.id,
            sid: item.sid !== undefined && item.sid !== null ? parseInt(item.sid) : sid,
            rDate: item.r_date || event_date,
            currencyCode: item.currency_code || null,
            sellerSku: sellerSkuValue, // 使用处理后的值，保持与唯一键一致
            asin: asinValue, // 使用处理后的值，保持与唯一键一致
            productName: item.product_name || null,
            mapValue: item.map_value !== undefined && item.map_value !== null && item.map_value !== '' 
              ? parseFloat(item.map_value) 
              : null,
            data: item, // 保存完整数据
            archived: false
          }
        });
      }

      console.log(`销量报表明细已保存: 共 ${reportData.length} 条记录`);
    } catch (error) {
      console.error('保存销量报表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 查询产品表现
   * API: POST /bd/productPerformance/openApi/asinList
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（必填）
   *   - length: 分页长度，最大10000（必填）
   *   - sort_field: 排序字段（必填，默认volume）
   *   - sort_type: 排序方式：desc/asc（必填，默认desc）
   *   - search_field: 搜索字段（可选）
   *   - search_value: 搜索值，最多批量搜索50个（可选）
   *   - mid: 站点id（可选）
   *   - sid: 店铺id，单店铺传字符串，多店铺传数组，上限200（必填）
   *   - start_date: 开始日期，格式：YYYY-MM-DD（必填）
   *   - end_date: 结束日期，格式：YYYY-MM-DD（必填，注意：建议与start_date相同）
   *   - extend_search: 表头筛选（可选）
   *   - summary_field: 汇总行维度：asin/parent_asin/msku/sku（必填）
   *   - currency_code: 货币类型（可选）
   *   - is_recently_enum: 是否仅查询活跃商品（可选，默认true）
   *   - purchase_status: 退货退款统计方式（可选，默认0）
   * @returns {Promise<Object>} 产品表现数据 { total: 0, list: [], chain_start_date, chain_end_date }
   */
  async getProductPerformance(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.offset === undefined || params.offset === null) {
        throw new Error('offset 为必填参数');
      }
      if (params.length === undefined || params.length === null) {
        throw new Error('length 为必填参数');
      }
      if (!params.sort_field) {
        throw new Error('sort_field 为必填参数');
      }
      if (!params.sort_type) {
        throw new Error('sort_type 为必填参数');
      }
      if (params.sid === undefined || params.sid === null) {
        throw new Error('sid 为必填参数');
      }
      if (!params.start_date || !params.end_date) {
        throw new Error('start_date 和 end_date 为必填参数');
      }
      // summary_field 在单次查询时是必填的，但在批量查询中会由调用方传入
      if (!params.summary_field) {
        throw new Error('summary_field 为必填参数');
      }

      // 验证日期格式
      const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRegex.test(params.start_date) || !dateRegex.test(params.end_date)) {
        throw new Error('日期格式错误，应为 YYYY-MM-DD 格式，例如：2024-08-01');
      }

      // 构建请求参数
      const requestParams = {
        offset: parseInt(params.offset),
        length: parseInt(params.length),
        sort_field: params.sort_field,
        sort_type: params.sort_type,
        sid: params.sid, // 可以是字符串或数组
        start_date: params.start_date,
        end_date: params.end_date,
        summary_field: params.summary_field,
        ...(params.search_field && { search_field: params.search_field }),
        ...(params.search_value && { search_value: params.search_value }),
        ...(params.mid !== undefined && { mid: parseInt(params.mid) }),
        ...(params.extend_search && { extend_search: params.extend_search }),
        ...(params.currency_code && { currency_code: params.currency_code }),
        ...(params.is_recently_enum !== undefined && { is_recently_enum: params.is_recently_enum }),
        ...(params.purchase_status !== undefined && { purchase_status: parseInt(params.purchase_status) })
      };

      // 调用API获取产品表现数据（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/bd/productPerformance/openApi/asinList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取产品表现失败');
      }

      const data = response.data || {};
      const list = data.list || [];
      const total = data.total || 0;

      // 保存产品表现数据到数据库（保存当前页的数据）
      if (list.length > 0 || data) {
        await this.saveProductPerformance(accountId, {
          sid: params.sid,
          start_date: params.start_date,
          end_date: params.end_date,
          summary_field: params.summary_field,
          offset: params.offset || 0,
          length: params.length || 10000
        }, {
          list: list,
          total: total,
          chain_start_date: data.chain_start_date,
          chain_end_date: data.chain_end_date,
          available_inventory_formula_zh: data.available_inventory_formula_zh
        });
      }

      return {
        total: total,
        list: list,
        chain_start_date: data.chain_start_date,
        chain_end_date: data.chain_end_date,
        available_inventory_formula_zh: data.available_inventory_formula_zh
      };
    } catch (error) {
      console.error('获取产品表现失败:', error.message);
      throw error;
    }
  }

  /**
   * 批量查询产品表现（自动处理分页和限流）
   * 注意：根据限流要求，单店铺间隔1s，多店铺间隔10s
   * 注意：每次只查询一天的数据，如果日期范围跨多天，会按天遍历
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（同 getProductPerformance）
   *   - sid: 店铺id（可选，不传则查询所有店铺）
   *   - summary_field: 汇总行维度（可选，不传则遍历所有：asin/parent_asin/msku/sku）
   *   - pageSize: 每页大小（可选，默认10000，最大10000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认1000）
   *   - delayBetweenSummaryFields: summary_field之间的延迟时间（毫秒，默认1000）
   *   - delayBetweenShops: 店铺之间的延迟时间（毫秒，默认1000）
   *   - delayBetweenDays: 每天之间的延迟时间（毫秒，默认1000）
   * @returns {Promise<Object>} 产品表现数据 { data: [], total: 0, stats: {} }
   */
  async fetchAllProductPerformance(accountId, params = {}) {
    const {
      pageSize = 10000,
      delayBetweenPages = 1000,
      delayBetweenSummaryFields = 1000,
      delayBetweenShops = 1000,
      delayBetweenDays = 1000
    } = params;

    try {
      // 验证必填参数
      if (!params.start_date || !params.end_date) {
        throw new Error('start_date 和 end_date 为必填参数');
      }

      // 验证日期格式
      const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRegex.test(params.start_date) || !dateRegex.test(params.end_date)) {
        throw new Error('日期格式错误，应为 YYYY-MM-DD 格式，例如：2024-08-01');
      }

      // 解析日期范围
      const startDate = new Date(params.start_date);
      const endDate = new Date(params.end_date);

      if (startDate > endDate) {
        throw new Error('start_date 不能大于 end_date');
      }

      // 如果 sid 未传，获取所有店铺
      let shopList = [];
      if (params.sid === undefined || params.sid === null) {
        console.log('sid 未传，自动获取所有店铺列表...');
        try {
          const sellers = await lingxingBasicDataService.getSellerLists(accountId, true);
          shopList = sellers.map(seller => ({
            sid: seller.sid,
            name: seller.name || seller.account_name || `店铺${seller.sid}`
          }));
          console.log(`获取到 ${shopList.length} 个店铺，将依次同步`);
        } catch (error) {
          console.error('获取店铺列表失败:', error.message);
          throw new Error(`获取店铺列表失败: ${error.message}`);
        }
      } else {
        // 如果传了 sid，处理为数组格式
        const sidArray = Array.isArray(params.sid) ? params.sid : [params.sid];
        shopList = sidArray.map(sid => ({
          sid: sid,
          name: `店铺${sid}`
        }));
      }

      // 确定需要遍历的 summary_field 值
      const summaryFields = params.summary_field 
        ? [params.summary_field] 
        : ['asin', 'parent_asin', 'msku', 'sku']; // 遍历所有维度

      console.log(`\n========== 开始批量查询产品表现 ==========`);
      console.log(`店铺数量: ${shopList.length}`);
      console.log(`日期范围: ${params.start_date} 至 ${params.end_date}`);
      console.log(`汇总维度: ${summaryFields.join(', ')} (共 ${summaryFields.length} 个)`);
      
      // 计算日期范围天数
      const daysDiff = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;
      console.log(`预计总查询次数: ${shopList.length} 店铺 × ${summaryFields.length} 维度 × ${daysDiff} 天`);

      const allData = [];
      const stats = {
        totalShops: shopList.length,
        successShops: 0,
        failedShops: 0,
        totalSummaryFields: summaryFields.length,
        successSummaryFields: 0,
        failedSummaryFields: 0,
        totalDays: 0,
        successDays: 0,
        failedDays: 0,
        totalPages: 0,
        successPages: 0,
        failedPages: 0,
        totalRecords: 0
      };

      // 遍历每个店铺
      for (let shopIndex = 0; shopIndex < shopList.length; shopIndex++) {
        const shop = shopList[shopIndex];
        const shopSid = shop.sid;
        const shopName = shop.name;

        console.log(`\n========== 开始同步店铺 ${shopIndex + 1}/${shopList.length}: ${shopName} (sid: ${shopSid}) ==========`);

        try {
          // 判断是单店铺还是多店铺（这里每个店铺单独查询，所以都是单店铺模式）
          const delayAfterRequest = 1000; // 单店铺1s

          // 遍历每个 summary_field
          for (let fieldIndex = 0; fieldIndex < summaryFields.length; fieldIndex++) {
            const summaryField = summaryFields[fieldIndex];
            
            console.log(`\n[${shopName}][${summaryField}] 开始同步维度 ${fieldIndex + 1}/${summaryFields.length}`);

            try {
              // 遍历每一天（每次只查询一天的数据）
              const currentDate = new Date(startDate);
              let fieldTotalRecords = 0;
              let fieldSuccessDays = 0;
              let fieldFailedDays = 0;

              while (currentDate <= endDate) {
                const dateStr = currentDate.toISOString().split('T')[0]; // YYYY-MM-DD 格式
                stats.totalDays++;

                try {
                  console.log(`[${shopName}][${summaryField}] 正在查询日期：${dateStr}`);

                  let offset = 0;
                  const actualPageSize = Math.min(pageSize, 10000); // 最大10000
                  let hasMore = true;
                  let lastRequestTime = 0;
                  let dayTotalRecords = 0;

                  // 自动分页获取当天的所有数据
                  while (hasMore) {
                    stats.totalPages++;

                    try {
                      // 计算距离上次请求的时间，确保满足限流要求
                      const timeSinceLastRequest = Date.now() - lastRequestTime;
                      if (timeSinceLastRequest < delayAfterRequest) {
                        const waitTime = delayAfterRequest - timeSinceLastRequest;
                        console.log(`[${shopName}][${summaryField}][${dateStr}] 等待 ${waitTime}ms 以满足限流要求...`);
                        await new Promise(resolve => setTimeout(resolve, waitTime));
                      }

                      console.log(`[${shopName}][${summaryField}][${dateStr}] 正在获取第 ${stats.totalPages} 页（offset: ${offset}, length: ${actualPageSize}）...`);

                      // 构建查询参数，确保所有必填参数都有值
                      // 注意：start_date 和 end_date 都设置为同一天
                      const queryParams = {
                        sid: shopSid, // 使用当前店铺的sid
                        summary_field: summaryField, // 使用当前维度
                        offset: offset,
                        length: actualPageSize,
                        sort_field: params.sort_field || 'volume',
                        sort_type: params.sort_type || 'desc',
                        start_date: dateStr, // 每次只查询一天
                        end_date: dateStr, // 每次只查询一天
                        // 可选参数
                        ...(params.search_field && { search_field: params.search_field }),
                        ...(params.search_value && { search_value: params.search_value }),
                        ...(params.mid !== undefined && { mid: params.mid }),
                        ...(params.extend_search && { extend_search: params.extend_search }),
                        ...(params.currency_code && { currency_code: params.currency_code }),
                        ...(params.is_recently_enum !== undefined && { is_recently_enum: params.is_recently_enum }),
                        ...(params.purchase_status !== undefined && { purchase_status: params.purchase_status })
                      };

                      const pageResult = await this.getProductPerformance(accountId, queryParams);

                      const pageData = pageResult.list || [];
                      allData.push(...pageData);

                      stats.successPages++;
                      dayTotalRecords += pageData.length;
                      fieldTotalRecords += pageData.length;
                      stats.totalRecords += pageData.length;
                      lastRequestTime = Date.now();

                      console.log(`[${shopName}][${summaryField}][${dateStr}] 第 ${stats.totalPages} 页获取完成，本页 ${pageData.length} 条记录，累计 ${dayTotalRecords} 条记录`);

                      // 判断是否还有更多数据
                      if (pageData.length < actualPageSize || dayTotalRecords >= pageResult.total) {
                        hasMore = false;
                      } else {
                        offset += actualPageSize;
                        // 分页之间延迟
                        if (delayBetweenPages > 0) {
                          await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
                        }
                      }

                      // 请求后延迟（满足限流要求）
                      if (hasMore && delayAfterRequest > 0) {
                        await new Promise(resolve => setTimeout(resolve, delayAfterRequest));
                      }
                    } catch (error) {
                      stats.failedPages++;
                      console.error(`[${shopName}][${summaryField}][${dateStr}] 第 ${stats.totalPages} 页获取失败:`, error.message);
                      
                      // 如果连续失败，停止分页
                      if (stats.failedPages > 3) {
                        console.error(`[${shopName}][${summaryField}][${dateStr}] 连续失败次数过多，停止分页`);
                        hasMore = false;
                      } else {
                        // 失败后也要等待，满足限流要求
                        if (delayAfterRequest > 0) {
                          await new Promise(resolve => setTimeout(resolve, delayAfterRequest));
                        }
                      }
                    }
                  }

                  console.log(`[${shopName}][${summaryField}][${dateStr}] 查询完成：共 ${dayTotalRecords} 条记录`);
                  fieldSuccessDays++;
                  stats.successDays++;

                  // 每天之间延迟
                  if (currentDate < endDate && delayBetweenDays > 0) {
                    await new Promise(resolve => setTimeout(resolve, delayBetweenDays));
                  }
                } catch (error) {
                  fieldFailedDays++;
                  stats.failedDays++;
                  console.error(`[${shopName}][${summaryField}][${dateStr}] 查询失败:`, error.message);
                }

                // 移动到下一天
                currentDate.setDate(currentDate.getDate() + 1);
              }

              console.log(`[${shopName}][${summaryField}] 同步完成：成功 ${fieldSuccessDays} 天，失败 ${fieldFailedDays} 天，共 ${fieldTotalRecords} 条记录`);
              stats.successSummaryFields++;

              // summary_field 之间延迟
              if (fieldIndex < summaryFields.length - 1 && delayBetweenSummaryFields > 0) {
                await new Promise(resolve => setTimeout(resolve, delayBetweenSummaryFields));
              }
            } catch (error) {
              stats.failedSummaryFields++;
              console.error(`[${shopName}][${summaryField}] 同步失败:`, error.message);
            }
          }

          console.log(`[${shopName}] 所有组合同步完成`);
          stats.successShops++;

          // 店铺之间延迟
          if (shopIndex < shopList.length - 1 && delayBetweenShops > 0) {
            console.log(`等待 ${delayBetweenShops}ms 后继续下一个店铺...`);
            await new Promise(resolve => setTimeout(resolve, delayBetweenShops));
          }
        } catch (error) {
          stats.failedShops++;
          console.error(`[${shopName}] 同步失败:`, error.message);
        }
      }

      console.log(`\n========== 批量查询完成 ==========`);
      console.log(`店铺统计：成功 ${stats.successShops}/${stats.totalShops} 个，失败 ${stats.failedShops} 个`);
      console.log(`维度统计：成功 ${stats.successSummaryFields}/${stats.totalSummaryFields * stats.totalShops} 个，失败 ${stats.failedSummaryFields} 个`);
      console.log(`日期统计：成功 ${stats.successDays} 天，失败 ${stats.failedDays} 天`);
      console.log(`分页统计：成功 ${stats.successPages} 页，失败 ${stats.failedPages} 页`);
      console.log(`总记录数：${stats.totalRecords} 条`);

      return {
        data: allData,
        total: allData.length,
        stats: stats
      };
    } catch (error) {
      console.error('批量查询产品表现失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存产品表现数据到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Object} reportParams - 报表参数
   *   - sid: 店铺ID（可以是字符串或数组）
   *   - start_date: 开始日期
   *   - end_date: 结束日期
   *   - summary_field: 汇总行维度
   *   - offset: 分页偏移量
   *   - length: 分页长度
   * @param {Object} pageData - 分页数据对象
   *   - list: 数据列表数组
   *   - total: 总记录数
   *   - chain_start_date: 环比开始日期
   *   - chain_end_date: 环比结束日期
   *   - available_inventory_formula_zh: 可用库存计算公式
   */
  async saveProductPerformance(accountId, reportParams, pageData) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingProductPerformance || !prisma.lingXingProductPerformancePage) {
        console.error('Prisma Client 中未找到产品表现模型，请重新生成 Prisma Client 并重启服务器');
        return;
      }

      const { sid, start_date, end_date, summary_field, offset = 0, length = 10000 } = reportParams;

      // 处理 sid（可能是字符串或数组）
      let sidArray = Array.isArray(sid) ? sid : [sid];
      // 对数组进行排序，确保相同的内容产生相同的 sidString
      sidArray = sidArray.map(s => parseInt(s)).sort((a, b) => a - b);
      const sidString = sidArray.join(',');

      // 计算页码（从0开始）
      const pageNumber = Math.floor(offset / length);

      // 创建或更新主表记录（记录查询条件）
      // 使用 try-catch 处理并发情况下的唯一约束冲突
      let performance;
      try {
        performance = await prisma.lingXingProductPerformance.upsert({
          where: {
            accountId_sidString_startDate_endDate_summaryField: {
              accountId: accountId,
              sidString: sidString,
              startDate: start_date,
              endDate: end_date,
              summaryField: summary_field
            }
          },
          update: {
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            sidString: sidString,
            startDate: start_date,
            endDate: end_date,
            summaryField: summary_field,
            archived: false
          }
        });
      } catch (error) {
        // 如果是唯一约束冲突（并发情况），尝试重新查询
        if (error.code === 'P2002') {
          console.log('检测到唯一约束冲突，重新查询主表记录...');
          performance = await prisma.lingXingProductPerformance.findUnique({
            where: {
              accountId_sidString_startDate_endDate_summaryField: {
                accountId: accountId,
                sidString: sidString,
                startDate: start_date,
                endDate: end_date,
                summaryField: summary_field
              }
            }
          });

          if (!performance) {
            // 如果查询不到，可能是其他错误，抛出异常
            throw new Error('无法创建或查询主表记录');
          }

          // 更新主表记录（只更新 updatedAt）
          performance = await prisma.lingXingProductPerformance.update({
            where: { id: performance.id },
            data: {
              updatedAt: new Date()
            }
          });
        } else {
          // 其他错误，直接抛出
          throw error;
        }
      }

      // 保存分页数据到子表（完整JSON）
      await prisma.lingXingProductPerformancePage.upsert({
        where: {
          performanceId_pageNumber: {
            performanceId: performance.id,
            pageNumber: pageNumber
          }
        },
        update: {
          pageOffset: offset,
          pageLength: length,
          total: pageData.total !== undefined ? pageData.total : null,
          data: pageData, // 保存完整的分页数据（包含list数组和所有元数据）
          archived: false,
          updatedAt: new Date()
        },
        create: {
          performanceId: performance.id,
          pageOffset: offset,
          pageLength: length,
          pageNumber: pageNumber,
          total: pageData.total !== undefined ? pageData.total : null,
          data: pageData, // 保存完整的分页数据
          archived: false
        }
      });

      console.log(`产品表现数据已保存: 主表ID=${performance.id}, 页码=${pageNumber}, 记录数=${pageData.list ? pageData.list.length : 0}`);
    } catch (error) {
      console.error('保存产品表现到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 查询利润统计-MSKU
   * API: POST /bd/profit/statistics/open/msku/list
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度，上限10000（可选，默认1000）
   *   - mids: 站点id（可选，数组）
   *   - sids: 店铺id（可选，数组）
   *   - startDate: 开始时间，双闭区间（必填，格式：YYYY-MM-DD，开始结束时间间隔最长不能跨度7天）
   *   - endDate: 结束时间，双闭区间（必填，格式：YYYY-MM-DD）
   *   - searchField: 搜索值类型：msku（可选）
   *   - searchValue: 搜索值（可选，数组）
   *   - currencyCode: 币种code（可选）
   * @returns {Promise<Object>} 利润统计数据 { records: [], total: 0 }
   */
  async getMskuProfitStatistics(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.startDate || !params.endDate) {
        throw new Error('startDate 和 endDate 为必填参数');
      }

      // 验证日期格式
      const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRegex.test(params.startDate) || !dateRegex.test(params.endDate)) {
        throw new Error('日期格式错误，应为 YYYY-MM-DD 格式，例如：2023-07-16');
      }

      // 验证日期范围（不能超过7天）
      const startDate = new Date(params.startDate);
      const endDate = new Date(params.endDate);
      const daysDiff = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;
      if (daysDiff > 7) {
        throw new Error('开始结束时间间隔最长不能跨度7天');
      }

      if (startDate > endDate) {
        throw new Error('startDate 不能大于 endDate');
      }

      // 构建请求参数
      const requestParams = {
        startDate: params.startDate,
        endDate: params.endDate,
        ...(params.offset !== undefined && { offset: parseInt(params.offset) }),
        ...(params.length !== undefined && { length: parseInt(params.length) }),
        ...(params.mids && { mids: Array.isArray(params.mids) ? params.mids : [params.mids] }),
        ...(params.sids && { sids: Array.isArray(params.sids) ? params.sids : [params.sids] }),
        ...(params.searchField && { searchField: params.searchField }),
        ...(params.searchValue && { searchValue: Array.isArray(params.searchValue) ? params.searchValue : [params.searchValue] }),
        ...(params.currencyCode && { currencyCode: params.currencyCode })
      };

      // 调用API获取利润统计数据（使用通用客户端，成功码为0，令牌桶容量为10）
      const response = await this.post(account, '/bd/profit/statistics/open/msku/list', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.msg || response.message || '获取利润统计失败');
      }

      const data = response.data || {};
      const records = data.records || [];
      const total = data.total || records.length;

      // 保存利润统计数据到数据库
      if (records.length > 0) {
        await this.saveMskuProfitStatistics(accountId, requestParams, data);
      }

      return {
        records: records,
        total: total
      };
    } catch (error) {
      console.error('获取利润统计失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存利润统计MSKU数据到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Object} queryParams - 查询参数
   *   - sids: 店铺ID数组（可选）
   *   - startDate: 开始日期
   *   - endDate: 结束日期
   *   - currencyCode: 币种code（可选）
   *   - mids: 站点id数组（可选）
   *   - searchField: 搜索值类型（可选）
   *   - searchValue: 搜索值数组（可选）
   * @param {Object} responseData - API响应数据
   *   - records: 数据列表数组
   *   - total: 总记录数
   */
  async saveMskuProfitStatistics(accountId, queryParams, responseData) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingMskuProfitStatistics || !prisma.lingXingMskuProfitStatisticsItem) {
        console.error('Prisma Client 中未找到利润统计MSKU模型，请重新生成 Prisma Client 并重启服务器');
        return;
      }

      const { sids, startDate, endDate, currencyCode, mids, searchField, searchValue } = queryParams;

      // 处理 sids（可能是数组或单个值）
      let sidArray = [];
      if (sids) {
        sidArray = Array.isArray(sids) ? sids : [sids];
        // 对数组进行排序，确保相同的内容产生相同的 sidString
        sidArray = sidArray.map(s => parseInt(s)).sort((a, b) => a - b);
      }
      const sidString = sidArray.length > 0 ? sidArray.join(',') : '';

      // 创建或更新主表记录（记录查询条件）
      let statistics;
      try {
        statistics = await prisma.lingXingMskuProfitStatistics.upsert({
          where: {
            accountId_sidString_startDate_endDate_currencyCode: {
              accountId: accountId,
              sidString: sidString,
              startDate: startDate,
              endDate: endDate,
              currencyCode: currencyCode || null
            }
          },
          update: {
            mids: mids || null,
            sids: sidArray.length > 0 ? sidArray : null,
            searchField: searchField || null,
            searchValue: searchValue || null,
            totalRecords: responseData.total !== undefined ? responseData.total : null,
            archived: false,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            sidString: sidString,
            startDate: startDate,
            endDate: endDate,
            currencyCode: currencyCode || null,
            mids: mids || null,
            sids: sidArray.length > 0 ? sidArray : null,
            searchField: searchField || null,
            searchValue: searchValue || null,
            totalRecords: responseData.total !== undefined ? responseData.total : null,
            archived: false
          }
        });
      } catch (error) {
        // 如果是唯一约束冲突（并发情况），尝试重新查询
        if (error.code === 'P2002') {
          console.warn(`唯一约束冲突，尝试重新查询并更新主表记录: ${error.message}`);
          statistics = await prisma.lingXingMskuProfitStatistics.findUnique({
            where: {
              accountId_sidString_startDate_endDate_currencyCode: {
                accountId: accountId,
                sidString: sidString,
                startDate: startDate,
                endDate: endDate,
                currencyCode: currencyCode || null
              }
            }
          });
          if (statistics) {
            statistics = await prisma.lingXingMskuProfitStatistics.update({
              where: { id: statistics.id },
              data: {
                mids: mids || null,
                sids: sidArray.length > 0 ? sidArray : null,
                searchField: searchField || null,
                searchValue: searchValue || null,
                totalRecords: responseData.total !== undefined ? responseData.total : null,
                updatedAt: new Date()
              }
            });
          } else {
            throw new Error(`处理唯一约束冲突失败，未找到主表记录: ${error.message}`);
          }
        } else {
          throw error;
        }
      }

      // 保存每条记录到子表
      const records = responseData.records || [];
      let savedCount = 0;
      let skippedCount = 0;

      for (const record of records) {
        // 确保关键字段有值，如果没有值则跳过
        const msku = record.msku || '';
        const sid = record.sid !== undefined && record.sid !== null ? record.sid : null;
        const dataDate = record.dataDate || '';

        // 如果msku和dataDate都为空，跳过这条记录
        if (!msku && !dataDate) {
          console.warn('跳过利润统计记录：msku和dataDate都为空', record);
          skippedCount++;
          continue;
        }

        try {
          await prisma.lingXingMskuProfitStatisticsItem.upsert({
            where: {
              statisticsId_msku_sid_dataDate: {
                statisticsId: statistics.id,
                msku: msku,
                sid: sid,
                dataDate: dataDate
              }
            },
            update: {
              asin: record.asin || null,
              data: record, // 保存完整的记录数据
              archived: false,
              updatedAt: new Date()
            },
            create: {
              statisticsId: statistics.id,
              msku: msku,
              asin: record.asin || null,
              sid: sid,
              dataDate: dataDate,
              data: record, // 保存完整的记录数据
              archived: false
            }
          });
          savedCount++;
        } catch (error) {
          if (error.code === 'P2002') {
            // 唯一约束冲突，可能是并发更新，尝试更新
            try {
              await prisma.lingXingMskuProfitStatisticsItem.updateMany({
                where: {
                  statisticsId: statistics.id,
                  msku: msku,
                  sid: sid,
                  dataDate: dataDate
                },
                data: {
                  asin: record.asin || null,
                  data: record,
                  archived: false,
                  updatedAt: new Date()
                }
              });
              savedCount++;
            } catch (updateError) {
              console.error(`更新利润统计记录失败: ${updateError.message}`, record);
              skippedCount++;
            }
          } else {
            console.error(`保存利润统计记录失败: ${error.message}`, record);
            skippedCount++;
          }
        }
      }

      console.log(`利润统计MSKU数据已保存: 主表ID=${statistics.id}, 成功=${savedCount}, 跳过=${skippedCount}, 总记录数=${records.length}`);
    } catch (error) {
      console.error('保存利润统计MSKU到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 销量报表增量同步（按日期范围，无更新时间维度）
   */
  async incrementalSyncSalesReport(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'salesReport',
      { defaultLookbackDays: options.defaultLookbackDays ?? 3, ...options },
      async (id, params, opts) => this.getSalesReportByDateRange(id, { ...params, ...opts })
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 产品表现增量同步（按日期范围）
   */
  async incrementalSyncProductPerformance(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'productPerformance',
      options,
      async (id, params, opts) => this.fetchAllProductPerformance(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润统计 MSKU 增量同步（按日期，接口单次最多 7 天，内部按 7 天分片）
   */
  async incrementalSyncMskuProfitStatistics(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'mskuProfitStatistics',
      options,
      async (id, params) => {
        const start = new Date(params.start_date + 'T00:00:00Z');
        const end = new Date(params.end_date + 'T23:59:59Z');
        let total = 0;
        for (let d = new Date(start); d <= end;) {
          const endChunk = new Date(d);
          endChunk.setUTCDate(endChunk.getUTCDate() + 6);
          if (endChunk > end) endChunk.setTime(end.getTime());
          const startStr = d.toISOString().slice(0, 10);
          const endStr = endChunk.toISOString().slice(0, 10);
          const res = await this.getMskuProfitStatistics(id, { startDate: startStr, endDate: endStr });
          total += res?.total ?? res?.records?.length ?? 0;
          d.setUTCDate(d.getUTCDate() + 7);
        }
        return { total };
      }
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }
}

export default new LingXingReportService();

