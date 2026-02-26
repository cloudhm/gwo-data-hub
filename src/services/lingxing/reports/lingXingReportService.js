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
   * 保存销量报表数据到数据库（仅主表：按天软删后插入，完整数据存 data，不写明细表）
   */
  async saveSalesReport(accountId, reportParams, reportData) {
    try {
      if (!prisma.lingXingSalesReport) {
        console.error('Prisma Client 中未找到 lingXingSalesReport 模型');
        return;
      }

      const { sid, event_date, asin_type, type } = reportParams;
      const sidVal = sid !== undefined && sid !== null ? parseInt(sid) : 0;
      const eventDate = String(event_date || '').trim().slice(0, 10);
      const asinTypeVal = asin_type !== undefined && asin_type !== null ? parseInt(asin_type) : 1;
      const typeVal = type !== undefined && type !== null ? parseInt(type) : 1;

      // 软删除该维度下已有主表（及关联明细，便于历史清理）
      const existing = await prisma.lingXingSalesReport.findMany({
        where: { accountId, sid: sidVal, eventDate, asinType: asinTypeVal, type: typeVal, archived: false },
        select: { id: true }
      });
      if (existing.length > 0) {
        const ids = existing.map(r => r.id);
        await prisma.lingXingSalesReportItem.updateMany(
          { where: { reportId: { in: ids } }, data: { archived: true, updatedAt: new Date() } }
        );
        await prisma.lingXingSalesReport.updateMany(
          { where: { id: { in: ids } }, data: { archived: true, updatedAt: new Date() } }
        );
      }

      // 插入一条主表，完整数据存 data，不写明细表
      await prisma.lingXingSalesReport.create({
        data: {
          accountId,
          sid: sidVal,
          eventDate,
          asinType: asinTypeVal,
          type: typeVal,
          totalRecords: reportData.length,
          data: reportData,
          archived: false
        }
      });

      console.log(`销量报表主表已保存: sid=${sidVal}, event_date=${eventDate}, asin_type=${asinTypeVal}, type=${typeVal}, 共 ${reportData.length} 条`);
    } catch (error) {
      console.error('保存销量报表到数据库失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询店铺汇总销量（按店铺维度查询店铺销量、销售额）
   * API: POST /erp/sc/data/sales_report/sales
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sid: 店铺id（必填）
   *   - start_date: 报表时间，格式：Y-m-d，闭区间（必填）
   *   - end_date: 报表时间，格式：Y-m-d，闭区间（必填）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认1000（可选）
   * @returns {Promise<Object>} { data: [], total: 0 }
   */
  async getStoreSummarySales(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (params.sid === undefined || params.sid === null) throw new Error('sid 为必填参数');
    if (!params.start_date || !params.end_date) throw new Error('start_date 和 end_date 为必填参数');
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(params.start_date) || !dateRegex.test(params.end_date)) {
      throw new Error('日期格式错误，应为 Y-m-d 格式');
    }
    const requestParams = {
      sid: parseInt(params.sid),
      start_date: params.start_date,
      end_date: params.end_date,
      ...(params.offset !== undefined && { offset: parseInt(params.offset) }),
      ...(params.length !== undefined && { length: parseInt(params.length) })
    };
    const response = await this.post(account, '/erp/sc/data/sales_report/sales', requestParams, {
      successCode: [0, 200, '200']
    });
    if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
      throw new Error(response.message || '获取店铺汇总销量失败');
    }
    const data = response.data || [];
    const total = response.total ?? data.length;
    return { data, total };
  }

  /**
   * 按天保存店铺汇总销量（软删当日该 sid 主表后插入一条，完整数据存 data）
   */
  async saveStoreSummarySalesForDay(accountId, sid, eventDate, records) {
    const sidVal = parseInt(sid);
    const eventDateStr = String(eventDate || '').trim().slice(0, 10);
    const existing = await prisma.lingXingStoreSummarySales.findMany({
      where: { accountId, sid: sidVal, eventDate: eventDateStr, archived: false },
      select: { id: true }
    });
    if (existing.length > 0) {
      const ids = existing.map(r => r.id);
      await prisma.lingXingStoreSummarySales.updateMany({
        where: { id: { in: ids } },
        data: { archived: true, updatedAt: new Date() }
      });
    }
    await prisma.lingXingStoreSummarySales.create({
      data: { accountId, sid: sidVal, eventDate: eventDateStr, data: records || [], archived: false }
    });
  }

  /**
   * 按日期范围、按 sid 遍历拉取店铺汇总销量并按天存储（支持增量）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - { start_date, end_date } 闭区间 Y-m-d
   * @param {Object} options - pageSize, delayBetweenDays, delayBetweenShops
   * @returns {Promise<{ total: number, data?: Array }>}
   */
  async fetchAllStoreSummarySalesByDay(accountId, listParams = {}, options = {}) {
    const { start_date, end_date } = listParams;
    if (!start_date || !end_date) throw new Error('start_date 和 end_date 为必填');
    const pageSize = options.pageSize ?? 1000;
    const delayBetweenDays = options.delayBetweenDays ?? 500;
    const delayBetweenShops = options.delayBetweenShops ?? 1000;

    let shopList = [];
    try {
      const sellers = await lingxingBasicDataService.getSellerLists(accountId, true);
      shopList = sellers.map(s => ({ sid: s.sid, name: s.name || s.account_name || `店铺${s.sid}` }));
    } catch (e) {
      console.error('获取店铺列表失败:', e.message);
      throw new Error(`获取店铺列表失败: ${e.message}`);
    }
    if (shopList.length === 0) return { total: 0, data: [] };

    const startDate = new Date(start_date);
    const endDate = new Date(end_date);
    if (startDate > endDate) return { total: 0, data: [] };

    let totalRecords = 0;
    for (let s = 0; s < shopList.length; s++) {
      const shop = shopList[s];
      const sid = shop.sid;
      const currentDate = new Date(startDate);
      while (currentDate <= endDate) {
        const dateStr = currentDate.toISOString().split('T')[0];
        let offset = 0;
        let hasMore = true;
        const dayData = [];
        while (hasMore) {
          const { data: pageData, total: pageTotal } = await this.getStoreSummarySales(accountId, {
            sid,
            start_date: dateStr,
            end_date: dateStr,
            offset,
            length: pageSize
          });
          dayData.push(...(pageData || []));
          if ((pageData?.length || 0) < pageSize) hasMore = false;
          else offset += pageSize;
        }
        if (dayData.length > 0) {
          await this.saveStoreSummarySalesForDay(accountId, sid, dateStr, dayData);
          totalRecords += dayData.length;
        }
        currentDate.setDate(currentDate.getDate() + 1);
        if (delayBetweenDays > 0) await new Promise(r => setTimeout(r, delayBetweenDays));
      }
      if (s < shopList.length - 1 && delayBetweenShops > 0) {
        await new Promise(r => setTimeout(r, delayBetweenShops));
      }
    }
    return { total: totalRecords, data: [] };
  }

  /**
   * 店铺汇总销量增量同步（按日期范围、按 sid 遍历、按天存储）
   */
  async incrementalSyncStoreSummarySales(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storeSummarySales',
      { defaultLookbackDays: options.defaultLookbackDays ?? 10, ...options },
      async (id, params, opts) => this.fetchAllStoreSummarySalesByDay(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 查询FBA月仓储费
   * API: POST /erp/sc/data/fba_report/storageFeeMonth
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - sid (必填), month (Y-m 必填), offset, length
   * @returns {Promise<Object>} { data: [], total: 0 }
   */
  async getFbaStorageFeeMonth(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (params.sid === undefined || params.sid === null) throw new Error('sid 为必填参数');
    if (!params.month) throw new Error('month 为必填参数，格式 Y-m');
    const requestParams = {
      sid: parseInt(params.sid),
      month: String(params.month).trim().slice(0, 7)
    };
    if (params.offset !== undefined) requestParams.offset = parseInt(params.offset);
    if (params.length !== undefined) requestParams.length = parseInt(params.length);
    const response = await this.post(account, '/erp/sc/data/fba_report/storageFeeMonth', requestParams, { successCode: [0, 200, '200'] });
    if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
      throw new Error(response.message || '查询FBA月仓储费失败');
    }
    const data = response.data || [];
    const total = response.total ?? data.length;
    return { data, total };
  }

  /**
   * 按 sid + 月份保存FBA月仓储费（软删当月该 sid 后插入一条，完整数据存 data）
   */
  async saveFbaStorageFeeMonthForMonth(accountId, sid, month, records) {
    const sidVal = parseInt(sid);
    const monthStr = String(month || '').trim().slice(0, 7);
    const existing = await prisma.lingXingFbaStorageFeeMonth.findMany({
      where: { accountId, sid: sidVal, month: monthStr, archived: false },
      select: { id: true }
    });
    if (existing.length > 0) {
      const ids = existing.map(r => r.id);
      await prisma.lingXingFbaStorageFeeMonth.updateMany({
        where: { id: { in: ids } },
        data: { archived: true, updatedAt: new Date() }
      });
    }
    await prisma.lingXingFbaStorageFeeMonth.create({
      data: { accountId, sid: sidVal, month: monthStr, data: records || [], archived: false }
    });
  }

  /**
   * 按日期范围生成月份列表（Y-m）
   */
  _getMonthsFromDateRange(startDate, endDate) {
    const start = new Date(startDate);
    const end = new Date(endDate);
    if (start > end) return [];
    const months = [];
    const current = new Date(start.getFullYear(), start.getMonth(), 1);
    const endFirst = new Date(end.getFullYear(), end.getMonth(), 1);
    while (current <= endFirst) {
      months.push(`${current.getFullYear()}-${String(current.getMonth() + 1).padStart(2, '0')}`);
      current.setMonth(current.getMonth() + 1);
    }
    return months;
  }

  /**
   * 按 sid 从 DB 遍历、按月份拉取FBA月仓储费并保存（支持增量）
   */
  async fetchAllFbaStorageFeeMonthByMonth(accountId, listParams = {}, options = {}) {
    const { start_date, end_date } = listParams;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const pageSize = options.pageSize ?? 1000;
    const delayBetweenMonths = options.delayBetweenMonths ?? 200;
    const delayBetweenShops = options.delayBetweenShops ?? 500;
    const months = this._getMonthsFromDateRange(start_date, end_date);
    if (months.length === 0) return { total: 0, data: [] };
    let shopList = [];
    try {
      const sellers = await lingxingBasicDataService.getSellerLists(accountId, true);
      shopList = sellers.map(s => ({ sid: s.sid, name: s.name || s.account_name || `店铺${s.sid}` }));
    } catch (e) {
      console.error('获取店铺列表失败:', e.message);
      throw new Error(`获取店铺列表失败: ${e.message}`);
    }
    if (shopList.length === 0) return { total: 0, data: [] };
    let totalRecords = 0;
    for (let s = 0; s < shopList.length; s++) {
      const shop = shopList[s];
      const sid = shop.sid;
      for (let m = 0; m < months.length; m++) {
        const monthStr = months[m];
        let offset = 0;
        let hasMore = true;
        const monthData = [];
        while (hasMore) {
          const { data: pageData, total: pageTotal } = await this.getFbaStorageFeeMonth(accountId, {
            sid,
            month: monthStr,
            offset,
            length: pageSize
          });
          monthData.push(...(pageData || []));
          if ((pageData?.length || 0) < pageSize) hasMore = false;
          else offset += pageSize;
        }
        if (monthData.length > 0) {
          await this.saveFbaStorageFeeMonthForMonth(accountId, sid, monthStr, monthData);
          totalRecords += monthData.length;
        }
        if (m < months.length - 1 && delayBetweenMonths > 0) await new Promise(r => setTimeout(r, delayBetweenMonths));
      }
      if (s < shopList.length - 1 && delayBetweenShops > 0) await new Promise(r => setTimeout(r, delayBetweenShops));
    }
    return { total: totalRecords, data: [] };
  }

  /**
   * FBA月仓储费增量同步（按 sid 从 DB 遍历、按月份查询并保存）
   */
  async incrementalSyncFbaStorageFeeMonth(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'fbaStorageFeeMonth',
      { defaultLookbackDays: options.defaultLookbackDays ?? 90, ...options },
      async (id, params, opts) => this.fetchAllFbaStorageFeeMonthByMonth(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 统计-查询退货分析
   * API: POST /basicOpen/salesAnalysis/returnOrder/analysisLists
   * 令牌桶容量: 1，最多支持366天范围
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - startDate, endDate (yyyy-MM-dd), length, offset; 可选: asinType, dateType, mids, principalUid, searchField, searchValue, sortField, sortType, storeId
   * @returns {Promise<Object>} { data: { records: [], total }, total }
   */
  async getReturnOrderAnalysisLists(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.startDate || !params.endDate) throw new Error('startDate、endDate 为必填，格式 yyyy-MM-dd');
    if (params.offset === undefined || params.length === undefined) throw new Error('offset、length 为必填');
    const requestParams = {
      startDate: params.startDate,
      endDate: params.endDate,
      offset: parseInt(params.offset),
      length: parseInt(params.length)
    };
    if (params.asinType !== undefined) requestParams.asinType = params.asinType;
    if (params.dateType !== undefined) requestParams.dateType = params.dateType;
    if (params.mids !== undefined) requestParams.mids = params.mids;
    if (params.principalUid !== undefined) requestParams.principalUid = params.principalUid;
    if (params.searchField !== undefined) requestParams.searchField = params.searchField;
    if (params.searchValue !== undefined) requestParams.searchValue = params.searchValue;
    if (params.sortField !== undefined) requestParams.sortField = params.sortField;
    if (params.sortType !== undefined) requestParams.sortType = params.sortType;
    if (params.storeId !== undefined) requestParams.storeId = params.storeId;
    const response = await this.post(account, '/basicOpen/salesAnalysis/returnOrder/analysisLists', requestParams, { successCode: [0, 200, '200'] });
    if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
      throw new Error(response.message || '查询退货分析失败');
    }
    const data = response.data || {};
    const records = data.records || [];
    const total = data.total ?? response.total ?? records.length;
    return { data: { records, total }, total };
  }

  /**
   * 按天保存退货分析（软删当日后插入一条，data 存 { records, total }）
   */
  async saveReturnOrderAnalysisForDay(accountId, eventDate, payload) {
    const eventDateStr = String(eventDate || '').trim().slice(0, 10);
    const existing = await prisma.lingXingReturnOrderAnalysis.findMany({
      where: { accountId, eventDate: eventDateStr, archived: false },
      select: { id: true }
    });
    if (existing.length > 0) {
      await prisma.lingXingReturnOrderAnalysis.updateMany({
        where: { id: { in: existing.map(r => r.id) } },
        data: { archived: true, updatedAt: new Date() }
      });
    }
    await prisma.lingXingReturnOrderAnalysis.create({
      data: { accountId, eventDate: eventDateStr, data: payload || {}, archived: false }
    });
  }

  /**
   * 按天拉取退货分析并保存（日期范围内逐日查询、分页合并后保存；storeId 未传时从 DB 店铺列表遍历）
   */
  async fetchAllReturnOrderAnalysisByDay(accountId, listParams = {}, options = {}) {
    const { start_date, end_date } = listParams;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const pageSize = options.pageSize ?? 20;
    const delayBetweenDays = options.delayBetweenDays ?? 200;
    let storeId = options.storeId;
    if (storeId === undefined || (Array.isArray(storeId) && storeId.length === 0)) {
      const sellers = await prisma.lingXingSeller.findMany({
        where: { accountId, status: 1 },
        select: { sid: true }
      });
      storeId = sellers.map(s => s.sid).filter(Boolean);
    }
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);
    if (startDate > endDate) return { total: 0, data: [] };
    let totalRecords = 0;
    const currentDate = new Date(startDate);
    while (currentDate <= endDate) {
      const dayStr = currentDate.toISOString().split('T')[0];
      let offset = 0;
      let allRecords = [];
      let hasMore = true;
      while (hasMore) {
        const res = await this.getReturnOrderAnalysisLists(accountId, {
          startDate: dayStr,
          endDate: dayStr,
          offset,
          length: pageSize,
          ...(Array.isArray(storeId) && storeId.length > 0 && { storeId }),
          ...(options.asinType !== undefined && { asinType: options.asinType }),
          ...(options.dateType !== undefined && { dateType: options.dateType })
        });
        const records = (res.data && res.data.records) || [];
        allRecords = allRecords.concat(records);
        hasMore = records.length >= pageSize;
        if (hasMore) offset += pageSize;
      }
      await this.saveReturnOrderAnalysisForDay(accountId, dayStr, { records: allRecords, total: allRecords.length });
      totalRecords += allRecords.length;
      currentDate.setDate(currentDate.getDate() + 1);
      if (delayBetweenDays > 0) await new Promise(r => setTimeout(r, delayBetweenDays));
    }
    return { total: totalRecords, data: [] };
  }

  /**
   * 退货分析增量同步（按天查询并保存）
   */
  async incrementalSyncReturnOrderAnalysis(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'returnOrderAnalysis',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 366, 366), ...options },
      (id, params, opts) => this.fetchAllReturnOrderAnalysisByDay(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
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

      // 保存产品表现数据到数据库（可由调用方传入 skipSave 跳过，用于按天拉取后统一按天覆盖保存）
      if ((list.length > 0 || data) && params.skipSave !== true) {
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
   * 注意：每次只查询一天的数据，如果日期范围跨多天，会按天遍历；按天拉取完成后按天覆盖保存（先删该日主表+子表再插入）
   * sid 不传时从数据库遍历该账户下所有店铺；summary_field 不传时遍历 asin、parent_asin、msku、sku
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（同 getProductPerformance）
   *   - sid: 店铺id（可选，不传则从数据库获取所有店铺并遍历）
   *   - summary_field: 汇总行维度（可选，不传则遍历 asin/parent_asin/msku/sku）
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
                  const dayPages = []; // 当日所有分页数据，用于按天覆盖保存

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
                        skipSave: true, // 按天拉取后统一按天覆盖保存，不在此处落库
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
                      const pagePayload = {
                        list: pageData,
                        total: pageResult.total,
                        chain_start_date: pageResult.chain_start_date,
                        chain_end_date: pageResult.chain_end_date,
                        available_inventory_formula_zh: pageResult.available_inventory_formula_zh
                      };
                      dayPages.push({ offset, length: actualPageSize, ...pagePayload });
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

                  // 按天覆盖保存：先删该日主表+子表，再插入当日所有分页
                  if (dayPages.length > 0) {
                    await this.saveProductPerformanceForDay(accountId, shopSid, dateStr, dateStr, summaryField, dayPages);
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
   * 按天覆盖保存产品表现：先软删除该日该 sid+summary_field 的主表及子表，再插入一条主表（完整数据存 data，不写 pages 子表）
   */
  async saveProductPerformanceForDay(accountId, sid, startDate, endDate, summaryField, dayPages) {
    try {
      if (!prisma.lingXingProductPerformance) return;
      const sidString = String(sid).trim();
      const existing = await prisma.lingXingProductPerformance.findMany({
        where: { accountId, sidString, startDate, endDate, summaryField, archived: false },
        select: { id: true }
      });
      const ids = existing.map(p => p.id);
      if (ids.length > 0) {
        if (prisma.lingXingProductPerformancePage) {
          await prisma.lingXingProductPerformancePage.updateMany(
            { where: { performanceId: { in: ids } }, data: { archived: true, updatedAt: new Date() } }
          );
        }
        await prisma.lingXingProductPerformance.updateMany(
          { where: { id: { in: ids } }, data: { archived: true, updatedAt: new Date() } }
        );
      }
      // 合并当日所有分页的 list 为完整数组，存入主表 data，不写 pages
      const fullList = (dayPages || []).reduce((acc, page) => {
        const list = page.list || page.data || [];
        return acc.concat(Array.isArray(list) ? list : []);
      }, []);
      await prisma.lingXingProductPerformance.create({
        data: {
          accountId,
          sidString,
          startDate,
          endDate,
          summaryField,
          data: fullList,
          archived: false
        }
      });
      console.log(`产品表现已按天覆盖保存: sid=${sidString} ${startDate} summary_field=${summaryField} 共 ${fullList.length} 条`);
    } catch (error) {
      console.error('按天保存产品表现失败:', error.message);
      throw error;
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

      // 保存利润统计数据到数据库（可由调用方传入 skipSave 跳过，用于按天拉取后统一按天覆盖保存）
      if (records.length > 0 && params.skipSave !== true) {
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
   * 查询 ASIN360 小时数据
   * API: POST /basicOpen/salesAnalysis/productPerformance/performanceTrendByHour  令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - sids: 店铺id，多个英文逗号分隔，最大200（必填）
   *   - date_start: 开始时间 Y-m-d 闭区间（必填）
   *   - date_end: 结束时间 Y-m-d 闭区间（必填）
   *   - summary_field: 查询维度 parent_asin | asin | msku | sku | spu（必填）
   *   - summary_field_value: 查询维度值（必填）
   *   - skipSave: 为 true 时不落库，由调用方按天覆盖保存（可选）
   * @returns {Promise<Object>} { list: [], total: {}, currency_icon, data }
   */
  async getAsin360HourData(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
      if (!account) throw new Error(`领星账户不存在: ${accountId}`);
      if (params.sids === undefined || params.sids === null || String(params.sids).trim() === '') {
        throw new Error('sids 为必填参数，多个使用英文逗号分隔');
      }
      if (!params.date_start || !params.date_end) throw new Error('date_start、date_end 为必填参数');
      if (!params.summary_field || params.summary_field_value === undefined || params.summary_field_value === null) {
        throw new Error('summary_field、summary_field_value 为必填参数');
      }
      const sidsStr = String(params.sids).trim();
      const requestParams = {
        sids: sidsStr,
        date_start: params.date_start,
        date_end: params.date_end,
        summary_field: params.summary_field,
        summary_field_value: String(params.summary_field_value)
      };
      const response = await this.post(account, '/basicOpen/salesAnalysis/productPerformance/performanceTrendByHour', requestParams, { successCode: [0, 200, '200'] });
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || response.msg || '获取ASIN360小时数据失败');
      }
      const data = response.data || {};
      const list = data.list || [];
      const total = data.total || null;
      const currencyIcon = data.currency_icon != null ? data.currency_icon : null;
      if ((list.length > 0 || total) && params.skipSave !== true) {
        await this.saveAsin360HourData(accountId, requestParams, data);
      }
      return { list, total, currency_icon: currencyIcon, data };
    } catch (error) {
      console.error('获取ASIN360小时数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存 ASIN360 小时数据到数据库（按查询条件 upsert）
   */
  async saveAsin360HourData(accountId, queryParams, responseData) {
    try {
      if (!prisma.lingXingAsin360HourData) {
        console.error('Prisma Client 中未找到 lingXingAsin360HourData 模型');
        return;
      }
      const sidsRaw = queryParams.sids;
      const sids = (sidsRaw != null ? String(sidsRaw).trim() : '').replace(/\s+/g, '');
      const sidArray = sids ? sids.split(',').map(s => s.trim()).filter(Boolean).map(s => parseInt(s, 10)).filter(n => !Number.isNaN(n)).sort((a, b) => a - b) : [];
      const sidString = sidArray.length > 0 ? sidArray.join(',') : sids || '';
      const dateStart = (queryParams.date_start != null ? String(queryParams.date_start) : '').trim().slice(0, 10);
      const dateEnd = (queryParams.date_end != null ? String(queryParams.date_end) : '').trim().slice(0, 10);
      const summaryField = (queryParams.summary_field != null ? String(queryParams.summary_field) : '').trim();
      const summaryFieldValue = (queryParams.summary_field_value != null ? String(queryParams.summary_field_value) : '').trim();
      await prisma.lingXingAsin360HourData.upsert({
        where: {
          accountId_sids_dates_summary_asin360: {
            accountId,
            sids: sidString,
            dateStart,
            dateEnd,
            summaryField,
            summaryFieldValue
          }
        },
        update: {
          data: responseData,
          archived: false,
          updatedAt: new Date()
        },
        create: {
          accountId,
          sids: sidString,
          dateStart,
          dateEnd,
          summaryField,
          summaryFieldValue,
          data: responseData,
          archived: false
        }
      });
      console.log(`ASIN360小时数据已保存: sids=${sidString} ${dateStart}~${dateEnd} ${summaryField}=${summaryFieldValue}`);
    } catch (error) {
      console.error('保存ASIN360小时数据失败:', error.message);
    }
  }

  /**
   * 按天覆盖保存 ASIN360 小时数据：先删除该日该 sids+summary_field+summary_field_value 的记录，再插入
   */
  async saveAsin360HourDataForDay(accountId, sids, day, summaryField, summaryFieldValue, responseData) {
    try {
      if (!prisma.lingXingAsin360HourData) return;
      const sidsRaw = sids != null ? String(sids).trim() : '';
      const sidArray = sidsRaw ? sidsRaw.replace(/\s+/g, '').split(',').map(s => s.trim()).filter(Boolean).map(s => parseInt(s, 10)).filter(n => !Number.isNaN(n)).sort((a, b) => a - b) : [];
      const sidString = sidArray.length > 0 ? sidArray.join(',') : sidsRaw || '';
      const dateStr = (day != null ? String(day) : '').trim().slice(0, 10);
      const summary = (summaryField != null ? String(summaryField) : '').trim();
      const summaryVal = (summaryFieldValue != null ? String(summaryFieldValue) : '').trim();
      await prisma.lingXingAsin360HourData.updateMany({
        where: {
          accountId,
          sids: sidString,
          dateStart: dateStr,
          dateEnd: dateStr,
          summaryField: summary,
          summaryFieldValue: summaryVal
        },
        data: { archived: true, updatedAt: new Date() }
      });
      await prisma.lingXingAsin360HourData.create({
        data: {
          accountId,
          sids: sidString,
          dateStart: dateStr,
          dateEnd: dateStr,
          summaryField: summary,
          summaryFieldValue: summaryVal,
          data: responseData || {},
          archived: false
        }
      });
      console.log(`ASIN360小时数据已按天覆盖保存: sids=${sidString} ${dateStr} ${summary}=${summaryVal}`);
    } catch (error) {
      console.error('按天保存ASIN360小时数据失败:', error.message);
    }
  }

  /**
   * 批量查询 ASIN360 小时数据（按天、按店铺、按 summary_field 遍历，按天覆盖保存）
   * 与产品表现同样处理：sid 不传时从数据库遍历该账户下所有店铺；summary_field 不传时遍历 asin、parent_asin、msku、sku、spu
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - start_date, end_date: Y-m-d 必填
   *   - summary_field_value: 查询维度值（API 必填，不传则报错）
   *   - sids: 可选，不传则从 DB 获取所有店铺并遍历
   *   - summary_field: 可选，不传则遍历 asin/parent_asin/msku/sku/spu
   *   - delayBetweenShops, delayBetweenDays, delayBetweenSummaryFields: 毫秒延迟
   * @returns {Promise<Object>} { stats, totalRecords }
   */
  async fetchAllAsin360HourData(accountId, params = {}) {
    const {
      delayBetweenShops = 1000,
      delayBetweenDays = 1000,
      delayBetweenSummaryFields = 1000
    } = params;

    if (!params.start_date || !params.end_date) {
      throw new Error('start_date 和 end_date 为必填参数');
    }
    if (params.summary_field_value === undefined || params.summary_field_value === null || String(params.summary_field_value).trim() === '') {
      throw new Error('summary_field_value 为必填参数（API 要求）');
    }

    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(params.start_date) || !dateRegex.test(params.end_date)) {
      throw new Error('日期格式错误，应为 YYYY-MM-DD');
    }

    const startDate = new Date(params.start_date);
    const endDate = new Date(params.end_date);
    if (startDate > endDate) throw new Error('start_date 不能大于 end_date');

    let shopList = [];
    if (params.sids === undefined || params.sids === null) {
      const sellers = await lingxingBasicDataService.getSellerLists(accountId, true);
      shopList = sellers.map(s => ({ sid: s.sid, name: s.name || s.account_name || `店铺${s.sid}` }));
    } else {
      const raw = params.sids;
      const arr = Array.isArray(raw) ? raw : (typeof raw === 'string' ? raw.split(',').map(s => s.trim()).filter(Boolean) : [raw]);
      shopList = arr.map(sid => ({ sid: typeof sid === 'number' ? sid : parseInt(sid, 10), name: `店铺${sid}` }));
    }

    const summaryFields = params.summary_field
      ? [params.summary_field]
      : ['asin', 'parent_asin', 'msku', 'sku', 'spu'];

    const summaryFieldValue = String(params.summary_field_value).trim();
    const stats = { totalShops: shopList.length, successDays: 0, failedDays: 0, totalRecords: 0 };

    for (let shopIndex = 0; shopIndex < shopList.length; shopIndex++) {
      const shop = shopList[shopIndex];
      const sidsStr = String(shop.sid);

      for (let fi = 0; fi < summaryFields.length; fi++) {
        const summaryField = summaryFields[fi];
        const currentDate = new Date(startDate);

        while (currentDate <= endDate) {
          const dateStr = currentDate.toISOString().split('T')[0];
          try {
            const result = await this.getAsin360HourData(accountId, {
              sids: sidsStr,
              date_start: dateStr,
              date_end: dateStr,
              summary_field: summaryField,
              summary_field_value: summaryFieldValue,
              skipSave: true
            });
            const data = result.data || { list: result.list, total: result.total, currency_icon: result.currency_icon };
            await this.saveAsin360HourDataForDay(accountId, sidsStr, dateStr, summaryField, summaryFieldValue, data);
            stats.successDays++;
            stats.totalRecords += (result.list || []).length;
          } catch (err) {
            stats.failedDays++;
            console.error(`[ASIN360] ${shop.name} ${summaryField} ${dateStr} 失败:`, err.message);
          }
          currentDate.setDate(currentDate.getDate() + 1);
          if (currentDate <= endDate && delayBetweenDays > 0) {
            await new Promise(r => setTimeout(r, delayBetweenDays));
          }
        }

        if (fi < summaryFields.length - 1 && delayBetweenSummaryFields > 0) {
          await new Promise(r => setTimeout(r, delayBetweenSummaryFields));
        }
      }

      if (shopIndex < shopList.length - 1 && delayBetweenShops > 0) {
        await new Promise(r => setTimeout(r, delayBetweenShops));
      }
    }

    return { stats, totalRecords: stats.totalRecords, total: stats.totalRecords };
  }

  /**
   * 保存利润统计MSKU数据到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Object} queryParams - 查询参数
   * @param {Object} responseData - API响应数据
   * @param {Object} options - 可选。overwriteForDay: true 时先删除该日主表及子表再插入（按天覆盖）
   */
  async saveMskuProfitStatistics(accountId, queryParams, responseData, options = {}) {
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
        sidArray = sidArray.map(s => parseInt(s)).sort((a, b) => a - b);
      }
      const sidString = sidArray.length > 0 ? sidArray.join(',') : '';

      const overwriteForDay = options.overwriteForDay === true;

      if (overwriteForDay) {
        // 按天覆盖：软删除该日同条件的主表及子表，再插入一条主表（完整数据存 data，不写 items 子表）
        const existingMain = await prisma.lingXingMskuProfitStatistics.findMany({
          where: {
            accountId,
            startDate,
            endDate,
            sidString,
            currencyCode: currencyCode || null,
            archived: false
          },
          select: { id: true }
        });
        const mainIds = existingMain.map(m => m.id);
        if (mainIds.length > 0) {
          await prisma.lingXingMskuProfitStatisticsItem.updateMany({
            where: { statisticsId: { in: mainIds } },
            data: { archived: true, updatedAt: new Date() }
          });
          await prisma.lingXingMskuProfitStatistics.updateMany({
            where: { id: { in: mainIds } },
            data: { archived: true, updatedAt: new Date() }
          });
        }
        const records = responseData.records || [];
        await prisma.lingXingMskuProfitStatistics.create({
          data: {
            accountId,
            sidString,
            startDate,
            endDate,
            currencyCode: currencyCode || null,
            mids: mids || null,
            sids: sidArray.length > 0 ? sidArray : null,
            searchField: searchField || null,
            searchValue: searchValue || null,
            totalRecords: responseData.total !== undefined ? responseData.total : records.length,
            data: records,
            archived: false
          }
        });
        console.log(`利润统计MSKU已按天覆盖保存: ${startDate}~${endDate} sidString=${sidString} 共 ${records.length} 条`);
        return;
      }

      // 非按天覆盖：创建或更新主表记录（记录查询条件）
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

  async fetchMskuProfitStatisticsByDayRange(accountId, listParams = {}, options = {}) {
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
        const res = await this.getMskuProfitStatistics(accountId, { ...baseParams, startDate: day, endDate: day, offset, length: actualPageSize, skipSave: true });
        const pageRecords = res.records || [];
        const total = res.total ?? pageRecords.length;
        if (offset === 0) totalCount = total;
        allRecords.push(...pageRecords);
        if (pageRecords.length < actualPageSize || (totalCount > 0 && allRecords.length >= totalCount)) hasMore = false;
        else { offset += actualPageSize; if (delayBetweenPages > 0) await new Promise(r => setTimeout(r, delayBetweenPages)); }
      }
      if (allRecords.length > 0) {
        await this.saveMskuProfitStatistics(accountId, { ...baseParams, startDate: day, endDate: day }, { records: allRecords, total: allRecords.length }, { overwriteForDay: true });
        totalRecords += allRecords.length;
      }
      if (onProgress) onProgress(i + 1, days.length, day, totalRecords);
    }
    return { total: totalRecords, data: totalRecords, stats: { daysProcessed: days.length, totalRecords } };
  }

  /**
   * 销量报表增量同步（按日期范围，无更新时间维度）
   */
  async incrementalSyncSalesReport(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'salesReport',
      { defaultLookbackDays: options.defaultLookbackDays ?? 10, ...options },
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
      { defaultLookbackDays: options.defaultLookbackDays ?? 10, ...options },
      async (id, params, opts) => this.fetchAllProductPerformance(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * ASIN360 小时数据增量同步（按天、sid 从库遍历、summary_field 遍历，按天覆盖保存）
   * 需在 options.listParams 中传入 summary_field_value（API 必填）
   */
  async incrementalSyncAsin360HourData(accountId, options = {}) {
    const listParams = options.listParams || {};
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'asin360HourData',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 10, 90), ...options, extraParams: listParams },
      async (id, params, opts) => this.fetchAllAsin360HourData(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 利润统计 MSKU 增量同步（按天拆分，startDate=endDate 同一天，默认回退90天，按天覆盖主表+子表）
   */
  async incrementalSyncMskuProfitStatistics(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'mskuProfitStatistics',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 10, 90), ...options },
      async (id, params, opts) => this.fetchMskuProfitStatisticsByDayRange(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  // ---------- 库存报表-本地仓/海外仓/FBA（按天或按月增量，按天/月软删除覆盖） ----------

  _getMonthsBetween(startYmd, endYmd) {
    const start = new Date(startYmd);
    const end = new Date(endYmd);
    const months = [];
    const d = new Date(start.getFullYear(), start.getMonth(), 1);
    const endFirst = new Date(end.getFullYear(), end.getMonth(), 1);
    while (d <= endFirst) {
      months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
      d.setMonth(d.getMonth() + 1);
    }
    return months;
  }

  /**
   * 库存报表-本地仓-新报表-汇总
   * POST /inventory/center/openapi/storageReport/local/aggregate/list 令牌桶 3
   */
  async getStorageReportLocalAggregate(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填');
    const body = {
      start_date: params.start_date,
      end_date: params.end_date
    };
    if (params.sys_wid != null && String(params.sys_wid).trim() !== '') body.sys_wid = String(params.sys_wid).trim();
    const res = await this.post(account, '/inventory/center/openapi/storageReport/local/aggregate/list', body, { successCode: [0, 200, '200'] });
    if (res.code !== 0 && res.code !== 200 && res.code !== '200') throw new Error(res.message || '本地仓汇总失败');
    return { data: res.data || [], total: (res.data || []).length };
  }

  async saveStorageReportLocalAggregateForDay(accountId, day, sysWid, data) {
    if (!prisma.lingXingStorageReportLocalAggregate) return;
    const sw = sysWid != null ? String(sysWid).trim() : '';
    await prisma.lingXingStorageReportLocalAggregate.updateMany({
      where: { accountId, startDate: day, endDate: day, sysWid: sw },
      data: { archived: true, updatedAt: new Date() }
    });
    await prisma.lingXingStorageReportLocalAggregate.create({
      data: { accountId, startDate: day, endDate: day, sysWid: sw, data: data || [], archived: false }
    });
  }

  async fetchAllStorageReportLocalAggregate(accountId, params = {}, options = {}) {
    const { start_date, end_date } = params;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const days = this._getDaysBetween(start_date, end_date);
    let sysWidList = [''];
    if (params.sys_wid !== undefined && params.sys_wid !== null && String(params.sys_wid).trim() !== '') {
      sysWidList = [String(params.sys_wid).trim()];
    } else {
      const warehouses = await prisma.lingXingWarehouse.findMany({
        where: { accountId, type: 1, archived: false },
        select: { wid: true }
      });
      if (warehouses.length > 0) {
        sysWidList = [warehouses.map(w => String(w.wid)).join(',')];
      }
    }
    let totalRecords = 0;
    for (const day of days) {
      for (const sw of sysWidList) {
        const result = await this.getStorageReportLocalAggregate(accountId, { start_date: day, end_date: day, sys_wid: sw || undefined });
        await this.saveStorageReportLocalAggregateForDay(accountId, day, sw, result.data);
        totalRecords += result.data.length;
      }
    }
    return { total: totalRecords, data: [] };
  }

  async incrementalSyncStorageReportLocalAggregate(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storageReportLocalAggregate',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllStorageReportLocalAggregate(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 库存报表-本地仓-新报表-明细（分页）
   * POST /inventory/center/openapi/storageReport/local/detail/page 令牌桶 3
   */
  async getStorageReportLocalDetailPage(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填');
    const body = {
      offset: params.offset != null ? params.offset : 1,
      length: params.length != null ? params.length : 500,
      start_date: params.start_date,
      end_date: params.end_date
    };
    if (params.sys_wid != null && String(params.sys_wid).trim() !== '') body.sys_wid = String(params.sys_wid).trim();
    const res = await this.post(account, '/inventory/center/openapi/storageReport/local/detail/page', body, { successCode: [0, 200, '200'] });
    if (res.code !== 0 && res.code !== 200 && res.code !== '200') throw new Error(res.message || '本地仓明细失败');
    return { data: res.data || [], total: res.total ?? 0 };
  }

  async saveStorageReportLocalDetailForDay(accountId, day, sysWid, payload) {
    if (!prisma.lingXingStorageReportLocalDetail) return;
    const sw = sysWid != null ? String(sysWid).trim() : '';
    await prisma.lingXingStorageReportLocalDetail.updateMany({
      where: { accountId, startDate: day, endDate: day, sysWid: sw },
      data: { archived: true, updatedAt: new Date() }
    });
    await prisma.lingXingStorageReportLocalDetail.create({
      data: { accountId, startDate: day, endDate: day, sysWid: sw, data: payload, archived: false }
    });
  }

  async fetchAllStorageReportLocalDetail(accountId, params = {}, options = {}) {
    const { start_date, end_date } = params;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const pageSize = Math.min(options.pageSize || 500, 500);
    const days = this._getDaysBetween(start_date, end_date);
    let sysWidList = [''];
    if (params.sys_wid !== undefined && params.sys_wid !== null && String(params.sys_wid).trim() !== '') {
      sysWidList = [String(params.sys_wid).trim()];
    } else {
      const warehouses = await prisma.lingXingWarehouse.findMany({
        where: { accountId, type: 1, archived: false },
        select: { wid: true }
      });
      if (warehouses.length > 0) sysWidList = [warehouses.map(w => String(w.wid)).join(',')];
    }
    let totalRecords = 0;
    for (const day of days) {
      for (const sw of sysWidList) {
        let offset = 1;
        let total = 0;
        const list = [];
        let page;
        do {
          page = await this.getStorageReportLocalDetailPage(accountId, { start_date: day, end_date: day, sys_wid: sw || undefined, offset, length: pageSize });
          list.push(...(page.data || []));
          total = page.total ?? 0;
          offset += 1;
        } while (list.length < total && (page.data || []).length === pageSize);
        await this.saveStorageReportLocalDetailForDay(accountId, day, sw, { list, total });
        totalRecords += list.length;
      }
    }
    return { total: totalRecords, data: [] };
  }

  async incrementalSyncStorageReportLocalDetail(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storageReportLocalDetail',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllStorageReportLocalDetail(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 库存报表-海外仓-新报表-汇总
   * POST /inventory/center/openapi/storageReport/overseas/aggregate/list 令牌桶 3
   */
  async getStorageReportOverseasAggregate(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填');
    const body = { start_date: params.start_date, end_date: params.end_date };
    if (params.sys_wid != null && String(params.sys_wid).trim() !== '') body.sys_wid = String(params.sys_wid).trim();
    const res = await this.post(account, '/inventory/center/openapi/storageReport/overseas/aggregate/list', body, { successCode: [0, 200, '200'] });
    if (res.code !== 0 && res.code !== 200 && res.code !== '200') throw new Error(res.message || '海外仓汇总失败');
    return { data: res.data || [], total: (res.data || []).length };
  }

  async saveStorageReportOverseasAggregateForDay(accountId, day, sysWid, data) {
    if (!prisma.lingXingStorageReportOverseasAggregate) return;
    const sw = sysWid != null ? String(sysWid).trim() : '';
    await prisma.lingXingStorageReportOverseasAggregate.updateMany({
      where: { accountId, startDate: day, endDate: day, sysWid: sw },
      data: { archived: true, updatedAt: new Date() }
    });
    await prisma.lingXingStorageReportOverseasAggregate.create({
      data: { accountId, startDate: day, endDate: day, sysWid: sw, data: data || [], archived: false }
    });
  }

  async fetchAllStorageReportOverseasAggregate(accountId, params = {}, options = {}) {
    const { start_date, end_date } = params;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const days = this._getDaysBetween(start_date, end_date);
    let sysWidList = [''];
    if (params.sys_wid !== undefined && params.sys_wid !== null && String(params.sys_wid).trim() !== '') {
      sysWidList = [String(params.sys_wid).trim()];
    } else {
      const warehouses = await prisma.lingXingWarehouse.findMany({
        where: { accountId, type: 3, archived: false },
        select: { wid: true }
      });
      if (warehouses.length > 0) sysWidList = [warehouses.map(w => String(w.wid)).join(',')];
    }
    let totalRecords = 0;
    for (const day of days) {
      for (const sw of sysWidList) {
        const result = await this.getStorageReportOverseasAggregate(accountId, { start_date: day, end_date: day, sys_wid: sw || undefined });
        await this.saveStorageReportOverseasAggregateForDay(accountId, day, sw, result.data);
        totalRecords += result.data.length;
      }
    }
    return { total: totalRecords, data: [] };
  }

  async incrementalSyncStorageReportOverseasAggregate(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storageReportOverseasAggregate',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllStorageReportOverseasAggregate(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 库存报表-海外仓-新报表-明细（分页）
   * POST /inventory/center/openapi/storageReport/overseas/detail/page 令牌桶 3
   */
  async getStorageReportOverseasDetailPage(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填');
    const body = {
      offset: params.offset != null ? params.offset : 1,
      length: params.length != null ? params.length : 500,
      start_date: params.start_date,
      end_date: params.end_date
    };
    if (params.sys_wid != null && String(params.sys_wid).trim() !== '') body.sys_wid = String(params.sys_wid).trim();
    const res = await this.post(account, '/inventory/center/openapi/storageReport/overseas/detail/page', body, { successCode: [0, 200, '200'] });
    if (res.code !== 0 && res.code !== 200 && res.code !== '200') throw new Error(res.message || '海外仓明细失败');
    return { data: res.data || [], total: res.total ?? 0 };
  }

  async saveStorageReportOverseasDetailForDay(accountId, day, sysWid, payload) {
    if (!prisma.lingXingStorageReportOverseasDetail) return;
    const sw = sysWid != null ? String(sysWid).trim() : '';
    await prisma.lingXingStorageReportOverseasDetail.updateMany({
      where: { accountId, startDate: day, endDate: day, sysWid: sw },
      data: { archived: true, updatedAt: new Date() }
    });
    await prisma.lingXingStorageReportOverseasDetail.create({
      data: { accountId, startDate: day, endDate: day, sysWid: sw, data: payload, archived: false }
    });
  }

  async fetchAllStorageReportOverseasDetail(accountId, params = {}, options = {}) {
    const { start_date, end_date } = params;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const pageSize = Math.min(options.pageSize || 500, 500);
    const days = this._getDaysBetween(start_date, end_date);
    let sysWidList = [''];
    if (params.sys_wid !== undefined && params.sys_wid !== null && String(params.sys_wid).trim() !== '') {
      sysWidList = [String(params.sys_wid).trim()];
    } else {
      const warehouses = await prisma.lingXingWarehouse.findMany({
        where: { accountId, type: 3, archived: false },
        select: { wid: true }
      });
      if (warehouses.length > 0) sysWidList = [warehouses.map(w => String(w.wid)).join(',')];
    }
    let totalRecords = 0;
    for (const day of days) {
      for (const sw of sysWidList) {
        let offset = 1;
        let total = 0;
        const list = [];
        let page;
        do {
          page = await this.getStorageReportOverseasDetailPage(accountId, { start_date: day, end_date: day, sys_wid: sw || undefined, offset, length: pageSize });
          list.push(...(page.data || []));
          total = page.total ?? 0;
          offset += 1;
        } while (list.length < total && (page.data || []).length === pageSize);
        await this.saveStorageReportOverseasDetailForDay(accountId, day, sw, { list, total });
        totalRecords += list.length;
      }
    }
    return { total: totalRecords, data: [] };
  }

  async incrementalSyncStorageReportOverseasDetail(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storageReportOverseasDetail',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllStorageReportOverseasDetail(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 库存报表-FBA-新版-汇总（按月，seller_id 必填）
   * POST /cost/center/openApi/fba/gather/query 令牌桶 10
   */
  async getStorageReportFbaGather(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填（格式 Y-m）');
    if (!params.seller_id || !Array.isArray(params.seller_id) || params.seller_id.length === 0) throw new Error('seller_id 为必填数组');
    const body = {
      start_date: params.start_date,
      end_date: params.end_date,
      seller_id: params.seller_id,
      offset: params.offset != null ? params.offset : 0,
      length: params.length != null ? params.length : 2100
    };
    const res = await this.post(account, '/cost/center/openApi/fba/gather/query', body, { successCode: [0, 200, '200'] });
    if (res.code !== 0 && res.code !== 200 && res.code !== '200') throw new Error(res.msg || res.message || 'FBA汇总失败');
    const data = res.data || {};
    return { data, row_data: data.row_data || [], total: data.total ?? 0 };
  }

  async saveStorageReportFbaGatherForMonth(accountId, month, sellerId, payload) {
    if (!prisma.lingXingStorageReportFbaGather) return;
    const sid = sellerId != null ? String(sellerId).trim() : '';
    await prisma.lingXingStorageReportFbaGather.updateMany({
      where: { accountId, startDate: month, endDate: month, sellerId: sid },
      data: { archived: true, updatedAt: new Date() }
    });
    await prisma.lingXingStorageReportFbaGather.create({
      data: { accountId, startDate: month, endDate: month, sellerId: sid, data: payload || {}, archived: false }
    });
  }

  async fetchAllStorageReportFbaGather(accountId, params = {}, options = {}) {
    const { start_date, end_date } = params;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const months = this._getMonthsBetween(start_date, end_date);
    let sellerIdList = [];
    if (params.seller_id && Array.isArray(params.seller_id) && params.seller_id.length > 0) {
      sellerIdList = [params.seller_id.join(',')];
    } else {
      const sellers = await prisma.lingXingSeller.findMany({
        where: { accountId, archived: false },
        select: { sellerId: true }
      });
      const ids = (sellers || []).map(s => s.sellerId).filter(Boolean);
      if (ids.length > 0) sellerIdList = [ids.join(',')];
      else return { total: 0, data: [] };
    }
    let totalRecords = 0;
    for (const month of months) {
      for (const sidStr of sellerIdList) {
        const sellerIdArr = sidStr ? sidStr.split(',').map(s => s.trim()).filter(Boolean) : [];
        if (sellerIdArr.length === 0) continue;
        const result = await this.getStorageReportFbaGather(accountId, { start_date: month, end_date: month, seller_id: sellerIdArr });
        await this.saveStorageReportFbaGatherForMonth(accountId, month, sidStr, result.data);
        totalRecords += (result.row_data || []).length;
      }
    }
    return { total: totalRecords, data: [] };
  }

  async incrementalSyncStorageReportFbaGather(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storageReportFbaGather',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllStorageReportFbaGather(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 库存报表-FBA-新版-明细（按月，分页）
   * POST /cost/center/openApi/fba/detail/query 令牌桶 10
   */
  async getStorageReportFbaDetailPage(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填（Y-m）');
    if (!params.seller_id || !Array.isArray(params.seller_id) || params.seller_id.length === 0) throw new Error('seller_id 为必填数组');
    const body = {
      start_date: params.start_date,
      end_date: params.end_date,
      seller_id: params.seller_id,
      offset: params.offset != null ? params.offset : 0,
      length: params.length != null ? params.length : 500
    };
    const res = await this.post(account, '/cost/center/openApi/fba/detail/query', body, { successCode: [0, 200, '200'] });
    if (res.code !== 0 && res.code !== 200 && res.code !== '200') throw new Error(res.msg || res.message || 'FBA明细失败');
    const data = res.data || {};
    return { data: data.row_data || [], total: data.total ?? 0 };
  }

  async saveStorageReportFbaDetailForMonth(accountId, month, sellerId, payload) {
    if (!prisma.lingXingStorageReportFbaDetail) return;
    const sid = sellerId != null ? String(sellerId).trim() : '';
    await prisma.lingXingStorageReportFbaDetail.updateMany({
      where: { accountId, startDate: month, endDate: month, sellerId: sid },
      data: { archived: true, updatedAt: new Date() }
    });
    await prisma.lingXingStorageReportFbaDetail.create({
      data: { accountId, startDate: month, endDate: month, sellerId: sid, data: payload || {}, archived: false }
    });
  }

  async fetchAllStorageReportFbaDetail(accountId, params = {}, options = {}) {
    const { start_date, end_date } = params;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const months = this._getMonthsBetween(start_date, end_date);
    const pageSize = Math.min(options.pageSize || 500, 2100);
    let sellerIdList = [];
    if (params.seller_id && Array.isArray(params.seller_id) && params.seller_id.length > 0) {
      sellerIdList = [params.seller_id.join(',')];
    } else {
      const sellers = await prisma.lingXingSeller.findMany({
        where: { accountId, archived: false },
        select: { sellerId: true }
      });
      const ids = (sellers || []).map(s => s.sellerId).filter(Boolean);
      if (ids.length > 0) sellerIdList = [ids.join(',')];
      else return { total: 0, data: [] };
    }
    let totalRecords = 0;
    for (const month of months) {
      for (const sidStr of sellerIdList) {
        const sellerIdArr = sidStr ? sidStr.split(',').map(s => s.trim()).filter(Boolean) : [];
        if (sellerIdArr.length === 0) continue;
        let offset = 0;
        let total = 0;
        const list = [];
        let page;
        do {
          page = await this.getStorageReportFbaDetailPage(accountId, { start_date: month, end_date: month, seller_id: sellerIdArr, offset, length: pageSize });
          list.push(...(page.data || []));
          total = page.total ?? 0;
          offset += pageSize;
        } while (list.length < total && (page.data || []).length === pageSize);
        await this.saveStorageReportFbaDetailForMonth(accountId, month, sidStr, { list, total });
        totalRecords += list.length;
      }
    }
    return { total: totalRecords, data: [] };
  }

  async incrementalSyncStorageReportFbaDetail(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'storageReportFbaDetail',
      { defaultLookbackDays: Math.min(options.defaultLookbackDays ?? 90, 90), ...options },
      async (id, params, opts) => this.fetchAllStorageReportFbaDetail(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }
}

export default new LingXingReportService();

