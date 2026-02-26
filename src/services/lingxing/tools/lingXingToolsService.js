import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';
import { runAccountLevelIncrementalSync } from '../sync/lingXingIncrementalRunner.js';

/**
 * 领星ERP工具服务
 * 工具相关接口
 */
class LingXingToolsService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询关键词列表
   * API: POST /erp/sc/routing/tool/toolKeywordRank/getKeywordList
   * 支持查询关键词排名数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - mid: 国家id（可选）
   *   - start_date: 开始日期，格式：Y-m-d（可选）
   *   - end_date: 结束日期，格式：Y-m-d（可选）
   *   - offset: 分页偏移量，默认0（必填）
   *   - length: 分页长度，默认20，最大值为2000（必填）
   * @returns {Promise<Object>} 关键词列表数据 { data: [], total: 0 }
   */
  async getKeywordList(accountId, params = {}) {
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

      // 构建请求参数
      const requestParams = {
        offset: params.offset,
        length: params.length
      };

      // 添加可选参数
      if (params.mid !== undefined && params.mid !== null) {
        requestParams.mid = params.mid;
      }
      if (params.start_date !== undefined && params.start_date !== null) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined && params.end_date !== null) {
        requestParams.end_date = params.end_date;
      }

      // 调用API获取关键词列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/tool/toolKeywordRank/getKeywordList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取关键词列表失败');
      }

      const keywordList = response.data || [];
      const total = response.total || 0;

      return {
        data: keywordList,
        total: total
      };
    } catch (error) {
      console.error('获取关键词列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询运营日志(新)
   * API: POST /basicOpen/operateManage/operateLog/list/v2
   * 令牌桶容量: 1
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - offset, length, start_date, end_date (yyyy-mm-dd), summary_type (必填: asin/parent_asin/msku); 可选: sids(array), mids(array), search_field, search_value(array)
   * @returns {Promise<Object>} { data: { data: [] }, total }
   */
  async getOperateLogListV2(accountId, params = {}) {
    const account = await prisma.lingXingAccount.findUnique({ where: { id: accountId } });
    if (!account) throw new Error(`领星账户不存在: ${accountId}`);
    if (!params.start_date || !params.end_date) throw new Error('start_date、end_date 为必填，格式 yyyy-mm-dd');
    if (!params.summary_type) throw new Error('summary_type 为必填，可选: asin, parent_asin, msku');
    const requestParams = {
      start_date: params.start_date,
      end_date: params.end_date,
      summary_type: params.summary_type
    };
    if (params.offset !== undefined) requestParams.offset = Number(params.offset);
    if (params.length !== undefined) requestParams.length = Number(params.length);
    if (params.sids !== undefined) requestParams.sids = params.sids;
    if (params.mids !== undefined) requestParams.mids = params.mids;
    if (params.search_field !== undefined) requestParams.search_field = params.search_field;
    if (params.search_value !== undefined) requestParams.search_value = params.search_value;
    const response = await this.post(account, '/basicOpen/operateManage/operateLog/list/v2', requestParams, { successCode: [0, 200, '200'] });
    if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
      throw new Error(response.message || '查询运营日志失败');
    }
    const inner = response.data && response.data.data ? response.data.data : (response.data || []);
    const total = response.total ?? (Array.isArray(inner) ? inner.length : 0);
    return { data: response.data || { data: [] }, total };
  }

  /**
   * 按天+summaryType 保存运营日志（软删当日后插入一条）
   */
  async saveOperateLogForDay(accountId, eventDate, summaryType, payload) {
    const eventDateStr = String(eventDate || '').trim().slice(0, 10);
    const st = String(summaryType || 'msku');
    const existing = await prisma.lingXingOperateLog.findMany({
      where: { accountId, eventDate: eventDateStr, summaryType: st, archived: false },
      select: { id: true }
    });
    if (existing.length > 0) {
      await prisma.lingXingOperateLog.updateMany({
        where: { id: { in: existing.map(r => r.id) } },
        data: { archived: true, updatedAt: new Date() }
      });
    }
    await prisma.lingXingOperateLog.create({
      data: { accountId, eventDate: eventDateStr, summaryType: st, data: payload || {}, archived: false }
    });
  }

  /**
   * 按天+summaryType 拉取运营日志并保存（sids 从 DB 遍历）
   */
  async fetchAllOperateLogByDay(accountId, listParams = {}, options = {}) {
    const { start_date, end_date } = listParams;
    if (!start_date || !end_date) throw new Error('start_date、end_date 为必填');
    const pageSize = options.pageSize ?? 40;
    const delayBetweenDays = options.delayBetweenDays ?? 150;
    const summaryTypes = ['asin', 'parent_asin', 'msku'];
    let sids = options.sids;
    if (!sids || (Array.isArray(sids) && sids.length === 0)) {
      const sellers = await prisma.lingXingSeller.findMany({ where: { accountId, status: 1 }, select: { sid: true } });
      sids = sellers.map(s => String(s.sid)).filter(Boolean);
    }
    if (Array.isArray(sids) && sids.length === 0) sids = undefined;
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);
    if (startDate > endDate) return { total: 0, data: [] };
    let totalRecords = 0;
    const currentDate = new Date(startDate);
    while (currentDate <= endDate) {
      const dayStr = currentDate.toISOString().split('T')[0];
      for (const summaryType of summaryTypes) {
        let offset = 0;
        let allData = [];
        let hasMore = true;
        while (hasMore) {
          const res = await this.getOperateLogListV2(accountId, {
            start_date: dayStr,
            end_date: dayStr,
            summary_type: summaryType,
            offset,
            length: pageSize,
            ...(sids && { sids })
          });
          const inner = (res.data && res.data.data) ? res.data.data : (Array.isArray(res.data) ? res.data : []);
          const list = Array.isArray(inner) ? inner : [];
          allData = allData.concat(list);
          hasMore = list.length >= pageSize;
          if (hasMore) offset += pageSize;
        }
        await this.saveOperateLogForDay(accountId, dayStr, summaryType, { data: allData });
        totalRecords += allData.length;
      }
      currentDate.setDate(currentDate.getDate() + 1);
      if (delayBetweenDays > 0) await new Promise(r => setTimeout(r, delayBetweenDays));
    }
    return { total: totalRecords, data: [] };
  }

  /**
   * 运营日志(新) 增量同步（按天+summaryType 查询并保存，sids 从 DB 遍历）
   */
  async incrementalSyncOperateLog(accountId, options = {}) {
    const result = await runAccountLevelIncrementalSync(
      accountId,
      'operateLog',
      { defaultLookbackDays: options.defaultLookbackDays ?? 30, ...options },
      (id, params, opts) => this.fetchAllOperateLogByDay(id, params, opts)
    );
    return { results: [result], summary: { successCount: result.success ? 1 : 0, failCount: result.success ? 0 : 1, totalRecords: result.recordCount ?? 0 } };
  }

  /**
   * 自动拉取所有关键词列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - mid: 国家id（可选）
   *   - start_date: 开始日期（可选）
   *   - end_date: 结束日期（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认2000，最大2000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { keywordList: [], total: 0, stats: {} }
   */
  async fetchAllKeywords(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 2000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 2000
    const actualPageSize = Math.min(pageSize, 2000);

    try {
      console.log('开始自动拉取所有关键词列表...');

      const allKeywordList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取关键词列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getKeywordList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageKeywordList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条关键词记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allKeywordList.push(...pageKeywordList);
          console.log(`获取完成，本页 ${pageKeywordList.length} 条记录，累计 ${allKeywordList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allKeywordList.length, totalCount);
          }

          if (pageKeywordList.length < actualPageSize || allKeywordList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取关键词列表失败:`, error.message);
          if (allKeywordList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有关键词列表获取完成，共 ${allKeywordList.length} 条记录`);

      // 保存到数据库（同一次拉取视为全量：先归档当前筛选条件下的旧数据，再写入新数据）
      await this.saveKeywordRanks(accountId, allKeywordList, filterParams);

      return {
        keywordList: allKeywordList,
        total: allKeywordList.length,
        stats: {
          totalKeywords: allKeywordList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有关键词列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 将关键词列表保存到数据库
   * 按 accountId + 筛选条件（mid, start_date, end_date）视为一批：先归档该批旧数据，再批量写入新数据（archived: false）
   * @param {string} accountId - 领星账户ID
   * @param {Array<Object>} keywordList - 关键词列表（API 返回的 data 数组）
   * @param {Object} filterParams - 本次拉取使用的筛选参数 { mid?, start_date?, end_date? }
   */
  async saveKeywordRanks(accountId, keywordList, filterParams = {}) {
    if (!prisma.lingXingKeywordRank) {
      console.error('Prisma Client 中未找到 lingXingKeywordRank 模型');
      return;
    }

    const mid = filterParams.mid !== undefined && filterParams.mid !== null ? filterParams.mid : null;
    const startDate = filterParams.start_date !== undefined && filterParams.start_date !== null ? String(filterParams.start_date) : null;
    const endDate = filterParams.end_date !== undefined && filterParams.end_date !== null ? String(filterParams.end_date) : null;

    try {
      // 归档与本次筛选条件一致的历史数据（同一 accountId + mid + startDate + endDate）
      const where = {
        accountId,
        mid,
        startDate,
        endDate
      };
      await prisma.lingXingKeywordRank.updateMany({
        where,
        data: { archived: true, updatedAt: new Date() }
      });

      if (!keywordList || keywordList.length === 0) {
        console.log('关键词列表为空，仅完成归档，未写入新记录');
        return;
      }

      // 批量写入新数据（archived: false）
      await prisma.lingXingKeywordRank.createMany({
        data: keywordList.map(item => ({
          accountId,
          mid,
          startDate,
          endDate,
          data: item,
          archived: false
        }))
      });

      console.log(`关键词排名已保存到数据库: 共 ${keywordList.length} 条记录（accountId=${accountId}, mid=${mid ?? 'all'}, start_date=${startDate ?? 'all'}, end_date=${endDate ?? 'all'}）`);
    } catch (error) {
      console.error('保存关键词排名到数据库失败:', error.message);
      throw error;
    }
  }
}

export default new LingXingToolsService();

