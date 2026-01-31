import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';

/**
 * 领星ERP物流服务
 * 头程物流相关接口
 */
class LingXingLogisticsService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询头程物流渠道列表
   * API: POST /erp/sc/data/local_inventory/channelList
   * 支持查询【物流】>【头程物流】>【物流渠道】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（必填）
   *   - length: 分页长度（必填）
   * @returns {Promise<Object>} 物流渠道列表数据 { data: [], total: 0 }
   */
  async getChannelList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.offset === undefined || params.length === undefined) {
        throw new Error('offset 和 length 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        offset: params.offset,
        length: params.length
      };

      // 调用API获取物流渠道列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/data/local_inventory/channelList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取物流渠道列表失败');
      }

      const channels = response.data || [];
      const total = response.total || 0;

      // 保存物流渠道列表到数据库
      if (channels.length > 0) {
        await this.saveChannels(accountId, channels);
      }

      return {
        data: channels,
        total: total
      };
    } catch (error) {
      console.error('获取物流渠道列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存物流渠道列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} channels - 物流渠道列表数据
   */
  async saveChannels(accountId, channels) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingLogisticsChannel) {
        console.error('Prisma Client 中未找到 lingXingLogisticsChannel 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const channel of channels) {
        if (!channel.id) {
          continue;
        }

        // 处理 channel id（可能是字符串或数字）
        const channelId = typeof channel.id === 'string' ? BigInt(channel.id) : BigInt(channel.id);
        
        // 处理 provider id（可能是字符串或数字）
        const providerId = channel.provider && channel.provider.id 
          ? (typeof channel.provider.id === 'string' ? channel.provider.id : String(channel.provider.id))
          : null;

        await prisma.lingXingLogisticsChannel.upsert({
          where: {
            channelId: channelId
          },
          update: {
            accountId: accountId,
            channelName: channel.channel_name || null,
            methodId: channel.method_id || null,
            methodName: channel.method_name || null,
            billingType: channel.billing_type !== undefined && channel.billing_type !== null ? parseInt(channel.billing_type) : null,
            volumeCalcParam: channel.volume_calc_param !== undefined && channel.volume_calc_param !== null ? String(channel.volume_calc_param) : null,
            zipCode: channel.zip_code || null,
            validPeriod: channel.valid_period !== undefined && channel.valid_period !== null ? parseInt(channel.valid_period) : null,
            remark: channel.remark || null,
            enabled: channel.enabled !== undefined && channel.enabled !== null ? parseInt(channel.enabled) : null,
            lastModifyUid: channel.last_modify_uid !== undefined && channel.last_modify_uid !== null ? BigInt(channel.last_modify_uid) : null,
            gmtModified: channel.gmt_modified || null,
            providerId: providerId,
            providerName: channel.provider && channel.provider.logistics_provider_name ? channel.provider.logistics_provider_name : null,
            freight: channel.freight || null,
            sendPlaceCodes: channel.send_place_codes || null,
            receiveCountryCodes: channel.receive_country_codes || null,
            isIncludeTax: channel.is_include_tax !== undefined && channel.is_include_tax !== null ? parseInt(channel.is_include_tax) : null,
            isPointsBehind: channel.is_points_behind !== undefined && channel.is_points_behind !== null ? parseInt(channel.is_points_behind) : null,
            pointsBehindCoeffient: channel.points_behind_coeffient !== undefined && channel.points_behind_coeffient !== null ? parseFloat(channel.points_behind_coeffient) : null,
            channelData: channel, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            channelId: channelId,
            accountId: accountId,
            channelName: channel.channel_name || null,
            methodId: channel.method_id || null,
            methodName: channel.method_name || null,
            billingType: channel.billing_type !== undefined && channel.billing_type !== null ? parseInt(channel.billing_type) : null,
            volumeCalcParam: channel.volume_calc_param !== undefined && channel.volume_calc_param !== null ? String(channel.volume_calc_param) : null,
            zipCode: channel.zip_code || null,
            validPeriod: channel.valid_period !== undefined && channel.valid_period !== null ? parseInt(channel.valid_period) : null,
            remark: channel.remark || null,
            enabled: channel.enabled !== undefined && channel.enabled !== null ? parseInt(channel.enabled) : null,
            lastModifyUid: channel.last_modify_uid !== undefined && channel.last_modify_uid !== null ? BigInt(channel.last_modify_uid) : null,
            gmtModified: channel.gmt_modified || null,
            providerId: providerId,
            providerName: channel.provider && channel.provider.logistics_provider_name ? channel.provider.logistics_provider_name : null,
            freight: channel.freight || null,
            sendPlaceCodes: channel.send_place_codes || null,
            receiveCountryCodes: channel.receive_country_codes || null,
            isIncludeTax: channel.is_include_tax !== undefined && channel.is_include_tax !== null ? parseInt(channel.is_include_tax) : null,
            isPointsBehind: channel.is_points_behind !== undefined && channel.is_points_behind !== null ? parseInt(channel.is_points_behind) : null,
            pointsBehindCoeffient: channel.points_behind_coeffient !== undefined && channel.points_behind_coeffient !== null ? parseFloat(channel.points_behind_coeffient) : null,
            channelData: channel // 保存完整数据
          }
        });
      }

      console.log(`物流渠道列表已保存到数据库: 共 ${channels.length} 个物流渠道`);
    } catch (error) {
      console.error('保存物流渠道列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有物流渠道列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，上限500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { channels: [], total: 0, stats: {} }
   */
  async fetchAllChannels(accountId, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有物流渠道列表...');

      const allChannels = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 500);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页物流渠道（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getChannelList(accountId, {
            offset: offset,
            length: actualPageSize
          });

          const pageChannels = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个物流渠道，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allChannels.push(...pageChannels);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageChannels.length} 个物流渠道，累计 ${allChannels.length} 个物流渠道`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allChannels.length, totalCount);
          }

          if (pageChannels.length < actualPageSize || allChannels.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页物流渠道失败:`, error.message);
          if (allChannels.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有物流渠道列表获取完成，共 ${allChannels.length} 个物流渠道`);

      return {
        channels: allChannels,
        total: allChannels.length,
        stats: {
          totalChannels: allChannels.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有物流渠道列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询头程物流商列表
   * API: POST /basicOpen/logistics/headLogisticsProvider/query/list
   * 支持查询【物流】>【头程物流商】数据，默认返回已启用现结api对接的物流商
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - search: 搜索参数对象（必填）
   *     - length: 分页长度，每页显示的记录数（必填）
   *     - page: 页码，从1开始（必填）
   *     - enabled: 启用状态，枚举值：0-禁用, 1-启用，默认启用（可选）
   *     - isAuth: 是否api对接，枚举值：0-否, 1-是，默认是（可选）
   *     - payMethod: 结算方式，枚举值：1-现结, 2-月结，默认现结（可选）
   *     - searchField: 搜索字段，指定搜索的目标字段名称，code 代码，name 物流商，默认物流商（可选）
   *     - searchValue: 搜索值，用于模糊搜索物流商名称、编码等（可选）
   * @returns {Promise<Object>} 物流商列表数据 { data: [], total: 0 }
   */
  async getHeadLogisticsProviderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.search || params.search.length === undefined || params.search.page === undefined) {
        throw new Error('search.length 和 search.page 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        search: {
          length: params.search.length,
          page: params.search.page,
          ...(params.search.enabled !== undefined && { enabled: params.search.enabled }),
          ...(params.search.isAuth !== undefined && { isAuth: params.search.isAuth }),
          ...(params.search.payMethod !== undefined && { payMethod: params.search.payMethod }),
          ...(params.search.searchField && { searchField: params.search.searchField }),
          ...(params.search.searchValue && { searchValue: params.search.searchValue })
        }
      };

      // 调用API获取物流商列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/basicOpen/logistics/headLogisticsProvider/query/list', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取物流商列表失败');
      }

      // 注意：这个接口返回的数据结构是 { data: { total: 3, providers: [...] } }
      const responseData = response.data || {};
      const providers = responseData.providers || [];
      const total = responseData.total || response.total || 0;

      // 保存物流商列表到数据库
      if (providers.length > 0) {
        await this.saveHeadLogisticsProviders(accountId, providers);
      }

      return {
        data: providers,
        total: total
      };
    } catch (error) {
      console.error('获取物流商列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存头程物流商列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} providers - 物流商列表数据
   */
  async saveHeadLogisticsProviders(accountId, providers) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingHeadLogisticsProvider) {
        console.error('Prisma Client 中未找到 lingXingHeadLogisticsProvider 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const provider of providers) {
        if (!provider.providerId) {
          continue;
        }

        // 处理 creatorId（可能是 long 类型，需要转换为 BigInt）
        const creatorIdValue = provider.creatorId !== undefined && provider.creatorId !== null 
          ? (typeof provider.creatorId === 'string' ? BigInt(provider.creatorId) : BigInt(provider.creatorId))
          : null;

        // 处理 createdAt（Unix时间戳，秒）
        let createdAtValue = null;
        if (provider.createdAt !== undefined && provider.createdAt !== null) {
          // 如果是秒级时间戳，转换为毫秒
          const timestamp = typeof provider.createdAt === 'string' ? parseInt(provider.createdAt) : provider.createdAt;
          createdAtValue = new Date(timestamp * 1000).toISOString();
        }

        await prisma.lingXingHeadLogisticsProvider.upsert({
          where: {
            providerId: provider.providerId
          },
          update: {
            accountId: accountId,
            name: provider.name || null,
            code: provider.code || null,
            enabled: provider.enabled !== undefined && provider.enabled !== null ? parseInt(provider.enabled) : null,
            logisticsType: provider.logisticsType !== undefined && provider.logisticsType !== null ? parseInt(provider.logisticsType) : null,
            isAuth: provider.isAuth !== undefined && provider.isAuth !== null ? parseInt(provider.isAuth) : null,
            supplierCode: provider.supplierCode !== undefined && provider.supplierCode !== null ? (typeof provider.supplierCode === 'string' ? provider.supplierCode : String(provider.supplierCode)) : null,
            supplierName: provider.supplierName || null,
            status: provider.status !== undefined && provider.status !== null ? parseInt(provider.status) : null,
            remark: provider.remark || null,
            payMethod: provider.payMethod !== undefined && provider.payMethod !== null ? parseInt(provider.payMethod) : null,
            contactName: provider.contactName || null,
            contactPhone: provider.contactPhone || null,
            creatorId: creatorIdValue,
            creatorName: provider.creatorName || null,
            createdAt: createdAtValue,
            providerData: provider, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            providerId: provider.providerId,
            accountId: accountId,
            name: provider.name || null,
            code: provider.code || null,
            enabled: provider.enabled !== undefined && provider.enabled !== null ? parseInt(provider.enabled) : null,
            logisticsType: provider.logisticsType !== undefined && provider.logisticsType !== null ? parseInt(provider.logisticsType) : null,
            isAuth: provider.isAuth !== undefined && provider.isAuth !== null ? parseInt(provider.isAuth) : null,
            supplierCode: provider.supplierCode !== undefined && provider.supplierCode !== null ? (typeof provider.supplierCode === 'string' ? provider.supplierCode : String(provider.supplierCode)) : null,
            supplierName: provider.supplierName || null,
            status: provider.status !== undefined && provider.status !== null ? parseInt(provider.status) : null,
            remark: provider.remark || null,
            payMethod: provider.payMethod !== undefined && provider.payMethod !== null ? parseInt(provider.payMethod) : null,
            contactName: provider.contactName || null,
            contactPhone: provider.contactPhone || null,
            creatorId: creatorIdValue,
            creatorName: provider.creatorName || null,
            createdAt: createdAtValue,
            providerData: provider // 保存完整数据
          }
        });
      }

      console.log(`物流商列表已保存到数据库: 共 ${providers.length} 个物流商`);
    } catch (error) {
      console.error('保存物流商列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有头程物流商列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} searchParams - 搜索参数（可选）
   *   - enabled: 启用状态（可选）
   *   - isAuth: 是否api对接（可选）
   *   - payMethod: 结算方式（可选）
   *   - searchField: 搜索字段（可选）
   *   - searchValue: 搜索值（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { providers: [], total: 0, stats: {} }
   */
  async fetchAllHeadLogisticsProviders(accountId, searchParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有头程物流商列表...');

      const allProviders = [];
      let currentPage = 1;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取第 ${currentPage} 页物流商（page: ${currentPage}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getHeadLogisticsProviderList(accountId, {
            search: {
              ...searchParams,
              page: currentPage,
              length: pageSize
            }
          });

          const pageProviders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个物流商，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allProviders.push(...pageProviders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageProviders.length} 个物流商，累计 ${allProviders.length} 个物流商`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / pageSize), allProviders.length, totalCount);
          }

          if (pageProviders.length < pageSize || allProviders.length >= totalCount) {
            hasMore = false;
          } else {
            currentPage++;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页物流商失败:`, error.message);
          if (allProviders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有物流商列表获取完成，共 ${allProviders.length} 个物流商`);

      return {
        providers: allProviders,
        total: allProviders.length,
        stats: {
          totalProviders: allProviders.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有物流商列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询已启用的自发货物流方式列表
   * API: POST /erp/sc/routing/wms/WmsLogistics/listUsedLogisticsType
   * 支持查询【物流】>【物流管理】当中的 API 物流、三方仓物流、平台物流、自定义物流列表
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - provider_type: 物流商类型（必填）：0 API物流，1 自定义物流，2 海外仓物流，4 平台物流
   *   - page: 分页页码（可选，默认1）
   *   - length: 分页长度（可选，默认20）
   * @returns {Promise<Object>} 物流方式列表数据 { data: [], total: 0 }
   */
  async getUsedLogisticsTypeList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.provider_type === undefined) {
        throw new Error('provider_type 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        param: {
          provider_type: params.provider_type,
          ...(params.page !== undefined && { page: params.page }),
          ...(params.length !== undefined && { length: params.length })
        }
      };

      // 调用API获取物流方式列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/wms/WmsLogistics/listUsedLogisticsType', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取物流方式列表失败');
      }

      // 获取返回数据
      const logisticsTypes = response.data || [];
      const total = response.total || 0;

      // 保存物流方式列表到数据库
      if (logisticsTypes.length > 0) {
        await this.saveUsedLogisticsTypes(accountId, logisticsTypes, params.provider_type);
      }

      return {
        data: logisticsTypes,
        total: total
      };
    } catch (error) {
      console.error('获取物流方式列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存已启用的自发货物流方式列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} logisticsTypes - 物流方式列表数据
   * @param {number} providerType - 物流商类型
   */
  async saveUsedLogisticsTypes(accountId, logisticsTypes, providerType) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingUsedLogisticsType) {
        console.error('Prisma Client 中未找到 lingXingUsedLogisticsType 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const logisticsType of logisticsTypes) {
        if (!logisticsType.type_id) {
          continue;
        }

        // 处理 type_id（可能是字符串或数字）
        const typeIdValue = typeof logisticsType.type_id === 'string' 
          ? logisticsType.type_id 
          : String(logisticsType.type_id);

        // 处理 logistics_provider_id（可能是字符串或数字）
        const providerIdValue = logisticsType.logistics_provider_id !== undefined && logisticsType.logistics_provider_id !== null
          ? (typeof logisticsType.logistics_provider_id === 'string' 
              ? logisticsType.logistics_provider_id 
              : String(logisticsType.logistics_provider_id))
          : null;

        // 处理 updated_at（Unix时间戳，秒）
        let updatedAtValue = null;
        if (logisticsType.updated_at !== undefined && logisticsType.updated_at !== null) {
          // 如果是秒级时间戳，转换为毫秒
          const timestamp = typeof logisticsType.updated_at === 'string' ? parseInt(logisticsType.updated_at) : logisticsType.updated_at;
          updatedAtValue = new Date(timestamp * 1000).toISOString();
        }

        // 处理 fee_template_id（可能是大整数）
        const feeTemplateIdValue = logisticsType.fee_template_id !== undefined && logisticsType.fee_template_id !== null
          ? (typeof logisticsType.fee_template_id === 'string' 
              ? BigInt(logisticsType.fee_template_id) 
              : BigInt(logisticsType.fee_template_id))
          : null;

        // 处理 wid（可能是大整数）
        const widValue = logisticsType.wid !== undefined && logisticsType.wid !== null
          ? (typeof logisticsType.wid === 'string' 
              ? BigInt(logisticsType.wid) 
              : BigInt(logisticsType.wid))
          : null;

        // 处理 supplier_id（可能是大整数）
        const supplierIdValue = logisticsType.supplier_id !== undefined && logisticsType.supplier_id !== null
          ? (typeof logisticsType.supplier_id === 'string' 
              ? BigInt(logisticsType.supplier_id) 
              : BigInt(logisticsType.supplier_id))
          : null;

        await prisma.lingXingUsedLogisticsType.upsert({
          where: {
            accountId_typeId: {
              accountId: accountId,
              typeId: typeIdValue
            }
          },
          update: {
            providerType: providerType,
            name: logisticsType.name || null,
            code: logisticsType.code || null,
            isUsed: logisticsType.is_used !== undefined && logisticsType.is_used !== null ? parseInt(logisticsType.is_used) : null,
            logisticsProviderId: providerIdValue,
            logisticsProviderName: logisticsType.logistics_provider_name || null,
            providerIsUsed: logisticsType.provider_is_used !== undefined && logisticsType.provider_is_used !== null ? parseInt(logisticsType.provider_is_used) : null,
            feeTemplateId: feeTemplateIdValue,
            updatedAt: updatedAtValue,
            warehouseType: logisticsType.warehouse_type !== undefined && logisticsType.warehouse_type !== null ? parseInt(logisticsType.warehouse_type) : null,
            supplierId: supplierIdValue,
            supplierCode: logisticsType.supplier_code || null,
            isPlatformProvider: logisticsType.is_platform_provider !== undefined && logisticsType.is_platform_provider !== null ? parseInt(logisticsType.is_platform_provider) : null,
            isSupportDomesticProvider: logisticsType.is_support_domestic_provider !== undefined ? Boolean(logisticsType.is_support_domestic_provider) : null,
            isNeedMarking: logisticsType.is_need_marking !== undefined ? Boolean(logisticsType.is_need_marking) : null,
            isCombineChannel: logisticsType.is_combine_channel !== undefined ? Boolean(logisticsType.is_combine_channel) : null,
            orderType: logisticsType.order_type !== undefined && logisticsType.order_type !== null ? parseInt(logisticsType.order_type) : null,
            billingType: logisticsType.billing_type !== undefined && logisticsType.billing_type !== null ? parseInt(logisticsType.billing_type) : null,
            volumeParam: logisticsType.volume_param !== undefined && logisticsType.volume_param !== null ? parseInt(logisticsType.volume_param) : null,
            wpCode: logisticsType.wp_code || null,
            wid: widValue,
            logisticsTypeData: logisticsType, // 保存完整数据
            updatedAtDb: new Date()
          },
          create: {
            accountId: accountId,
            typeId: typeIdValue,
            providerType: providerType,
            name: logisticsType.name || null,
            code: logisticsType.code || null,
            isUsed: logisticsType.is_used !== undefined && logisticsType.is_used !== null ? parseInt(logisticsType.is_used) : null,
            logisticsProviderId: providerIdValue,
            logisticsProviderName: logisticsType.logistics_provider_name || null,
            providerIsUsed: logisticsType.provider_is_used !== undefined && logisticsType.provider_is_used !== null ? parseInt(logisticsType.provider_is_used) : null,
            feeTemplateId: feeTemplateIdValue,
            updatedAt: updatedAtValue,
            warehouseType: logisticsType.warehouse_type !== undefined && logisticsType.warehouse_type !== null ? parseInt(logisticsType.warehouse_type) : null,
            supplierId: supplierIdValue,
            supplierCode: logisticsType.supplier_code || null,
            isPlatformProvider: logisticsType.is_platform_provider !== undefined && logisticsType.is_platform_provider !== null ? parseInt(logisticsType.is_platform_provider) : null,
            isSupportDomesticProvider: logisticsType.is_support_domestic_provider !== undefined ? Boolean(logisticsType.is_support_domestic_provider) : null,
            isNeedMarking: logisticsType.is_need_marking !== undefined ? Boolean(logisticsType.is_need_marking) : null,
            isCombineChannel: logisticsType.is_combine_channel !== undefined ? Boolean(logisticsType.is_combine_channel) : null,
            orderType: logisticsType.order_type !== undefined && logisticsType.order_type !== null ? parseInt(logisticsType.order_type) : null,
            billingType: logisticsType.billing_type !== undefined && logisticsType.billing_type !== null ? parseInt(logisticsType.billing_type) : null,
            volumeParam: logisticsType.volume_param !== undefined && logisticsType.volume_param !== null ? parseInt(logisticsType.volume_param) : null,
            wpCode: logisticsType.wp_code || null,
            wid: widValue,
            logisticsTypeData: logisticsType // 保存完整数据
          }
        });
      }

      console.log(`物流方式列表已保存到数据库: 共 ${logisticsTypes.length} 个物流方式`);
    } catch (error) {
      console.error('保存物流方式列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有已启用的自发货物流方式列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {number} providerType - 物流商类型（必填）：0 API物流，1 自定义物流，2 海外仓物流，4 平台物流
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { logisticsTypes: [], total: 0, stats: {} }
   */
  async fetchAllUsedLogisticsTypes(accountId, providerType, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log(`开始自动拉取所有已启用的自发货物流方式列表（物流商类型: ${providerType}）...`);

      const allLogisticsTypes = [];
      let currentPage = 1;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取第 ${currentPage} 页物流方式（page: ${currentPage}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getUsedLogisticsTypeList(accountId, {
            provider_type: providerType,
            page: currentPage,
            length: pageSize
          });

          const pageLogisticsTypes = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个物流方式，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allLogisticsTypes.push(...pageLogisticsTypes);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageLogisticsTypes.length} 个物流方式，累计 ${allLogisticsTypes.length} 个物流方式`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / pageSize), allLogisticsTypes.length, totalCount);
          }

          if (pageLogisticsTypes.length < pageSize || allLogisticsTypes.length >= totalCount) {
            hasMore = false;
          } else {
            currentPage++;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页物流方式失败:`, error.message);
          if (allLogisticsTypes.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有物流方式列表获取完成，共 ${allLogisticsTypes.length} 个物流方式`);

      return {
        logisticsTypes: allLogisticsTypes,
        total: allLogisticsTypes.length,
        stats: {
          totalLogisticsTypes: allLogisticsTypes.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有物流方式列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询运输方式列表
   * API: POST /basicOpen/businessConfig/transportMethod/list
   * 支持查询【设置】>【业务配置】>【物流】当中的运输方式
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Object>} 运输方式列表数据 { data: [], total: 0 }
   */
  async getTransportMethodList(accountId) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 调用API获取运输方式列表（使用通用客户端，成功码为0，令牌桶容量为1）
      // 注意：这个接口不需要参数
      const response = await this.post(account, '/basicOpen/businessConfig/transportMethod/list', {}, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取运输方式列表失败');
      }

      // 获取返回数据
      const transportMethods = response.data || [];
      const total = response.total || 0;

      // 保存运输方式列表到数据库
      if (transportMethods.length > 0) {
        await this.saveTransportMethods(accountId, transportMethods);
      }

      return {
        data: transportMethods,
        total: total
      };
    } catch (error) {
      console.error('获取运输方式列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存运输方式列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} transportMethods - 运输方式列表数据
   */
  async saveTransportMethods(accountId, transportMethods) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingTransportMethod) {
        console.error('Prisma Client 中未找到 lingXingTransportMethod 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const method of transportMethods) {
        if (!method.method_id) {
          continue;
        }

        // 处理 method_id（可能是字符串或数字，可能是大整数）
        const methodIdValue = typeof method.method_id === 'string' 
          ? method.method_id 
          : String(method.method_id);

        // 处理 code（序号，可能是字符串或数字）
        const codeValue = method.code !== undefined && method.code !== null
          ? (typeof method.code === 'string' ? method.code : String(method.code))
          : null;

        // 处理 creator_id 和 updater_id（可能是大整数）
        const creatorIdValue = method.creator_id !== undefined && method.creator_id !== null
          ? (typeof method.creator_id === 'string' 
              ? BigInt(method.creator_id) 
              : BigInt(method.creator_id))
          : null;

        const updaterIdValue = method.updater_id !== undefined && method.updater_id !== null
          ? (typeof method.updater_id === 'string' 
              ? BigInt(method.updater_id) 
              : BigInt(method.updater_id))
          : null;

        // 处理 created_at 和 updated_at（Unix时间戳，秒）
        let createdAtValue = null;
        if (method.created_at !== undefined && method.created_at !== null) {
          // 如果是秒级时间戳，转换为毫秒
          const timestamp = typeof method.created_at === 'string' ? parseInt(method.created_at) : method.created_at;
          createdAtValue = new Date(timestamp * 1000).toISOString();
        }

        let updatedAtValue = null;
        if (method.updated_at !== undefined && method.updated_at !== null) {
          // 如果是秒级时间戳，转换为毫秒
          const timestamp = typeof method.updated_at === 'string' ? parseInt(method.updated_at) : method.updated_at;
          updatedAtValue = new Date(timestamp * 1000).toISOString();
        }

        await prisma.lingXingTransportMethod.upsert({
          where: {
            accountId_methodId: {
              accountId: accountId,
              methodId: methodIdValue
            }
          },
          update: {
            code: codeValue,
            name: method.name || null,
            isSystem: method.is_system !== undefined ? Boolean(method.is_system) : null,
            enabled: method.enabled !== undefined && method.enabled !== null ? parseInt(method.enabled) : null,
            remark: method.remark || null,
            creatorId: creatorIdValue,
            creatorName: method.creator_name || null,
            updaterId: updaterIdValue,
            updaterName: method.updater_name || null,
            createdAt: createdAtValue,
            updatedAt: updatedAtValue,
            transportMethodData: method, // 保存完整数据
            updatedAtDb: new Date()
          },
          create: {
            accountId: accountId,
            methodId: methodIdValue,
            code: codeValue,
            name: method.name || null,
            isSystem: method.is_system !== undefined ? Boolean(method.is_system) : null,
            enabled: method.enabled !== undefined && method.enabled !== null ? parseInt(method.enabled) : null,
            remark: method.remark || null,
            creatorId: creatorIdValue,
            creatorName: method.creator_name || null,
            updaterId: updaterIdValue,
            updaterName: method.updater_name || null,
            createdAt: createdAtValue,
            updatedAt: updatedAtValue,
            transportMethodData: method // 保存完整数据
          }
        });
      }

      console.log(`运输方式列表已保存到数据库: 共 ${transportMethods.length} 个运输方式`);
    } catch (error) {
      console.error('保存运输方式列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有运输方式列表
   * 注意：这个接口不需要分页参数，直接返回所有数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项（预留，当前不需要）
   * @returns {Promise<Object>} { transportMethods: [], total: 0 }
   */
  async fetchAllTransportMethods(accountId, options = {}) {
    try {
      console.log('开始自动拉取所有运输方式列表...');

      // 调用API获取所有运输方式列表（这个接口不需要分页参数，直接返回所有数据）
      const result = await this.getTransportMethodList(accountId);

      const transportMethods = result.data || [];
      const total = result.total || 0;

      console.log(`所有运输方式列表获取完成，共 ${transportMethods.length} 个运输方式`);

      return {
        transportMethods: transportMethods,
        total: total,
        stats: {
          totalTransportMethods: transportMethods.length
        }
      };
    } catch (error) {
      console.error('自动拉取所有运输方式列表失败:', error.message);
      throw error;
    }
  }
}

export default new LingXingLogisticsService();


