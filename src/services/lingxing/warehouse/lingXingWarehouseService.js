import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';

/**
 * 领星ERP仓库服务
 * 仓库相关接口
 */
class LingXingWarehouseService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询仓库列表
   * API: POST /erp/sc/data/local_inventory/warehouse
   * 支持查询【设置】>【仓库设置】仓库列表
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - type: 仓库类型（可选，默认1）：1 本地仓，3 海外仓，4 亚马逊平台仓，6 AWD仓
   *   - sub_type: 海外仓子类型（可选，只在type=3生效）：1 无API海外仓，2 有API海外仓
   *   - is_delete: 是否删除（可选，默认0）：0 未删除，1 已删除
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认1000）
   * @returns {Promise<Object>} 仓库列表数据 { data: [], total: 0 }
   */
  async getWarehouseList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.type !== undefined) {
        requestParams.type = params.type;
      }
      if (params.sub_type !== undefined) {
        requestParams.sub_type = params.sub_type;
      }
      if (params.is_delete !== undefined) {
        requestParams.is_delete = params.is_delete;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        requestParams.length = params.length;
      }

      // 调用API获取仓库列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/data/local_inventory/warehouse', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取仓库列表失败');
      }

      const warehouses = response.data || [];
      const total = response.total || 0;

      // 保存仓库列表到数据库
      if (warehouses.length > 0) {
        await this.saveWarehouses(accountId, warehouses);
      }

      return {
        data: warehouses,
        total: total
      };
    } catch (error) {
      console.error('获取仓库列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存仓库列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} warehouses - 仓库列表数据
   */
  async saveWarehouses(accountId, warehouses) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingWarehouse) {
        console.error('Prisma Client 中未找到 lingXingWarehouse 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const warehouse of warehouses) {
        if (!warehouse.wid) {
          continue;
        }

        // 处理 wid（系统仓库id，可能是字符串或数字，可能是大整数）
        const widValue = typeof warehouse.wid === 'string' 
          ? BigInt(warehouse.wid) 
          : BigInt(warehouse.wid);

        // 处理 type（仓库类型）
        const typeValue = warehouse.type !== undefined && warehouse.type !== null 
          ? parseInt(warehouse.type) 
          : null;

        // 处理 is_delete（是否删除，可能是字符串或数字）
        const isDeleteValue = warehouse.is_delete !== undefined && warehouse.is_delete !== null
          ? (typeof warehouse.is_delete === 'string' ? warehouse.is_delete : String(warehouse.is_delete))
          : null;

        // 处理 t_status（状态）
        const tStatusValue = warehouse.t_status !== undefined && warehouse.t_status !== null 
          ? parseInt(warehouse.t_status) 
          : null;

        // 处理 wp_id（服务商ID，可能是大整数）
        const wpIdValue = warehouse.wp_id !== undefined && warehouse.wp_id !== null
          ? (typeof warehouse.wp_id === 'string' 
              ? BigInt(warehouse.wp_id) 
              : BigInt(warehouse.wp_id))
          : null;

        await prisma.lingXingWarehouse.upsert({
          where: {
            accountId_wid: {
              accountId: accountId,
              wid: widValue
            }
          },
          update: {
            name: warehouse.name || null,
            type: typeValue,
            isDelete: isDeleteValue,
            tCountryAreaName: warehouse.t_country_area_name || null,
            tStatus: tStatusValue,
            tWarehouseCode: warehouse.t_warehouse_code || null,
            tWarehouseName: warehouse.t_warehouse_name || null,
            countryCode: warehouse.country_code || null,
            wpId: wpIdValue,
            wpName: warehouse.wp_name || null,
            warehouseData: warehouse, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            wid: widValue,
            name: warehouse.name || null,
            type: typeValue,
            isDelete: isDeleteValue,
            tCountryAreaName: warehouse.t_country_area_name || null,
            tStatus: tStatusValue,
            tWarehouseCode: warehouse.t_warehouse_code || null,
            tWarehouseName: warehouse.t_warehouse_name || null,
            countryCode: warehouse.country_code || null,
            wpId: wpIdValue,
            wpName: warehouse.wp_name || null,
            warehouseData: warehouse // 保存完整数据
          }
        });
      }

      console.log(`仓库列表已保存到数据库: 共 ${warehouses.length} 个仓库`);
    } catch (error) {
      console.error('保存仓库列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有仓库列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - type: 仓库类型（可选）
   *   - sub_type: 海外仓子类型（可选）
   *   - is_delete: 是否删除（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { warehouses: [], total: 0, stats: {} }
   */
  async fetchAllWarehouses(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有仓库列表...');

      const allWarehouses = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取仓库列表（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getWarehouseList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageWarehouses = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个仓库，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allWarehouses.push(...pageWarehouses);
          console.log(`获取完成，本页 ${pageWarehouses.length} 个仓库，累计 ${allWarehouses.length} 个仓库`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allWarehouses.length, totalCount);
          }

          if (pageWarehouses.length < pageSize || allWarehouses.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取仓库列表失败:`, error.message);
          if (allWarehouses.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有仓库列表获取完成，共 ${allWarehouses.length} 个仓库`);

      return {
        warehouses: allWarehouses,
        total: allWarehouses.length,
        stats: {
          totalWarehouses: allWarehouses.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有仓库列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询本地仓位列表
   * API: POST