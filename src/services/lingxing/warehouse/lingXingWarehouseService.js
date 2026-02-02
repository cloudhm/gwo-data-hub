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

      // 保存到数据库
      if (allWarehouses && allWarehouses.length > 0) {
        await this.saveWarehouses(accountId, allWarehouses);
      }

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
   * API: POST /erp/sc/routing/data/local_inventory/warehouseBin
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - wid: 仓库ID，字符串id，多个使用英文逗号分隔
   *   - id: 仓位ID，字符串id，多个使用英文逗号分隔
   *   - status: 仓位状态：1 禁用，2 启用
   *   - type: 仓位类型：5 可用，6 次品
   *   - offset: 分页偏移量，默认为0
   *   - limit: 限制条数，默认20条
   * @returns {Promise<Object>} 仓位列表数据 { data: [], total: 0 }
   */
  async getWarehouseBinList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.id !== undefined) {
        requestParams.id = params.id;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.type !== undefined) {
        requestParams.type = params.type;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.limit !== undefined) {
        requestParams.limit = params.limit;
      }

      // 调用API获取仓位列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/warehouseBin', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取仓位列表失败');
      }

      const bins = response.data || [];
      const total = response.total || 0;

      // 保存仓位列表到数据库
      if (bins.length > 0) {
        await this.saveWarehouseBins(accountId, bins);
      }

      return {
        data: bins,
        total: total
      };
    } catch (error) {
      console.error('获取仓位列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存仓位列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} bins - 仓位列表数据
   */
  async saveWarehouseBins(accountId, bins) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingWarehouseBin) {
        console.error('Prisma Client 中未找到 lingXingWarehouseBin 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const bin of bins) {
        if (!bin.id) {
          continue;
        }

        // 处理 id（仓位id，可能是字符串或数字，可能是大整数）
        const binIdValue = typeof bin.id === 'string' 
          ? BigInt(bin.id) 
          : BigInt(bin.id);

        // 处理 wid（仓库ID，可能是字符串或数字，可能是大整数）
        const widValue = bin.wid !== undefined && bin.wid !== null
          ? (typeof bin.wid === 'string' 
              ? BigInt(bin.wid) 
              : BigInt(bin.wid))
          : null;

        // 处理 status（仓位状态，可能是字符串或数字）
        const statusValue = bin.status !== undefined && bin.status !== null
          ? (typeof bin.status === 'string' ? bin.status : String(bin.status))
          : null;

        // 处理 type（仓位类型，可能是字符串或数字）
        const typeValue = bin.type !== undefined && bin.type !== null
          ? (typeof bin.type === 'string' ? bin.type : String(bin.type))
          : null;

        // 处理 storage_bin（仓位，可能是字符串或数字）
        const storageBinValue = bin.storage_bin !== undefined && bin.storage_bin !== null
          ? (typeof bin.storage_bin === 'string' ? bin.storage_bin : String(bin.storage_bin))
          : null;

        // 处理 whb_status（仓位状态，可能是字符串）
        const whbStatusValue = bin.whb_status !== undefined && bin.whb_status !== null
          ? String(bin.whb_status)
          : null;

        // 保存仓位基本信息
        await prisma.lingXingWarehouseBin.upsert({
          where: {
            accountId_binId: {
              accountId: accountId,
              binId: binIdValue
            }
          },
          update: {
            wid: widValue,
            warehouseName: bin.Ware_house_name || null,
            storageBin: storageBinValue,
            whbStatus: whbStatusValue,
            status: statusValue,
            type: typeValue,
            binData: bin, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            binId: binIdValue,
            wid: widValue,
            warehouseName: bin.Ware_house_name || null,
            storageBin: storageBinValue,
            whbStatus: whbStatusValue,
            status: statusValue,
            type: typeValue,
            binData: bin // 保存完整数据
          }
        });

        // 保存仓位商品关系（sku_fnsku）
        if (bin.sku_fnsku && Array.isArray(bin.sku_fnsku) && bin.sku_fnsku.length > 0) {
          await this.saveWarehouseBinSkus(accountId, binIdValue, bin.sku_fnsku);
        }
      }

      console.log(`仓位列表已保存到数据库: 共 ${bins.length} 个仓位`);
    } catch (error) {
      console.error('保存仓位列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 保存仓位商品关系到数据库
   * @param {string} accountId - 领星账户ID
   * @param {BigInt} binId - 仓位ID
   * @param {Array} skuFnsks - 仓位商品关系数组
   */
  async saveWarehouseBinSkus(accountId, binId, skuFnsks) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingWarehouseBinSku) {
        console.error('Prisma Client 中未找到 lingXingWarehouseBinSku 模型，请重新生成 Prisma Client 并重启服务器');
        return;
      }

      for (const skuFnsku of skuFnsks) {
        if (!skuFnsku.sku) {
          continue;
        }

        // 处理 product_id（可能是大整数）
        const productIdValue = skuFnsku.product_id !== undefined && skuFnsku.product_id !== null
          ? (typeof skuFnsku.product_id === 'string' 
              ? BigInt(skuFnsku.product_id) 
              : BigInt(skuFnsku.product_id))
          : null;

        // 处理 store_id（可能是字符串或数字）
        const storeIdValue = skuFnsku.store_id !== undefined && skuFnsku.store_id !== null
          ? (typeof skuFnsku.store_id === 'string' ? skuFnsku.store_id : String(skuFnsku.store_id))
          : null;

        await prisma.lingXingWarehouseBinSku.upsert({
          where: {
            accountId_binId_sku: {
              accountId: accountId,
              binId: binId,
              sku: skuFnsku.sku
            }
          },
          update: {
            productId: productIdValue,
            fnsku: skuFnsku.fnsku || null,
            storeId: storeIdValue,
            sellerName: skuFnsku.seller_name || null,
            productName: skuFnsku.product_name || null,
            skuData: skuFnsku, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            binId: binId,
            sku: skuFnsku.sku,
            productId: productIdValue,
            fnsku: skuFnsku.fnsku || null,
            storeId: storeIdValue,
            sellerName: skuFnsku.seller_name || null,
            productName: skuFnsku.product_name || null,
            skuData: skuFnsku // 保存完整数据
          }
        });
      }
    } catch (error) {
      console.error('保存仓位商品关系到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有仓位列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wid: 仓库ID（可选）
   *   - id: 仓位ID（可选）
   *   - status: 仓位状态（可选）
   *   - type: 仓位类型（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { bins: [], total: 0, stats: {} }
   */
  async fetchAllWarehouseBins(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有仓位列表...');

      const allBins = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取仓位列表（offset: ${currentOffset}, limit: ${pageSize}）...`);

        try {
          const pageResult = await this.getWarehouseBinList(accountId, {
            ...filterParams,
            offset: currentOffset,
            limit: pageSize
          });

          const pageBins = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个仓位，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allBins.push(...pageBins);
          console.log(`获取完成，本页 ${pageBins.length} 个仓位，累计 ${allBins.length} 个仓位`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allBins.length, totalCount);
          }

          if (pageBins.length < pageSize || allBins.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取仓位列表失败:`, error.message);
          if (allBins.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有仓位列表获取完成，共 ${allBins.length} 个仓位`);

      // 保存到数据库
      if (allBins && allBins.length > 0) {
        await this.saveWarehouseBins(accountId, allBins);
      }

      return {
        bins: allBins,
        total: allBins.length,
        stats: {
          totalBins: allBins.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有仓位列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询产品仓位列表
   * API: POST /basicOpen/warehouseConfig/warehouseBin/getEntryRecommendBinList
   * 用于查询产品在仓库中的可用仓位和次品仓位列表
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - list: 产品列表（必填，数组）
   *     - wid: 仓库id（必填，string）
   *     - productId: 产品id（必填，string）
   *     - fnsku: fnsku（可选，string）
   *     - sid: 店铺id（可选，string）
   *   - withHistory: 是否查询历史仓位（可选，boolean，默认false）
   * @returns {Promise<Object>} 产品仓位列表数据 { data: [], total: 0 }
   */
  async getEntryRecommendBinList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.list || !Array.isArray(params.list) || params.list.length === 0) {
        throw new Error('list 为必填参数，且必须是非空数组');
      }

      // 验证 list 中每个元素的必填字段
      for (const item of params.list) {
        if (!item.wid || !item.productId) {
          throw new Error('list 中每个元素必须包含 wid 和 productId 字段');
        }
      }

      // 构建请求参数
      const requestParams = {
        list: params.list.map(item => ({
          wid: String(item.wid),
          productId: String(item.productId),
          ...(item.fnsku !== undefined && { fnsku: String(item.fnsku) }),
          ...(item.sid !== undefined && { sid: String(item.sid) })
        })),
        ...(params.withHistory !== undefined && { withHistory: params.withHistory })
      };

      // 调用API获取产品仓位列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/basicOpen/warehouseConfig/warehouseBin/getEntryRecommendBinList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取产品仓位列表失败');
      }

      const binList = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (binList && binList.length > 0) {
        await this.saveProductEntryRecommendBins(accountId, binList);
      }

      return {
        data: binList,
        total: total
      };
    } catch (error) {
      console.error('获取产品仓位列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动批量拉取所有产品仓位列表
   * 将产品列表分批查询，避免单次请求过大
   * @param {string} accountId - 领星账户ID
   * @param {Array} productList - 产品列表（必填，数组）
   *   每个元素包含：
   *     - wid: 仓库id（必填，string）
   *     - productId: 产品id（必填，string）
   *     - fnsku: fnsku（可选，string）
   *     - sid: 店铺id（可选，string）
   * @param {Object} options - 选项
   *   - batchSize: 每批查询的产品数量（默认50）
   *   - delayBetweenBatches: 批次之间的延迟时间（毫秒，默认500）
   *   - withHistory: 是否查询历史仓位（可选，boolean，默认false）
   *   - onProgress: 进度回调函数 (currentBatch, totalBatches, currentCount, totalCount) => void
   * @returns {Promise<Object>} { binList: [], total: 0, stats: {} }
   */
  async fetchAllEntryRecommendBins(accountId, productList, options = {}) {
    const {
      batchSize = 50,
      delayBetweenBatches = 500,
      withHistory = false,
      onProgress = null
    } = options;

    try {
      if (!productList || !Array.isArray(productList) || productList.length === 0) {
        throw new Error('productList 为必填参数，且必须是非空数组');
      }

      // 验证产品列表中每个元素的必填字段
      for (const item of productList) {
        if (!item.wid || !item.productId) {
          throw new Error('productList 中每个元素必须包含 wid 和 productId 字段');
        }
      }

      console.log(`开始自动批量拉取产品仓位列表，共 ${productList.length} 个产品，每批 ${batchSize} 个...`);

      const allBinList = [];
      const totalProducts = productList.length;
      const totalBatches = Math.ceil(totalProducts / batchSize);

      for (let i = 0; i < totalBatches; i++) {
        const startIndex = i * batchSize;
        const endIndex = Math.min(startIndex + batchSize, totalProducts);
        const batch = productList.slice(startIndex, endIndex);
        const currentBatch = i + 1;

        console.log(`正在查询第 ${currentBatch}/${totalBatches} 批产品仓位（${startIndex + 1}-${endIndex}，共 ${batch.length} 个产品）...`);

        try {
          const batchResult = await this.getEntryRecommendBinList(accountId, {
            list: batch,
            withHistory: withHistory
          });

          const batchBinList = batchResult.data || [];
          allBinList.push(...batchBinList);

          console.log(`第 ${currentBatch} 批查询完成，本批 ${batchBinList.length} 个结果，累计 ${allBinList.length} 个结果`);

          if (onProgress) {
            onProgress(currentBatch, totalBatches, allBinList.length, totalProducts);
          }

          // 如果不是最后一批，延迟一下
          if (i < totalBatches - 1 && delayBetweenBatches > 0) {
            await new Promise(resolve => setTimeout(resolve, delayBetweenBatches));
          }
        } catch (error) {
          console.error(`第 ${currentBatch} 批查询失败:`, error.message);
          // 如果第一批就失败，抛出错误
          if (i === 0) {
            throw error;
          }
          // 否则继续处理下一批
          console.log('继续处理下一批...');
        }
      }

      console.log(`所有产品仓位列表查询完成，共 ${allBinList.length} 个结果`);

      return {
        binList: allBinList,
        total: allBinList.length,
        stats: {
          totalProducts: totalProducts,
          totalBatches: totalBatches,
          totalResults: allBinList.length
        }
      };
    } catch (error) {
      console.error('自动批量拉取产品仓位列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询FBA库存列表-v2
   * API: POST /basicOpen/openapi/storage/fbaWarehouseDetail
   * 支持查询FBA库存，对应系统【仓库】>【FBA库存明细】数据,数量维度展示
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20，取值范围[20,200]）
   *   - search_field: 搜索维度（可选）：sku, product_name, seller_sku, fnsku, asin, parent_asin, spu, spu_name
   *   - search_value: 搜索值（可选）
   *   - cid: 分类（可选）
   *   - bid: 品牌（可选）
   *   - attribute: 属性（可选）
   *   - asin_principal: Listing负责人uid（可选，多个使用,分隔）
   *   - status: 在售状态（可选）：0 停售，1 在售
   *   - senior_search_list: 高级搜索列表（可选，JSON字符串）
   *   - fulfillment_channel_type: 配送方式（可选）：FBA, FBM
   *   - is_hide_zero_stock: 是否隐藏零库存行（可选）：0 不隐藏，1 隐藏
   *   - is_parant_asin_merge: 是否合并父ASIN（可选）：0 不合并，1 合并
   *   - is_contain_del_ls: 是否显示已删除Listing（可选）：0 不显示，1 显示
   *   - query_fba_storage_quantity_list: 是否查询FBA可售信息列表（可选，Boolean，默认false）
   * @returns {Promise<Object>} FBA库存列表数据 { data: [], total: 0 }
   */
  async getFbaWarehouseDetailList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        requestParams.length = params.length;
      }
      if (params.search_field !== undefined) {
        requestParams.search_field = params.search_field;
      }
      if (params.search_value !== undefined) {
        requestParams.search_value = params.search_value;
      }
      if (params.cid !== undefined) {
        requestParams.cid = params.cid;
      }
      if (params.bid !== undefined) {
        requestParams.bid = params.bid;
      }
      if (params.attribute !== undefined) {
        requestParams.attribute = params.attribute;
      }
      if (params.asin_principal !== undefined) {
        requestParams.asin_principal = params.asin_principal;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.senior_search_list !== undefined) {
        // senior_search_list 应该是 JSON 字符串
        requestParams.senior_search_list = typeof params.senior_search_list === 'string' 
          ? params.senior_search_list 
          : JSON.stringify(params.senior_search_list);
      }
      if (params.fulfillment_channel_type !== undefined) {
        requestParams.fulfillment_channel_type = params.fulfillment_channel_type;
      }
      if (params.is_hide_zero_stock !== undefined) {
        requestParams.is_hide_zero_stock = params.is_hide_zero_stock;
      }
      if (params.is_parant_asin_merge !== undefined) {
        requestParams.is_parant_asin_merge = params.is_parant_asin_merge;
      }
      if (params.is_contain_del_ls !== undefined) {
        requestParams.is_contain_del_ls = params.is_contain_del_ls;
      }
      if (params.query_fba_storage_quantity_list !== undefined) {
        requestParams.query_fba_storage_quantity_list = params.query_fba_storage_quantity_list;
      }

      // 调用API获取FBA库存列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/basicOpen/openapi/storage/fbaWarehouseDetail', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取FBA库存列表失败');
      }

      const fbaInventoryList = response.data || [];
      const total = response.total || 0;

      // 保存FBA库存列表到数据库
      if (fbaInventoryList.length > 0) {
        await this.saveFbaWarehouseDetails(accountId, fbaInventoryList);
      }

      return {
        data: fbaInventoryList,
        total: total
      };
    } catch (error) {
      console.error('获取FBA库存列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存FBA库存明细列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} fbaInventoryList - FBA库存列表数据
   */
  async saveFbaWarehouseDetails(accountId, fbaInventoryList) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingFbaWarehouseDetail) {
        console.error('Prisma Client 中未找到 lingXingFbaWarehouseDetail 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      // 辅助函数：处理字符串字段（正确处理空字符串）
      const getStringValue = (value, altValue = null) => {
        if (value !== undefined && value !== null && value !== '') {
          return value;
        }
        if (altValue !== undefined && altValue !== null && altValue !== '') {
          return altValue;
        }
        return null;
      };

      // 辅助函数：处理数量字段（正确处理0值）
      const parseQuantity = (value) => {
        if (value === undefined || value === null || value === '') {
          return null;
        }
        const parsed = parseInt(value);
        return isNaN(parsed) ? null : parsed;
      };

      // 辅助函数：处理价格字段（正确处理0值）
      const parsePrice = (value) => {
        if (value === undefined || value === null || value === '') {
          return null;
        }
        const parsed = parseFloat(value);
        return isNaN(parsed) ? null : parsed;
      };

      for (const fbaInventory of fbaInventoryList) {
        // 使用 name + sid + asin 作为唯一标识
        // name 字段可能来自 name、product_name 或 productName，允许为空字符串
        const name = getStringValue(fbaInventory.name, fbaInventory.product_name, fbaInventory.productName) || '';
        const sid = fbaInventory.sid !== undefined && fbaInventory.sid !== null ? String(fbaInventory.sid) : null;
        const asin = getStringValue(fbaInventory.asin);
        
        // 跳过缺少必要字段的记录（sid 和 asin 是唯一键的一部分，name 可以为空字符串）
        if (!sid || !asin) {
          console.warn('FBA库存记录缺少 sid 或 asin，跳过保存:', fbaInventory);
          continue;
        }

        // 处理数量字段
        const availableQuantity = parseQuantity(fbaInventory.available_quantity);
        const reservedQuantity = parseQuantity(fbaInventory.reserved_quantity);
        const inboundQuantity = parseQuantity(fbaInventory.inbound_quantity);
        const totalQuantity = parseQuantity(fbaInventory.total_quantity);
        const total = parseQuantity(fbaInventory.total);
        const availableTotal = parseQuantity(fbaInventory.available_total);

        // 处理价格字段
        const cgPrice = parsePrice(fbaInventory.cg_price);

        // 获取 sellerSku（虽然不是唯一键的一部分，但仍然保存）
        const sellerSku = getStringValue(fbaInventory.seller_sku, fbaInventory.sellerSku);

        await prisma.lingXingFbaWarehouseDetail.upsert({
          where: {
            accountId_name_sid_asin: {
              accountId: accountId,
              name: name,
              sid: sid,
              asin: asin
            }
          },
          update: {
            fnsku: getStringValue(fbaInventory.fnsku, fbaInventory.fnSku),
            parentAsin: getStringValue(fbaInventory.parent_asin, fbaInventory.parentAsin),
            productName: getStringValue(fbaInventory.product_name, fbaInventory.productName),
            name: name,
            sku: getStringValue(fbaInventory.sku),
            spu: getStringValue(fbaInventory.spu),
            spuName: getStringValue(fbaInventory.spu_name, fbaInventory.spuName),
            sid: fbaInventory.sid !== undefined && fbaInventory.sid !== null ? String(fbaInventory.sid) : null,
            sellerName: getStringValue(fbaInventory.seller_name, fbaInventory.sellerName),
            status: fbaInventory.status !== undefined && fbaInventory.status !== null ? String(fbaInventory.status) : null,
            fulfillmentChannelType: getStringValue(fbaInventory.fulfillment_channel_type, fbaInventory.fulfillmentChannelType),
            availableQuantity: availableQuantity,
            reservedQuantity: reservedQuantity,
            inboundQuantity: inboundQuantity,
            totalQuantity: totalQuantity,
            total: total,
            availableTotal: availableTotal,
            cgPrice: cgPrice,
            fbaStorageQuantityList: fbaInventory.fba_storage_quantity_list !== undefined && fbaInventory.fba_storage_quantity_list !== null ? fbaInventory.fba_storage_quantity_list : (fbaInventory.fbaStorageQuantityList !== undefined && fbaInventory.fbaStorageQuantityList !== null ? fbaInventory.fbaStorageQuantityList : null),
            fbaInventoryData: fbaInventory.fba_inventory_data !== undefined && fbaInventory.fba_inventory_data !== null ? fbaInventory.fba_inventory_data : (fbaInventory.fbaInventoryData !== undefined && fbaInventory.fbaInventoryData !== null ? fbaInventory.fbaInventoryData : null),
            data: fbaInventory, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            sellerSku: getStringValue(fbaInventory.seller_sku, fbaInventory.sellerSku),
            asin: asin,
            fnsku: getStringValue(fbaInventory.fnsku, fbaInventory.fnSku),
            parentAsin: getStringValue(fbaInventory.parent_asin, fbaInventory.parentAsin),
            productName: getStringValue(fbaInventory.product_name, fbaInventory.productName),
            name: name,
            sku: getStringValue(fbaInventory.sku),
            spu: getStringValue(fbaInventory.spu),
            spuName: getStringValue(fbaInventory.spu_name, fbaInventory.spuName),
            sid: fbaInventory.sid !== undefined && fbaInventory.sid !== null ? String(fbaInventory.sid) : null,
            sellerName: getStringValue(fbaInventory.seller_name, fbaInventory.sellerName),
            status: fbaInventory.status !== undefined && fbaInventory.status !== null ? String(fbaInventory.status) : null,
            fulfillmentChannelType: getStringValue(fbaInventory.fulfillment_channel_type, fbaInventory.fulfillmentChannelType),
            availableQuantity: availableQuantity,
            reservedQuantity: reservedQuantity,
            inboundQuantity: inboundQuantity,
            totalQuantity: totalQuantity,
            total: total,
            availableTotal: availableTotal,
            cgPrice: cgPrice,
            fbaStorageQuantityList: fbaInventory.fba_storage_quantity_list !== undefined && fbaInventory.fba_storage_quantity_list !== null ? fbaInventory.fba_storage_quantity_list : (fbaInventory.fbaStorageQuantityList !== undefined && fbaInventory.fbaStorageQuantityList !== null ? fbaInventory.fbaStorageQuantityList : null),
            fbaInventoryData: fbaInventory.fba_inventory_data !== undefined && fbaInventory.fba_inventory_data !== null ? fbaInventory.fba_inventory_data : (fbaInventory.fbaInventoryData !== undefined && fbaInventory.fbaInventoryData !== null ? fbaInventory.fbaInventoryData : null),
            data: fbaInventory // 保存完整数据
          }
        });
      }
    } catch (error) {
      console.error('保存FBA库存明细到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有FBA库存列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - 所有 getFbaWarehouseDetailList 支持的筛选参数
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，最大200）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { fbaInventoryList: [], total: 0, stats: {} }
   */
  async fetchAllFbaWarehouseDetails(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 200
    const actualPageSize = Math.min(pageSize, 200);

    try {
      console.log('开始自动拉取所有FBA库存列表...');

      const allFbaInventoryList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取FBA库存列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getFbaWarehouseDetailList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageFbaInventoryList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条FBA库存记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allFbaInventoryList.push(...pageFbaInventoryList);
          console.log(`获取完成，本页 ${pageFbaInventoryList.length} 条记录，累计 ${allFbaInventoryList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allFbaInventoryList.length, totalCount);
          }

          if (pageFbaInventoryList.length < actualPageSize || allFbaInventoryList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取FBA库存列表失败:`, error.message);
          if (allFbaInventoryList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有FBA库存列表获取完成，共 ${allFbaInventoryList.length} 条记录`);

      // 保存所有FBA库存列表到数据库
      if (allFbaInventoryList.length > 0) {
        console.log('开始保存FBA库存列表到数据库...');
        await this.saveFbaWarehouseDetails(accountId, allFbaInventoryList);
        console.log('FBA库存列表保存完成');
      }

      return {
        fbaInventoryList: allFbaInventoryList,
        total: allFbaInventoryList.length,
        stats: {
          totalFbaInventory: allFbaInventoryList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有FBA库存列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询仓库库存明细
   * API: POST /erp/sc/routing/data/local_inventory/inventoryDetails
   * 支持查询本地仓/海外仓库存明细，对应系统【仓库】>【库存明细】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - wid: 仓库id，多个使用英文逗号分隔（可选）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20，上限800）
   *   - sku: SKU，单个,（模糊搜索）（可选）
   * @returns {Promise<Object>} 库存明细列表数据 { data: [], total: 0 }
   */
  async getInventoryDetailsList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 800
        requestParams.length = Math.min(params.length, 800);
      }
      if (params.sku !== undefined) {
        requestParams.sku = params.sku;
      }

      // 调用API获取库存明细列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/inventoryDetails', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取库存明细列表失败');
      }

      const inventoryDetailsList = response.data || [];
      const total = response.total || 0;

      // 保存库存明细列表到数据库
      if (inventoryDetailsList.length > 0) {
        await this.saveInventoryDetails(accountId, inventoryDetailsList);
      }

      return {
        data: inventoryDetailsList,
        total: total
      };
    } catch (error) {
      console.error('获取库存明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有仓库库存明细（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wid: 仓库id（可选）
   *   - sku: SKU（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认800，最大800）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { inventoryDetailsList: [], total: 0, stats: {} }
   */
  async fetchAllInventoryDetails(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 800,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 800
    const actualPageSize = Math.min(pageSize, 800);

    try {
      console.log('开始自动拉取所有仓库库存明细...');

      const allInventoryDetailsList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取库存明细列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getInventoryDetailsList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageInventoryDetailsList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条库存明细记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allInventoryDetailsList.push(...pageInventoryDetailsList);
          console.log(`获取完成，本页 ${pageInventoryDetailsList.length} 条记录，累计 ${allInventoryDetailsList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allInventoryDetailsList.length, totalCount);
          }

          if (pageInventoryDetailsList.length < actualPageSize || allInventoryDetailsList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取库存明细列表失败:`, error.message);
          if (allInventoryDetailsList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有库存明细列表获取完成，共 ${allInventoryDetailsList.length} 条记录`);

      // 保存到数据库（在get方法中已经保存，这里可以再次保存以确保数据最新）
      // if (allInventoryDetailsList && allInventoryDetailsList.length > 0) {
      //   await this.saveInventoryDetails(accountId, allInventoryDetailsList);
      // }

      return {
        inventoryDetailsList: allInventoryDetailsList,
        total: allInventoryDetailsList.length,
        stats: {
          totalInventoryDetails: allInventoryDetailsList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有库存明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询仓位库存明细
   * API: POST /erp/sc/routing/data/local_inventory/inventoryBinDetails
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - wid: 仓库id，多个仓库用英文逗号分隔，默认所有仓库（可选）
   *   - bin_type_list: 仓位类型，多个类型用英文逗号分隔（可选）
   *     1 待检暂存，2 可用暂存，3 次品暂存，4 拣货暂存，5 可用，6 次品
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20，上限500）
   * @returns {Promise<Object>} 仓位库存明细列表数据 { data: [], total: 0 }
   */
  async getInventoryBinDetailsList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.bin_type_list !== undefined) {
        requestParams.bin_type_list = params.bin_type_list;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 500
        requestParams.length = Math.min(params.length, 500);
      }

      // 调用API获取仓位库存明细列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/inventoryBinDetails', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取仓位库存明细列表失败');
      }

      const inventoryBinDetailsList = response.data || [];
      const total = response.total || 0;

      // 保存仓位库存明细列表到数据库
      if (inventoryBinDetailsList.length > 0) {
        await this.saveInventoryBinDetails(accountId, inventoryBinDetailsList);
      }

      return {
        data: inventoryBinDetailsList,
        total: total
      };
    } catch (error) {
      console.error('获取仓位库存明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有仓位库存明细（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wid: 仓库id（可选）
   *   - bin_type_list: 仓位类型（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，最大500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { inventoryBinDetailsList: [], total: 0, stats: {} }
   */
  async fetchAllInventoryBinDetails(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 500
    const actualPageSize = Math.min(pageSize, 500);

    try {
      console.log('开始自动拉取所有仓位库存明细...');

      const allInventoryBinDetailsList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取仓位库存明细列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getInventoryBinDetailsList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageInventoryBinDetailsList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条仓位库存明细记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allInventoryBinDetailsList.push(...pageInventoryBinDetailsList);
          console.log(`获取完成，本页 ${pageInventoryBinDetailsList.length} 条记录，累计 ${allInventoryBinDetailsList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allInventoryBinDetailsList.length, totalCount);
          }

          if (pageInventoryBinDetailsList.length < actualPageSize || allInventoryBinDetailsList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取仓位库存明细列表失败:`, error.message);
          if (allInventoryBinDetailsList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有仓位库存明细列表获取完成，共 ${allInventoryBinDetailsList.length} 条记录`);

      // 保存到数据库（在get方法中已经保存，这里可以再次保存以确保数据最新）
      // if (allInventoryBinDetailsList && allInventoryBinDetailsList.length > 0) {
      //   await this.saveInventoryBinDetails(accountId, allInventoryBinDetailsList);
      // }

      return {
        inventoryBinDetailsList: allInventoryBinDetailsList,
        total: allInventoryBinDetailsList.length,
        stats: {
          totalInventoryBinDetails: allInventoryBinDetailsList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有仓位库存明细列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询库存流水（新）
   * API: POST /erp/sc/routing/inventoryLog/WareHouseInventory/wareHouseCenterStatement
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量（必填，默认0）
   *   - length: 分页长度（必填，默认20）
   *   - wids: 仓库id，多个使用英文逗号分隔（可选）
   *   - types: 流水类型，多个使用英文逗号分隔（可选）
   *   - sub_types: 子类流水类型，多个使用英文逗号分隔（可选）
   *   - start_date: 操作开始时间，格式：Y-m-d（可选）
   *   - end_date: 操作结束时间，格式：Y-m-d（可选）
   * @returns {Promise<Object>} 库存流水列表数据 { data: [], total: 0 }
   */
  async getWarehouseInventoryStatementList(accountId, params = {}) {
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

      // 添加可选参数
      if (params.wids !== undefined) {
        requestParams.wids = params.wids;
      }
      if (params.types !== undefined) {
        requestParams.types = params.types;
      }
      if (params.sub_types !== undefined) {
        requestParams.sub_types = params.sub_types;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }

      // 调用API获取库存流水列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/inventoryLog/WareHouseInventory/wareHouseCenterStatement', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取库存流水列表失败');
      }

      const statementList = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (statementList && statementList.length > 0) {
        await this.saveWarehouseInventoryStatements(accountId, statementList);
      }

      return {
        data: statementList,
        total: total
      };
    } catch (error) {
      console.error('获取库存流水列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有库存流水（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wids: 仓库id（可选）
   *   - types: 流水类型（可选）
   *   - sub_types: 子类流水类型（可选）
   *   - start_date: 操作开始时间（可选）
   *   - end_date: 操作结束时间（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { statementList: [], total: 0, stats: {} }
   */
  async fetchAllWarehouseInventoryStatements(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有库存流水...');

      const allStatementList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取库存流水列表（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getWarehouseInventoryStatementList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageStatementList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条库存流水记录，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allStatementList.push(...pageStatementList);
          console.log(`获取完成，本页 ${pageStatementList.length} 条记录，累计 ${allStatementList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allStatementList.length, totalCount);
          }

          if (pageStatementList.length < pageSize || allStatementList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取库存流水列表失败:`, error.message);
          if (allStatementList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有库存流水列表获取完成，共 ${allStatementList.length} 条记录`);

      return {
        statementList: allStatementList,
        total: allStatementList.length,
        stats: {
          totalStatements: allStatementList.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有库存流水列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存库存流水列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} statements - 库存流水列表数据
   */
  async saveWarehouseInventoryStatements(accountId, statements) {
    try {
      if (!prisma.lingXingWarehouseInventoryStatement) {
        console.error('Prisma Client 中未找到 lingXingWarehouseInventoryStatement 模型');
        return;
      }

      for (const statement of statements) {
        // 使用 wid + order_sn + product_id 作为唯一键
        const wid = statement.wid || statement.warehouse_id;
        const orderSn = statement.order_sn || statement.orderSn || statement.order_number || statement.orderNumber;
        const productId = statement.product_id || statement.productId;
        
        // 跳过缺少必要字段的记录（这些字段是唯一键的一部分）
        if (!wid || !orderSn) {
          console.warn('跳过缺少 wid 或 order_sn 的库存流水记录:', statement);
          continue;
        }

        const widStr = String(wid);
        const orderSnStr = String(orderSn);
        
        // 处理 productId（可能为 null，但仍然是唯一键的一部分）
        const productIdValue = productId !== undefined && productId !== null
          ? (typeof productId === 'string' ? BigInt(productId) : BigInt(productId))
          : null;

        // 辅助函数：处理字符串字段
        const getStringValue = (value, altValue = null) => {
          if (value !== undefined && value !== null && value !== '') {
            return String(value);
          }
          if (altValue !== undefined && altValue !== null && altValue !== '') {
            return String(altValue);
          }
          return null;
        };

        // 辅助函数：处理整数字段
        const parseIntValue = (value) => {
          if (value === undefined || value === null || value === '') {
            return null;
          }
          const parsed = parseInt(value);
          return isNaN(parsed) ? null : parsed;
        };

        // 辅助函数：处理小数字段
        const parseDecimalValue = (value) => {
          if (value === undefined || value === null || value === '') {
            return null;
          }
          const parsed = parseFloat(value);
          return isNaN(parsed) ? null : parsed;
        };

        // 提取所有字段
        const wareHouseName = getStringValue(statement.ware_house_name, statement.warehouseName);
        const productName = getStringValue(statement.product_name, statement.productName);
        const sku = getStringValue(statement.sku);
        const sellerId = statement.seller_id !== undefined && statement.seller_id !== null ? String(statement.seller_id) : null;
        const fnsku = getStringValue(statement.fnsku);
        const productGoodNum = parseIntValue(statement.product_good_num);
        const productBadNum = parseIntValue(statement.product_bad_num);
        const productQcNum = parseIntValue(statement.product_qc_num);
        const productLockGoodNum = parseIntValue(statement.product_lock_good_num);
        const productLockBadNum = parseIntValue(statement.product_lock_bad_num);
        const goodTransitNum = parseIntValue(statement.good_transit_num);
        const badTransitNum = parseIntValue(statement.bad_transit_num);
        const type = parseIntValue(statement.type);
        const typeText = getStringValue(statement.type_text);
        const subType = getStringValue(statement.sub_type);
        const subTypeText = getStringValue(statement.sub_type_text);
        const feeCost = parseDecimalValue(statement.fee_cost);
        const singleCgPrice = parseDecimalValue(statement.single_cg_price);
        const singleFeeCost = parseDecimalValue(statement.single_fee_cost);
        const singleStockPrice = parseDecimalValue(statement.single_stock_price);
        const stockCost = parseDecimalValue(statement.stock_cost);
        const productAmounts = parseDecimalValue(statement.product_amounts);
        const headStockPrice = parseDecimalValue(statement.head_stock_price);
        const headStockCost = parseDecimalValue(statement.head_stock_cost);
        const optUid = statement.opt_uid !== undefined && statement.opt_uid !== null ? String(statement.opt_uid) : null;
        const optTime = getStringValue(statement.opt_time);
        const optRealName = getStringValue(statement.opt_real_name);
        const remark = getStringValue(statement.remark);
        const bid = statement.bid !== undefined && statement.bid !== null ? String(statement.bid) : null;
        const brandName = getStringValue(statement.brand_name);
        const refOrderSn = getStringValue(statement.ref_order_sn);
        const productTotal = parseIntValue(statement.product_total);
        const goodBalanceNum = parseIntValue(statement.good_balance_num);
        const badBalanceNum = parseIntValue(statement.bad_balance_num);
        const goodLockBalanceNum = parseIntValue(statement.good_lock_balance_num);
        const badLockBalanceNum = parseIntValue(statement.bad_lock_balance_num);
        const qcBalanceNum = parseIntValue(statement.qc_balance_num);
        const goodTransitBalanceNum = parseIntValue(statement.good_transit_balance_num);
        const statementId = getStringValue(statement.statement_id);
        const badTransitBalanceNum = parseIntValue(statement.bad_transit_balance_num);

        // 使用 upsert 避免重复数据，唯一键包含 accountId, wid, orderSn, productId
        await prisma.lingXingWarehouseInventoryStatement.upsert({
          where: {
            accountId_wid_orderSn_productId: {
              accountId: accountId,
              wid: widStr,
              orderSn: orderSnStr,
              productId: productIdValue
            }
          },
          update: {
            wid: widStr,
            wareHouseName: wareHouseName,
            orderSn: orderSnStr,
            productId: productIdValue,
            productName: productName,
            sku: sku,
            sellerId: sellerId,
            fnsku: fnsku,
            productGoodNum: productGoodNum,
            productBadNum: productBadNum,
            productQcNum: productQcNum,
            productLockGoodNum: productLockGoodNum,
            productLockBadNum: productLockBadNum,
            goodTransitNum: goodTransitNum,
            badTransitNum: badTransitNum,
            type: type,
            typeText: typeText,
            subType: subType,
            subTypeText: subTypeText,
            feeCost: feeCost,
            singleCgPrice: singleCgPrice,
            singleFeeCost: singleFeeCost,
            singleStockPrice: singleStockPrice,
            stockCost: stockCost,
            productAmounts: productAmounts,
            headStockPrice: headStockPrice,
            headStockCost: headStockCost,
            optUid: optUid,
            optTime: optTime,
            optRealName: optRealName,
            remark: remark,
            bid: bid,
            brandName: brandName,
            refOrderSn: refOrderSn,
            productTotal: productTotal,
            goodBalanceNum: goodBalanceNum,
            badBalanceNum: badBalanceNum,
            goodLockBalanceNum: goodLockBalanceNum,
            badLockBalanceNum: badLockBalanceNum,
            qcBalanceNum: qcBalanceNum,
            goodTransitBalanceNum: goodTransitBalanceNum,
            statementId: statementId,
            badTransitBalanceNum: badTransitBalanceNum,
            data: statement,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            wid: widStr,
            wareHouseName: wareHouseName,
            orderSn: orderSnStr,
            productId: productIdValue,
            productName: productName,
            sku: sku,
            sellerId: sellerId,
            fnsku: fnsku,
            productGoodNum: productGoodNum,
            productBadNum: productBadNum,
            productQcNum: productQcNum,
            productLockGoodNum: productLockGoodNum,
            productLockBadNum: productLockBadNum,
            goodTransitNum: goodTransitNum,
            badTransitNum: badTransitNum,
            type: type,
            typeText: typeText,
            subType: subType,
            subTypeText: subTypeText,
            feeCost: feeCost,
            singleCgPrice: singleCgPrice,
            singleFeeCost: singleFeeCost,
            singleStockPrice: singleStockPrice,
            stockCost: stockCost,
            productAmounts: productAmounts,
            headStockPrice: headStockPrice,
            headStockCost: headStockCost,
            optUid: optUid,
            optTime: optTime,
            optRealName: optRealName,
            remark: remark,
            bid: bid,
            brandName: brandName,
            refOrderSn: refOrderSn,
            productTotal: productTotal,
            goodBalanceNum: goodBalanceNum,
            badBalanceNum: badBalanceNum,
            goodLockBalanceNum: goodLockBalanceNum,
            badLockBalanceNum: badLockBalanceNum,
            qcBalanceNum: qcBalanceNum,
            goodTransitBalanceNum: goodTransitBalanceNum,
            statementId: statementId,
            badTransitBalanceNum: badTransitBalanceNum,
            data: statement
          }
        });
      }

      console.log(`库存流水列表已保存到数据库: 共 ${statements.length} 条记录`);
    } catch (error) {
      console.error('保存库存流水列表到数据库失败:', error.message);
      console.error('错误详情:', error);
    }
  }

  /**
   * 保存仓位流水列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} binStatements - 仓位流水列表数据
   */
  async saveWarehouseBinStatements(accountId, binStatements) {
    try {
      if (!prisma.lingXingWarehouseBinStatement) {
        console.error('Prisma Client 中未找到 lingXingWarehouseBinStatement 模型');
        return;
      }

      for (const statement of binStatements) {
        // 使用 wid + whb_id + order_sn + product_id 作为唯一键
        // API返回的字段是 whb_id（仓位id），不是 binId
        const wid = statement.wid || statement.warehouse_id;
        const binId = statement.whb_id || statement.whbId || statement.bin_id || statement.binId;
        const orderSn = statement.order_sn || statement.orderSn || statement.order_number || statement.orderNumber;
        const productId = statement.product_id || statement.productId;
        
        // 跳过缺少必要字段的记录（这些字段是唯一键的一部分）
        if (!wid || !binId || !orderSn) {
          console.warn('跳过缺少 wid、whb_id 或 order_sn 的仓位流水记录:', statement);
          continue;
        }

        const widStr = String(wid);
        const binIdStr = String(binId);
        const orderSnStr = String(orderSn);
        
        // 处理 productId（可能为 null，但仍然是唯一键的一部分）
        const productIdValue = productId !== undefined && productId !== null
          ? (typeof productId === 'string' ? BigInt(productId) : BigInt(productId))
          : null;

        // 使用 upsert 避免重复数据，唯一键包含 accountId, wid, binId, orderSn, productId
        await prisma.lingXingWarehouseBinStatement.upsert({
          where: {
            accountId_wid_binId_orderSn_productId: {
              accountId: accountId,
              wid: widStr,
              binId: binIdStr,
              orderSn: orderSnStr,
              productId: productIdValue
            }
          },
          update: {
            wid: widStr,
            binId: binIdStr,
            orderSn: orderSnStr,
            productId: productIdValue,
            data: statement,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            wid: widStr,
            binId: binIdStr,
            orderSn: orderSnStr,
            productId: productIdValue,
            data: statement
          }
        });
      }

      console.log(`仓位流水列表已保存到数据库: 共 ${binStatements.length} 条记录`);
    } catch (error) {
      console.error('保存仓位流水列表到数据库失败:', error.message);
      console.error('错误详情:', error);
    }
  }

  /**
   * 保存产品仓位推荐列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} binList - 产品仓位推荐列表数据
   */
  async saveProductEntryRecommendBins(accountId, binList) {
    try {
      if (!prisma.lingXingProductEntryRecommendBin) {
        console.error('Prisma Client 中未找到 lingXingProductEntryRecommendBin 模型');
        return;
      }

      for (const bin of binList) {
        // 使用 wid + productId + whbId 作为唯一键
        const wid = bin.wid || bin.warehouse_id;
        const productId = bin.productId || bin.product_id;
        const whbId = bin.whbId || bin.whb_id || bin.id || bin.binId || bin.bin_id;
        
        // 跳过缺少必要字段的记录（这些字段是唯一键的一部分）
        if (!wid || !productId || !whbId) {
          console.warn('跳过缺少 wid、productId 或 whbId 的产品仓位推荐记录:', bin);
          continue;
        }

        const widStr = String(wid);
        const productIdValue = typeof productId === 'string' 
          ? BigInt(productId) 
          : BigInt(productId);
        const whbIdValue = typeof whbId === 'string' 
          ? BigInt(whbId) 
          : BigInt(whbId);

        // 使用 upsert 避免重复数据，唯一键包含 accountId, wid, productId, whbId
        await prisma.lingXingProductEntryRecommendBin.upsert({
          where: {
            accountId_wid_productId_whbId: {
              accountId: accountId,
              wid: widStr,
              productId: productIdValue,
              whbId: whbIdValue
            }
          },
          update: {
            wid: widStr,
            productId: productIdValue,
            whbId: whbIdValue,
            data: bin,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            wid: widStr,
            productId: productIdValue,
            whbId: whbIdValue,
            data: bin
          }
        });
      }

      console.log(`产品仓位推荐列表已保存到数据库: 共 ${binList.length} 条记录`);
    } catch (error) {
      console.error('保存产品仓位推荐列表到数据库失败:', error.message);
      console.error('错误详情:', error);
    }
  }

  /**
   * 保存入库单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} inboundOrders - 入库单列表数据
   */
  async saveInboundOrders(accountId, inboundOrders) {
    try {
      if (!prisma.lingXingInboundOrder) {
        console.error('Prisma Client 中未找到 lingXingInboundOrder 模型');
        return;
      }

      for (const order of inboundOrders) {
        if (!order.order_sn) {
          continue;
        }

        const wid = order.wid || order.warehouse_id || null;

        await prisma.lingXingInboundOrder.upsert({
          where: {
            accountId_inboundOrderSn: {
              accountId: accountId,
              orderSn: order.order_sn
            }
          },
          update: {
            wid: wid || null,
            data: order,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: order.order_sn,
            wid: wid || null,
            data: order
          }
        });
      }

      console.log(`入库单列表已保存到数据库: 共 ${inboundOrders.length} 条记录`);
    } catch (error) {
      console.error('保存入库单列表到数据库失败:', error.message);
      console.error('错误详情:', error);
    }
  }

  /**
   * 保存出库单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} outboundOrders - 出库单列表数据
   */
  async saveOutboundOrders(accountId, outboundOrders) {
    try {
      if (!prisma.lingXingOutboundOrder) {
        console.error('Prisma Client 中未找到 lingXingOutboundOrder 模型');
        return;
      }

      for (const order of outboundOrders) {
        if (!order.order_sn) {
          continue;
        }

        const wid = order.wid || order.warehouse_id || null;

        await prisma.lingXingOutboundOrder.upsert({
          where: {
            accountId_outboundOrderSn: {
              accountId: accountId,
              orderSn: order.order_sn
            }
          },
          update: {
            wid: wid || null,
            data: order,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            orderSn: order.order_sn,
            wid: wid || null,
            data: order
          }
        });
      }

      console.log(`出库单列表已保存到数据库: 共 ${outboundOrders.length} 条记录`);
    } catch (error) {
      console.error('保存出库单列表到数据库失败:', error.message);
      console.error('错误详情:', error);
    }
  }

  /**
   * 查询仓位流水
   * API: POST /erp/sc/routing/data/local_inventory/wareHouseBinStatement
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - wid: 仓库ID，多个仓库ID用英文逗号,分隔（可选）
   *   - type: 流水类型，多个流水类型用英文逗号分隔（可选）
   *   - bin_type_list: 仓位类型，多个类型用逗号分隔（可选）
   *     1 待检暂存，2 可用暂存，3 次品暂存，4 拣货暂存，5 可用，6 次品
   *   - start_date: 操作开始时间，Y-m-d（可选）
   *   - end_date: 操作结束时间，Y-m-d（可选）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20）
   * @returns {Promise<Object>} 仓位流水列表数据 { data: [], total: 0 }
   */
  async getWarehouseBinStatementList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.type !== undefined) {
        requestParams.type = params.type;
      }
      if (params.bin_type_list !== undefined) {
        requestParams.bin_type_list = params.bin_type_list;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        requestParams.length = params.length;
      }

      // 调用API获取仓位流水列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/wareHouseBinStatement', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取仓位流水列表失败');
      }

      const binStatementList = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (binStatementList && binStatementList.length > 0) {
        await this.saveWarehouseBinStatements(accountId, binStatementList);
      }

      return {
        data: binStatementList,
        total: total
      };
    } catch (error) {
      console.error('获取仓位流水列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有仓位流水（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wid: 仓库ID（可选）
   *   - type: 流水类型（可选）
   *   - bin_type_list: 仓位类型（可选）
   *   - start_date: 操作开始时间（可选）
   *   - end_date: 操作结束时间（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认20）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { binStatementList: [], total: 0, stats: {} }
   */
  async fetchAllWarehouseBinStatements(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 20,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有仓位流水...');

      const allBinStatementList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取仓位流水列表（offset: ${currentOffset}, length: ${pageSize}）...`);

        try {
          const pageResult = await this.getWarehouseBinStatementList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: pageSize
          });

          const pageBinStatementList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条仓位流水记录，预计 ${Math.ceil(totalCount / pageSize)} 页`);
          }

          allBinStatementList.push(...pageBinStatementList);
          console.log(`获取完成，本页 ${pageBinStatementList.length} 条记录，累计 ${allBinStatementList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / pageSize) + 1, Math.ceil(totalCount / pageSize), allBinStatementList.length, totalCount);
          }

          if (pageBinStatementList.length < pageSize || allBinStatementList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += pageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取仓位流水列表失败:`, error.message);
          if (allBinStatementList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有仓位流水列表获取完成，共 ${allBinStatementList.length} 条记录`);

      return {
        binStatementList: allBinStatementList,
        total: allBinStatementList.length,
        stats: {
          totalBinStatements: allBinStatementList.length,
          pagesFetched: Math.floor(currentOffset / pageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有仓位流水列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询收货单列表
   * API: POST /erp/sc/routing/deliveryReceipt/PurchaseReceiptOrder/getOrderList
   * 支持查询收货单列表，对应系统【仓库】>【收货单】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - date_type: 查询时间类型（可选）：1 预计到货时间，2 收货时间，3 创建时间，4 更新时间
   *   - start_date: 开始时间，格式：Y-m-d，当筛选更新时间时，支持Y-m-d或Y-m-d H:i:s（可选）
   *   - end_date: 结束时间，格式：Y-m-d，当筛选更新时间时，支持Y-m-d或Y-m-d H:i:s（可选）
   *   - order_sns: 收货单号，多个使用英文逗号分隔（可选）
   *   - status: 状态（可选）：10 待收货，40 已完成
   *   - wid: 仓库id，多个使用英文逗号分隔（可选）
   *   - order_type: 收货类型（可选）：1 采购订单，2 委外订单
   *   - qc_status: 质检状态，多个使用英文逗号分隔（可选）：0 未质检，1 部分质检，2 完成质检
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认200，上限500）
   * @returns {Promise<Object>} 收货单列表数据 { data: [], total: 0 }
   */
  async getPurchaseReceiptOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.date_type !== undefined) {
        requestParams.date_type = params.date_type;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }
      if (params.order_sns !== undefined) {
        requestParams.order_sns = params.order_sns;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.order_type !== undefined) {
        requestParams.order_type = params.order_type;
      }
      if (params.qc_status !== undefined) {
        requestParams.qc_status = params.qc_status;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 500
        requestParams.length = Math.min(params.length, 500);
      }

      // 调用API获取收货单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/deliveryReceipt/PurchaseReceiptOrder/getOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取收货单列表失败');
      }

      const orderList = response.data?.list || [];
      const total = response.data?.total || 0;

      return {
        data: orderList,
        total: total
      };
    } catch (error) {
      console.error('获取收货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有收货单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - date_type: 查询时间类型（可选）
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - order_sns: 收货单号（可选）
   *   - status: 状态（可选）
   *   - wid: 仓库id（可选）
   *   - order_type: 收货类型（可选）
   *   - qc_status: 质检状态（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，最大500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { orderList: [], total: 0, stats: {} }
   */
  async fetchAllPurchaseReceiptOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 500
    const actualPageSize = Math.min(pageSize, 500);

    try {
      console.log('开始自动拉取所有收货单列表...');

      const allOrderList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取收货单列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchaseReceiptOrderList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageOrderList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条收货单记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allOrderList.push(...pageOrderList);
          console.log(`获取完成，本页 ${pageOrderList.length} 条记录，累计 ${allOrderList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allOrderList.length, totalCount);
          }

          if (pageOrderList.length < actualPageSize || allOrderList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取收货单列表失败:`, error.message);
          if (allOrderList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有收货单列表获取完成，共 ${allOrderList.length} 条记录`);

      return {
        orderList: allOrderList,
        total: allOrderList.length,
        stats: {
          totalOrders: allOrderList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有收货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询质检单列表
   * API: POST /erp/sc/routing/deliveryReceipt/ReceiptOrderQc/getOrderList
   * 支持查询质检单列表，对应系统【仓库】>【质检单】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - date_type: 查询时间类型（可选）：1 质检时间，2 收货时间，3 创建时间
   *   - start_date: 开始时间，格式：Y-m-d（可选）
   *   - end_date: 结束时间，格式：Y-m-d（可选）
   *   - qc_sns: 质检单号，多个使用英文逗号分隔（可选）
   *   - status: 状态，多个使用英文逗号分隔（可选）：0 待质检，1 已质检，2 已免检，10 已质检（撤销），20 已免检（撤销）
   *   - wid: 仓库id，多个用英文逗号分隔（可选）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认200，上限500）
   * @returns {Promise<Object>} 质检单列表数据 { data: [], total: 0 }
   */
  async getReceiptOrderQcList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.date_type !== undefined) {
        requestParams.date_type = params.date_type;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }
      if (params.qc_sns !== undefined) {
        requestParams.qc_sns = params.qc_sns;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 500
        requestParams.length = Math.min(params.length, 500);
      }

      // 调用API获取质检单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/deliveryReceipt/ReceiptOrderQc/getOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取质检单列表失败');
      }

      const qcList = response.data?.list || [];
      const total = response.data?.total || 0;

      return {
        data: qcList,
        total: total
      };
    } catch (error) {
      console.error('获取质检单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存质检单详情到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Object} qcDetail - 质检单详情数据
   */
  async saveReceiptOrderQcDetail(accountId, qcDetail) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingReceiptOrderQc) {
        console.error('Prisma Client 中未找到 lingXingReceiptOrderQc 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      if (!qcDetail.qc_sn) {
        console.warn('质检单详情缺少 qc_sn，跳过保存');
        return;
      }

      // 处理 qc_id（质检单id，可能是字符串）
      const qcIdValue = qcDetail.qc_id || null;

      // 处理各种字段
      const qcTypeValue = qcDetail.qc_type !== undefined && qcDetail.qc_type !== null 
        ? String(qcDetail.qc_type) 
        : null;

      const qcMethodValue = qcDetail.qc_method !== undefined && qcDetail.qc_method !== null
        ? String(qcDetail.qc_method)
        : null;

      const statusValue = qcDetail.status !== undefined && qcDetail.status !== null
        ? String(qcDetail.status)
        : null;

      const widValue = qcDetail.wid !== undefined && qcDetail.wid !== null
        ? (typeof qcDetail.wid === 'string' ? parseInt(qcDetail.wid) : qcDetail.wid)
        : null;

      const orderTypeValue = qcDetail.order_type !== undefined && qcDetail.order_type !== null
        ? String(qcDetail.order_type)
        : null;

      const productReceiveNumValue = qcDetail.product_receive_num !== undefined && qcDetail.product_receive_num !== null
        ? parseInt(qcDetail.product_receive_num)
        : null;

      const productGoodNumValue = qcDetail.product_good_num !== undefined && qcDetail.product_good_num !== null
        ? parseInt(qcDetail.product_good_num)
        : null;

      const productBadNumValue = qcDetail.product_bad_num !== undefined && qcDetail.product_bad_num !== null
        ? parseInt(qcDetail.product_bad_num)
        : null;

      const qcNumValue = qcDetail.qc_num !== undefined && qcDetail.qc_num !== null
        ? parseInt(qcDetail.qc_num)
        : null;

      const qcBadNumValue = qcDetail.qc_bad_num !== undefined && qcDetail.qc_bad_num !== null
        ? parseInt(qcDetail.qc_bad_num)
        : null;

      const productQcNumValue = qcDetail.product_qc_num !== undefined && qcDetail.product_qc_num !== null
        ? parseInt(qcDetail.product_qc_num)
        : null;

      const isComboValue = qcDetail.is_combo !== undefined && qcDetail.is_combo !== null
        ? parseInt(qcDetail.is_combo)
        : null;

      const isAuxValue = qcDetail.is_aux !== undefined && qcDetail.is_aux !== null
        ? parseInt(qcDetail.is_aux)
        : null;

      const supplierIdValue = qcDetail.supplier_id !== undefined && qcDetail.supplier_id !== null
        ? (typeof qcDetail.supplier_id === 'string' ? parseInt(qcDetail.supplier_id) : qcDetail.supplier_id)
        : null;

      const sourceValue = qcDetail.source !== undefined && qcDetail.source !== null
        ? parseInt(qcDetail.source)
        : null;

      await prisma.lingXingReceiptOrderQc.upsert({
        where: {
          accountId_qcSn: {
            accountId: accountId,
            qcSn: qcDetail.qc_sn
          }
        },
        update: {
          qcId: qcIdValue,
          qcType: qcTypeValue,
          qcTypeText: qcDetail.qc_type_text || null,
          qcMethod: qcMethodValue,
          qcMethodText: qcDetail.qc_method_text || null,
          qcImage: qcDetail.qc_image || null,
          receiveTime: qcDetail.receive_time || null,
          receiveUid: qcDetail.receive_uid || null,
          qcUid: qcDetail.qc_uid || null,
          sid: qcDetail.sid || null,
          productReceiveNum: productReceiveNumValue,
          productGoodNum: productGoodNumValue,
          productBadNum: productBadNumValue,
          qcTime: qcDetail.qc_time || null,
          status: statusValue,
          statusText: qcDetail.status_text || null,
          price: qcDetail.price || null,
          productId: qcDetail.product_id || null,
          productName: qcDetail.product_name || null,
          sku: qcDetail.sku || null,
          wid: widValue,
          orderId: qcDetail.order_id || null,
          orderSn: qcDetail.order_sn || null,
          orderType: orderTypeValue,
          cgUid: qcDetail.cg_uid || null,
          fnsku: qcDetail.fnsku || null,
          fileId: qcDetail.file_id || null,
          qcNum: qcNumValue,
          qcBadNum: qcBadNumValue,
          qcRate: qcDetail.qc_rate || null,
          qcRatePass: qcDetail.qc_rate_pass || null,
          qcRemark: qcDetail.qc_remark || null,
          qcPicUrl: qcDetail.qc_pic_url || null,
          whbCodeGood: qcDetail.whb_code_good || null,
          whbCodeBad: qcDetail.whb_code_bad || null,
          productQcNum: productQcNumValue,
          qcRealname: qcDetail.qc_realname || null,
          receiveRealname: qcDetail.receive_realname || null,
          optRealname: qcDetail.opt_realname || null,
          picUrl: qcDetail.pic_url || null,
          isCombo: isComboValue,
          isAux: isAuxValue,
          supplierId: supplierIdValue,
          supplierName: qcDetail.supplier_name || null,
          source: sourceValue,
          file: qcDetail.file || null,
          image: qcDetail.image || null,
          qcStandard: qcDetail.qc_standard || null,
          customReceiveTime: qcDetail.custom_receive_time || null,
          customQcTime: qcDetail.custom_qc_time || null,
          deliveryOrderSn: qcDetail.delivery_order_sn || null,
          sourceCustomOrderSn: qcDetail.source_custom_order_sn || null,
          whbCodeGoodList: qcDetail.whb_code_good_list || null,
          whbCodeBadList: qcDetail.whb_code_bad_list || null,
          qcDetailData: qcDetail, // 保存完整数据
          updatedAt: new Date()
        },
        create: {
          accountId: accountId,
          qcSn: qcDetail.qc_sn,
          qcId: qcIdValue,
          qcType: qcTypeValue,
          qcTypeText: qcDetail.qc_type_text || null,
          qcMethod: qcMethodValue,
          qcMethodText: qcDetail.qc_method_text || null,
          qcImage: qcDetail.qc_image || null,
          receiveTime: qcDetail.receive_time || null,
          receiveUid: qcDetail.receive_uid || null,
          qcUid: qcDetail.qc_uid || null,
          sid: qcDetail.sid || null,
          productReceiveNum: productReceiveNumValue,
          productGoodNum: productGoodNumValue,
          productBadNum: productBadNumValue,
          qcTime: qcDetail.qc_time || null,
          status: statusValue,
          statusText: qcDetail.status_text || null,
          price: qcDetail.price || null,
          productId: qcDetail.product_id || null,
          productName: qcDetail.product_name || null,
          sku: qcDetail.sku || null,
          wid: widValue,
          orderId: qcDetail.order_id || null,
          orderSn: qcDetail.order_sn || null,
          orderType: orderTypeValue,
          cgUid: qcDetail.cg_uid || null,
          fnsku: qcDetail.fnsku || null,
          fileId: qcDetail.file_id || null,
          qcNum: qcNumValue,
          qcBadNum: qcBadNumValue,
          qcRate: qcDetail.qc_rate || null,
          qcRatePass: qcDetail.qc_rate_pass || null,
          qcRemark: qcDetail.qc_remark || null,
          qcPicUrl: qcDetail.qc_pic_url || null,
          whbCodeGood: qcDetail.whb_code_good || null,
          whbCodeBad: qcDetail.whb_code_bad || null,
          productQcNum: productQcNumValue,
          qcRealname: qcDetail.qc_realname || null,
          receiveRealname: qcDetail.receive_realname || null,
          optRealname: qcDetail.opt_realname || null,
          picUrl: qcDetail.pic_url || null,
          isCombo: isComboValue,
          isAux: isAuxValue,
          supplierId: supplierIdValue,
          supplierName: qcDetail.supplier_name || null,
          source: sourceValue,
          file: qcDetail.file || null,
          image: qcDetail.image || null,
          qcStandard: qcDetail.qc_standard || null,
          customReceiveTime: qcDetail.custom_receive_time || null,
          customQcTime: qcDetail.custom_qc_time || null,
          deliveryOrderSn: qcDetail.delivery_order_sn || null,
          sourceCustomOrderSn: qcDetail.source_custom_order_sn || null,
          whbCodeGoodList: qcDetail.whb_code_good_list || null,
          whbCodeBadList: qcDetail.whb_code_bad_list || null,
          qcDetailData: qcDetail // 保存完整数据
        }
      });
    } catch (error) {
      console.error('保存质检单详情到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有质检单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - date_type: 查询时间类型（可选）
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - qc_sns: 质检单号（可选）
   *   - status: 状态（可选）
   *   - wid: 仓库id（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，最大500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - autoFetchDetails: 是否自动拉取并保存详情（默认true）
   *   - delayBetweenDetails: 详情请求之间的延迟时间（毫秒，默认300）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { qcList: [], total: 0, stats: {} }
   */
  async fetchAllReceiptOrderQcs(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      autoFetchDetails = true,
      delayBetweenDetails = 300,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 500
    const actualPageSize = Math.min(pageSize, 500);

    try {
      console.log('开始自动拉取所有质检单列表...');

      const allQcList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取质检单列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getReceiptOrderQcList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageQcList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条质检单记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allQcList.push(...pageQcList);
          console.log(`获取完成，本页 ${pageQcList.length} 条记录，累计 ${allQcList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allQcList.length, totalCount);
          }

          if (pageQcList.length < actualPageSize || allQcList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取质检单列表失败:`, error.message);
          if (allQcList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有质检单列表获取完成，共 ${allQcList.length} 条记录`);

      // 自动拉取并保存每个质检单的详情
      if (autoFetchDetails && allQcList.length > 0) {
        console.log('开始自动拉取并保存质检单详情...');
        let successCount = 0;
        let failCount = 0;

        for (let i = 0; i < allQcList.length; i++) {
          const qc = allQcList[i];
          if (!qc.qc_sn) {
            console.warn(`第 ${i + 1} 条记录缺少 qc_sn，跳过`);
            failCount++;
            continue;
          }

          try {
            console.log(`正在获取质检单详情 (${i + 1}/${allQcList.length}): ${qc.qc_sn}...`);
            const detailResult = await this.getReceiptOrderQcDetail(accountId, { qc_sn: qc.qc_sn });
            
            if (detailResult.data) {
              await this.saveReceiptOrderQcDetail(accountId, detailResult.data);
              successCount++;
              console.log(`质检单详情保存成功: ${qc.qc_sn}`);
            } else {
              console.warn(`质检单详情为空: ${qc.qc_sn}`);
              failCount++;
            }

            // 如果不是最后一条，延迟一下
            if (i < allQcList.length - 1 && delayBetweenDetails > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenDetails));
            }
          } catch (error) {
            console.error(`获取或保存质检单详情失败 (${qc.qc_sn}):`, error.message);
            failCount++;
            // 继续处理下一个，不中断整个流程
          }
        }

        console.log(`质检单详情拉取完成: 成功 ${successCount} 条，失败 ${failCount} 条`);
      }

      return {
        qcList: allQcList,
        total: allQcList.length,
        stats: {
          totalQcs: allQcList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有质检单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询质检单详情
   * API: POST /basicOpen/qualityInspectionOrder/detail
   * 支持查询质检单详情信息
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - qc_sn: 质检单号（必填）
   * @returns {Promise<Object>} 质检单详情数据
   */
  async getReceiptOrderQcDetail(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.qc_sn) {
        throw new Error('qc_sn 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        qc_sn: params.qc_sn
      };

      // 调用API获取质检单详情（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/basicOpen/qualityInspectionOrder/detail', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取质检单详情失败');
      }

      const qcDetail = response.data || {};

      return {
        data: qcDetail
      };
    } catch (error) {
      console.error('获取质检单详情失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询入库单列表
   * API: POST /erp/sc/routing/storage/inbound/getOrders
   * 支持查询入库单列表，对应系统【仓库】>【入库单】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20，上限200）
   *   - wid: 系统仓库id（可选）
   *   - search_field_time: 日期筛选类型（可选）：create_time 创建时间，opt_time 入库时间，increment_time 更新时间
   *   - start_date: 日期查询开始时间，格式：Y-m-d，当筛选更新时间时，支持Y-m-d或Y-m-d H:i:s（可选）
   *   - end_date: 日期查询结束时间，格式：Y-m-d，当筛选更新时间时，支持Y-m-d或Y-m-d H:i:s（可选）
   *   - order_sn: 入库单单号，多个使用英文逗号分隔（可选）
   *   - inbound_idempotent_code: 客户参考单号，多个使用英文逗号分隔（可选）
   *   - status: 入库单状态（可选）：10 待提交，20 待入库，40 已完成，50 已撤销，121 待审批，122 已驳回
   *   - type: 入库类型（可选）：-1 其他入库（含所有自定义类型），1 其他入库（非自定义类型），2 采购入库，3 调拨入库，4 赠品入库，26 退货入库，27 移除入库
   * @returns {Promise<Object>} 入库单列表数据 { data: [], total: 0 }
   */
  async getInboundOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 200
        requestParams.length = Math.min(params.length, 200);
      }
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.search_field_time !== undefined) {
        requestParams.search_field_time = params.search_field_time;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }
      if (params.order_sn !== undefined) {
        requestParams.order_sn = params.order_sn;
      }
      if (params.inbound_idempotent_code !== undefined) {
        requestParams.inbound_idempotent_code = params.inbound_idempotent_code;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.type !== undefined) {
        requestParams.type = params.type;
      }

      // 调用API获取入库单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/storage/inbound/getOrders', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取入库单列表失败');
      }

      const inboundOrderList = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (inboundOrderList && inboundOrderList.length > 0) {
        await this.saveInboundOrders(accountId, inboundOrderList);
      }

      return {
        data: inboundOrderList,
        total: total
      };
    } catch (error) {
      console.error('获取入库单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有入库单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wid: 系统仓库id（可选）
   *   - search_field_time: 日期筛选类型（可选）
   *   - start_date: 日期查询开始时间（可选）
   *   - end_date: 日期查询结束时间（可选）
   *   - order_sn: 入库单单号（可选）
   *   - inbound_idempotent_code: 客户参考单号（可选）
   *   - status: 入库单状态（可选）
   *   - type: 入库类型（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，最大200）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { inboundOrderList: [], total: 0, stats: {} }
   */
  async fetchAllInboundOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 200
    const actualPageSize = Math.min(pageSize, 200);

    try {
      console.log('开始自动拉取所有入库单列表...');

      const allInboundOrderList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取入库单列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getInboundOrderList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageInboundOrderList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条入库单记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allInboundOrderList.push(...pageInboundOrderList);
          console.log(`获取完成，本页 ${pageInboundOrderList.length} 条记录，累计 ${allInboundOrderList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allInboundOrderList.length, totalCount);
          }

          if (pageInboundOrderList.length < actualPageSize || allInboundOrderList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取入库单列表失败:`, error.message);
          if (allInboundOrderList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有入库单列表获取完成，共 ${allInboundOrderList.length} 条记录`);

      return {
        inboundOrderList: allInboundOrderList,
        total: allInboundOrderList.length,
        stats: {
          totalInboundOrders: allInboundOrderList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有入库单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询出库单列表
   * API: POST /erp/sc/routing/storage/outbound/getOrders
   * 支持查询出库单列表，对应系统【仓库】>【出库单】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认20，上限200）
   *   - wid: 系统仓库id（可选）
   *   - search_field_time: 日期筛选类型（可选）：create_time 创建时间，opt_time 出库时间，increment_time 更新时间
   *   - start_date: 日期查询开始时间，格式：Y-m-d，当筛选更新时间时，支持Y-m-d或Y-m-d H:i:s（可选）
   *   - end_date: 日期查询结束时间，格式：Y-m-d，当筛选更新时间时，支持Y-m-d或Y-m-d H:i:s（可选）
   *   - order_sn: 出库单单号，多个使用英文逗号分隔（可选）
   *   - idempotent_code: 客户参考号，多个使用英文逗号分隔（可选）
   *   - status: 出库单状态（可选）：10 待提交，30 待出库，40 已完成，50 已撤销，121 待审批，122 已驳回
   *   - type: 出库类型（可选）：11 其他出库，12 FBA出库，14 退货出库，15 调拨出库，16 WFS出库，17 Temu出库
   * @returns {Promise<Object>} 出库单列表数据 { data: [], total: 0 }
   */
  async getOutboundOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 200
        requestParams.length = Math.min(params.length, 200);
      }
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.search_field_time !== undefined) {
        requestParams.search_field_time = params.search_field_time;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }
      if (params.order_sn !== undefined) {
        requestParams.order_sn = params.order_sn;
      }
      if (params.idempotent_code !== undefined) {
        requestParams.idempotent_code = params.idempotent_code;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.type !== undefined) {
        requestParams.type = params.type;
      }

      // 调用API获取出库单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/storage/outbound/getOrders', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取出库单列表失败');
      }

      const outboundOrderList = response.data || [];
      const total = response.total || 0;

      // 保存到数据库
      if (outboundOrderList && outboundOrderList.length > 0) {
        await this.saveOutboundOrders(accountId, outboundOrderList);
      }

      return {
        data: outboundOrderList,
        total: total
      };
    } catch (error) {
      console.error('获取出库单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有出库单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - wid: 系统仓库id（可选）
   *   - search_field_time: 日期筛选类型（可选）
   *   - start_date: 日期查询开始时间（可选）
   *   - end_date: 日期查询结束时间（可选）
   *   - order_sn: 出库单单号（可选）
   *   - idempotent_code: 客户参考号（可选）
   *   - status: 出库单状态（可选）
   *   - type: 出库类型（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，最大200）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { outboundOrderList: [], total: 0, stats: {} }
   */
  async fetchAllOutboundOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 200
    const actualPageSize = Math.min(pageSize, 200);

    try {
      console.log('开始自动拉取所有出库单列表...');

      const allOutboundOrderList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取出库单列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getOutboundOrderList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageOutboundOrderList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条出库单记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allOutboundOrderList.push(...pageOutboundOrderList);
          console.log(`获取完成，本页 ${pageOutboundOrderList.length} 条记录，累计 ${allOutboundOrderList.length} 条记录`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allOutboundOrderList.length, totalCount);
          }

          if (pageOutboundOrderList.length < actualPageSize || allOutboundOrderList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取出库单列表失败:`, error.message);
          if (allOutboundOrderList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有出库单列表获取完成，共 ${allOutboundOrderList.length} 条记录`);

      return {
        outboundOrderList: allOutboundOrderList,
        total: allOutboundOrderList.length,
        stats: {
          totalOutboundOrders: allOutboundOrderList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有出库单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询海外仓备货单列表
   * API: POST /erp/sc/routing/owms/inbound/listInbound
   * 支持查询海外仓备货单列表，对应系统【仓库】>【海外仓备货单】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - status: 状态（可选）：10 待审核，20 已驳回，30 待配货，40 待发货，50 待收货，51 已撤销，60 已完成
   *   - sub_status: 子状态（可选，仅在待收货状态下生效）：0 全部，1 未收货，2 部分收货
   *   - s_wid: 发货仓库id（可选，数组）
   *   - r_wid: 收货仓库id（可选，数组）
   *   - overseas_order_no: 备货单号（可选）
   *   - create_time_from: 查询开始日期，格式：Y-m-d（可选）
   *   - create_time_to: 查询结束日期，格式：Y-m-d（可选）
   *   - page_size: 分页数量，最大50，默认20（可选）
   *   - page: 当前页码，默认1（可选）
   *   - date_type: 备货单时间查询类型（可选）：delivery_time 发货时间，create_time 创建时间，receive_time 收货时间，update_time 更新时间
   *   - is_delete: 订单是否删除（可选）：0 未删除，1 已删除，2 全部
   * @returns {Promise<Object>} 备货单列表数据 { data: [], total: 0 }
   */
  async getOverseasWarehouseStockOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.sub_status !== undefined) {
        requestParams.sub_status = params.sub_status;
      }
      if (params.s_wid !== undefined) {
        requestParams.s_wid = params.s_wid;
      }
      if (params.r_wid !== undefined) {
        requestParams.r_wid = params.r_wid;
      }
      if (params.overseas_order_no !== undefined) {
        requestParams.overseas_order_no = params.overseas_order_no;
      }
      if (params.create_time_from !== undefined) {
        requestParams.create_time_from = params.create_time_from;
      }
      if (params.create_time_to !== undefined) {
        requestParams.create_time_to = params.create_time_to;
      }
      if (params.page_size !== undefined) {
        // 确保 page_size 不超过 50
        requestParams.page_size = Math.min(params.page_size, 50);
      }
      if (params.page !== undefined) {
        requestParams.page = params.page;
      }
      if (params.date_type !== undefined) {
        requestParams.date_type = params.date_type;
      }
      if (params.is_delete !== undefined) {
        requestParams.is_delete = params.is_delete;
      }

      // 调用API获取备货单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/owms/inbound/listInbound', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取海外仓备货单列表失败');
      }

      const stockOrderList = response.data || [];
      const total = response.total || 0;

      return {
        data: stockOrderList,
        total: total
      };
    } catch (error) {
      console.error('获取海外仓备货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有海外仓备货单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - status: 状态（可选）
   *   - sub_status: 子状态（可选）
   *   - s_wid: 发货仓库id（可选）
   *   - r_wid: 收货仓库id（可选）
   *   - overseas_order_no: 备货单号（可选）
   *   - create_time_from: 查询开始日期（可选）
   *   - create_time_to: 查询结束日期（可选）
   *   - date_type: 备货单时间查询类型（可选）
   *   - is_delete: 订单是否删除（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认50，最大50）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { stockOrderList: [], total: 0, stats: {} }
   */
  async fetchAllOverseasWarehouseStockOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 50,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 50
    const actualPageSize = Math.min(pageSize, 50);

    try {
      console.log('开始自动拉取所有海外仓备货单列表...');

      const allStockOrderList = [];
      let currentPage = 1;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取备货单列表（page: ${currentPage}, page_size: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getOverseasWarehouseStockOrderList(accountId, {
            ...filterParams,
            page: currentPage,
            page_size: actualPageSize
          });

          const pageStockOrderList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条备货单记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allStockOrderList.push(...pageStockOrderList);
          console.log(`获取完成，本页 ${pageStockOrderList.length} 条记录，累计 ${allStockOrderList.length} 条记录`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allStockOrderList.length, totalCount);
          }

          if (pageStockOrderList.length < actualPageSize || allStockOrderList.length >= totalCount) {
            hasMore = false;
          } else {
            currentPage++;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取备货单列表失败:`, error.message);
          if (allStockOrderList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有海外仓备货单列表获取完成，共 ${allStockOrderList.length} 条记录`);

      return {
        stockOrderList: allStockOrderList,
        total: allStockOrderList.length,
        stats: {
          totalStockOrders: allStockOrderList.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有海外仓备货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询备货单详情
   * API: POST /basicOpen/overSeaWarehouse/stockOrder/detail
   * 支持查询备货单详情信息
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - overseas_order_no: 备货单号（必填）
   * @returns {Promise<Object>} 备货单详情数据
   */
  async getOverseasWarehouseStockOrderDetail(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.overseas_order_no) {
        throw new Error('overseas_order_no 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        overseas_order_no: params.overseas_order_no
      };

      // 调用API获取备货单详情（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/basicOpen/overSeaWarehouse/stockOrder/detail', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取备货单详情失败');
      }

      const stockOrderDetail = response.data || {};

      return {
        data: stockOrderDetail
      };
    } catch (error) {
      console.error('获取备货单详情失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询销售出库单列表
   * API: POST /erp/sc/routing/wms/order/wmsOrderList
   * 支持查询ERP中【仓库】>【销售出库单】数据，即自发货订单销售出库单
   * 默认返回一个月内审核出库的数据，超过一个月请加上时间入参
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - page: 分页页码（可选，默认1）
   *   - page_size: 分页长度（可选，默认20，上限200）
   *   - sid_arr: 店铺id（可选，数组）
   *   - status_arr: 状态（可选，数组）：1 物流下单，2 发货中，3 已发货，4 已删除
   *   - logistics_status_arr: 物流状态（可选，数组）：1 待导入，2 物流待下单，3 物流下单中，4 下单异常，5 下单完成，6 待海外仓下单，7 海外仓下单中，11 待导入国内物流，41 物流取消中，42 物流取消异常，43 物流取消完成
   *   - platform_order_no_arr: 平台单号（可选，数组）
   *   - order_number_arr: 系统单号（可选，数组）
   *   - wo_number_arr: 销售出库单号（可选，数组）
   *   - time_type: 时间类型（可选）：create_at 创建时间，delivered_at 出库时间，stock_delivered_at 流水出库时间，update_at 变更时间
   *   - start_date: 开始日期，格式：Y-m-d，默认为最近1个月（可选）
   *   - end_date: 结束日期，格式：Y-m-d，默认为最近1个月（可选）
   * @returns {Promise<Object>} 销售出库单列表数据 { data: [], total: 0 }
   */
  async getWmsOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.page !== undefined) {
        requestParams.page = params.page;
      }
      if (params.page_size !== undefined) {
        // 确保 page_size 不超过 200
        requestParams.page_size = Math.min(params.page_size, 200);
      }
      if (params.sid_arr !== undefined) {
        requestParams.sid_arr = params.sid_arr;
      }
      if (params.status_arr !== undefined) {
        requestParams.status_arr = params.status_arr;
      }
      if (params.logistics_status_arr !== undefined) {
        requestParams.logistics_status_arr = params.logistics_status_arr;
      }
      if (params.platform_order_no_arr !== undefined) {
        requestParams.platform_order_no_arr = params.platform_order_no_arr;
      }
      if (params.order_number_arr !== undefined) {
        requestParams.order_number_arr = params.order_number_arr;
      }
      if (params.wo_number_arr !== undefined) {
        requestParams.wo_number_arr = params.wo_number_arr;
      }
      if (params.time_type !== undefined) {
        requestParams.time_type = params.time_type;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }

      // 调用API获取销售出库单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/wms/order/wmsOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取销售出库单列表失败');
      }

      const wmsOrderList = response.data || [];
      const total = response.total || 0;

      return {
        data: wmsOrderList,
        total: total
      };
    } catch (error) {
      console.error('获取销售出库单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有销售出库单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - sid_arr: 店铺id（可选）
   *   - status_arr: 状态（可选）
   *   - logistics_status_arr: 物流状态（可选）
   *   - platform_order_no_arr: 平台单号（可选）
   *   - order_number_arr: 系统单号（可选）
   *   - wo_number_arr: 销售出库单号（可选）
   *   - time_type: 时间类型（可选）
   *   - start_date: 开始日期（可选）
   *   - end_date: 结束日期（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认200，最大200）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { wmsOrderList: [], total: 0, stats: {} }
   */
  async fetchAllWmsOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 200,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 200
    const actualPageSize = Math.min(pageSize, 200);

    try {
      console.log('开始自动拉取所有销售出库单列表...');

      const allWmsOrderList = [];
      let currentPage = 1;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取销售出库单列表（page: ${currentPage}, page_size: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getWmsOrderList(accountId, {
            ...filterParams,
            page: currentPage,
            page_size: actualPageSize
          });

          const pageWmsOrderList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 条销售出库单记录，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allWmsOrderList.push(...pageWmsOrderList);
          console.log(`获取完成，本页 ${pageWmsOrderList.length} 条记录，累计 ${allWmsOrderList.length} 条记录`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allWmsOrderList.length, totalCount);
          }

          if (pageWmsOrderList.length < actualPageSize || allWmsOrderList.length >= totalCount) {
            hasMore = false;
          } else {
            currentPage++;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取销售出库单列表失败:`, error.message);
          if (allWmsOrderList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有销售出库单列表获取完成，共 ${allWmsOrderList.length} 条记录`);

      return {
        wmsOrderList: allWmsOrderList,
        total: allWmsOrderList.length,
        stats: {
          totalWmsOrders: allWmsOrderList.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有销售出库单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存库存明细列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} inventoryDetails - 库存明细列表数据
   */
  async saveInventoryDetails(accountId, inventoryDetails) {
    try {
      if (!prisma.lingXingInventoryDetail) {
        console.error('Prisma Client 中未找到 lingXingInventoryDetail 模型');
        return;
      }

      for (const detail of inventoryDetails) {
        // 跳过没有 wid 或 product_id 的记录（这些字段是唯一键的一部分）
        // 使用 product_id 而不是 sku 作为唯一键，因为同一SKU可能对应不同的product_id
        if (!detail.wid) {
          continue;
        }

        const wid = String(detail.wid);
        const sku = detail.sku ? String(detail.sku) : null;

        // 提取关键字段 - product_id 是唯一键的一部分，必须存在
        const productId = detail.product_id || detail.productId;
        if (!productId) {
          // 如果没有 product_id，跳过这条记录（因为它是唯一键的一部分）
          console.warn('跳过缺少 product_id 的库存明细记录:', detail);
          continue;
        }
        
        const productIdValue = typeof productId === 'string' 
          ? BigInt(productId) 
          : BigInt(productId);
        
        const quantity = detail.product_total !== undefined && detail.product_total !== null 
          ? parseInt(detail.product_total) 
          : null;
        
        const purchasePrice = detail.purchase_price !== undefined && detail.purchase_price !== null && detail.purchase_price !== ''
          ? parseFloat(detail.purchase_price)
          : (detail.purchasePrice !== undefined && detail.purchasePrice !== null && detail.purchasePrice !== ''
              ? parseFloat(detail.purchasePrice)
              : null);
        
        const stockPrice = detail.stock_price !== undefined && detail.stock_price !== null && detail.stock_price !== ''
          ? parseFloat(detail.stock_price)
          : (detail.stockPrice !== undefined && detail.stockPrice !== null && detail.stockPrice !== ''
              ? parseFloat(detail.stockPrice)
              : null);

        // 使用 upsert 避免重复数据，唯一键包含 accountId, wid, productId
        await prisma.lingXingInventoryDetail.upsert({
          where: {
            accountId_wid_productId: {
              accountId: accountId,
              wid: wid,
              productId: productIdValue
            }
          },
          update: {
            sku: sku,
            productId: productIdValue,
            quantity: quantity,
            purchasePrice: purchasePrice,
            stockPrice: stockPrice,
            data: detail,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            wid: wid,
            sku: sku,
            productId: productIdValue,
            quantity: quantity,
            purchasePrice: purchasePrice,
            stockPrice: stockPrice,
            data: detail
          }
        });
      }

      console.log(`库存明细列表已保存到数据库: 共 ${inventoryDetails.length} 条记录`);
    } catch (error) {
      console.error('保存库存明细列表到数据库失败:', error.message);
    }
  }

  /**
   * 保存仓位库存明细列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} inventoryBinDetails - 仓位库存明细列表数据
   */
  async saveInventoryBinDetails(accountId, inventoryBinDetails) {
    try {
      if (!prisma.lingXingInventoryBinDetail) {
        console.error('Prisma Client 中未找到 lingXingInventoryBinDetail 模型');
        return;
      }

      for (const detail of inventoryBinDetails) {
        // 跳过没有 wid、whb_id 或 product_id 的记录（这些字段是唯一键的一部分）
        // API返回的字段是 whb_id（仓位id），不是 binId
        const binId = detail.whb_id || detail.whbId || detail.bin_id || detail.binId;
        if (!detail.wid || !binId) {
          continue;
        }

        const wid = String(detail.wid);
        const binIdStr = String(binId);

        // 提取关键字段 - product_id 是唯一键的一部分，必须存在
        const productId = detail.product_id || detail.productId;
        if (!productId) {
          // 如果没有 product_id，跳过这条记录（因为它是唯一键的一部分）
          console.warn('跳过缺少 product_id 的仓位库存明细记录:', detail);
          continue;
        }
        
        const productIdValue = typeof productId === 'string' 
          ? BigInt(productId) 
          : BigInt(productId);
        
        // API返回的数量字段是 total
        const quantity = detail.total !== undefined && detail.total !== null 
          ? parseInt(detail.total) 
          : (detail.product_total !== undefined && detail.product_total !== null 
              ? parseInt(detail.product_total) 
              : (detail.quantity !== undefined && detail.quantity !== null 
                  ? parseInt(detail.quantity) 
                  : null));

        // 使用 upsert 避免重复数据，唯一键包含 accountId, wid, binId, productId
        await prisma.lingXingInventoryBinDetail.upsert({
          where: {
            accountId_wid_binId_productId: {
              accountId: accountId,
              wid: wid,
              binId: binIdStr,
              productId: productIdValue
            }
          },
          update: {
            productId: productIdValue,
            quantity: quantity,
            data: detail,
            updatedAt: new Date()
          },
          create: {
            accountId: accountId,
            wid: wid,
            binId: binIdStr,
            productId: productIdValue,
            quantity: quantity,
            data: detail
          }
        });
      }

      console.log(`仓位库存明细列表已保存到数据库: 共 ${inventoryBinDetails.length} 条记录`);
    } catch (error) {
      console.error('保存仓位库存明细列表到数据库失败:', error.message);
    }
  }
}

export default new LingXingWarehouseService();

