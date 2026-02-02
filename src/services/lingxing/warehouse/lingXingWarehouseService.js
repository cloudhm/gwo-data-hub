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
        // 使用 seller_sku 和 asin 作为唯一标识，如果都没有则跳过
        const sellerSku = getStringValue(fbaInventory.seller_sku, fbaInventory.sellerSku);
        const asin = getStringValue(fbaInventory.asin);
        
        if (!sellerSku && !asin) {
          console.warn('FBA库存记录缺少 seller_sku 和 asin，跳过保存:', fbaInventory);
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

        // 确保唯一键字段不为空（使用默认值）
        const sellerSkuForKey = sellerSku || '';
        const asinForKey = asin || '';

        await prisma.lingXingFbaWarehouseDetail.upsert({
          where: {
            accountId_sellerSku_asin: {
              accountId: accountId,
              sellerSku: sellerSkuForKey,
              asin: asinForKey
            }
          },
          update: {
            fnsku: getStringValue(fbaInventory.fnsku, fbaInventory.fnSku),
            parentAsin: getStringValue(fbaInventory.parent_asin, fbaInventory.parentAsin),
            productName: getStringValue(fbaInventory.product_name, fbaInventory.productName),
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
            sellerSku: sellerSkuForKey,
            asin: asinForKey,
            fnsku: getStringValue(fbaInventory.fnsku, fbaInventory.fnSku),
            parentAsin: getStringValue(fbaInventory.parent_asin, fbaInventory.parentAsin),
            productName: getStringValue(fbaInventory.product_name, fbaInventory.productName),
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
}

export default new LingXingWarehouseService();

