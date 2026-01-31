import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';
import { Prisma } from '@prisma/client';

/**
 * 领星ERP基础数据服务
 * 基础数据接口
 */
class LingXingBasicDataService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询亚马逊市场列表
   * API: GET /erp/sc/data/seller/allMarketplace
   * @param {string} accountId - 领星账户ID
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 市场列表数据
   */
  async getAllMarketplaces(accountId, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 如果使用缓存，先尝试从数据库获取
      if (useCache) {
        const cachedMarketplaces = await this.getMarketplacesFromDB();
        if (cachedMarketplaces && cachedMarketplaces.length > 0) {
          console.log('从缓存获取市场列表');
          return cachedMarketplaces;
        }
      }

      // 调用API获取市场列表（使用通用客户端，成功码为0）
      const response = await this.get(account, '/erp/sc/data/seller/allMarketplace', {}, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功，不是200）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取市场列表失败');
      }

      const marketplaces = response.data || [];

      // 保存到数据库（因为数据变更频率低，建议本地保留）
      if (marketplaces.length > 0) {
        await this.saveMarketplaces(marketplaces);
      }

      return marketplaces;
    } catch (error) {
      console.error('获取亚马逊市场列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedMarketplaces = await this.getMarketplacesFromDB();
        if (cachedMarketplaces && cachedMarketplaces.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedMarketplaces;
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存市场列表到数据库
   * @param {Array} marketplaces - 市场列表数据
   */
  async saveMarketplaces(marketplaces) {
    try {
      for (const marketplace of marketplaces) {
        await prisma.lingXingMarketplace.upsert({
          where: {
            marketplaceId: marketplace.marketplace_id
          },
          update: {
            mid: marketplace.mid,
            region: marketplace.region,
            awsRegion: marketplace.aws_region,
            country: marketplace.country,
            code: marketplace.code,
            data: marketplace,
            updatedAt: new Date()
          },
          create: {
            mid: marketplace.mid,
            marketplaceId: marketplace.marketplace_id,
            region: marketplace.region,
            awsRegion: marketplace.aws_region,
            country: marketplace.country,
            code: marketplace.code,
            data: marketplace
          }
        });
      }
    } catch (error) {
      console.error('保存市场列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取市场列表（如果存在）
   * @returns {Promise<Array>} 市场列表数据
   */
  async getMarketplacesFromDB() {
    try {
      const marketplaces = await prisma.lingXingMarketplace.findMany({
        orderBy: { mid: 'asc' }
      });

      return marketplaces.map(m => ({
        mid: m.mid,
        region: m.region,
        aws_region: m.awsRegion,
        country: m.country,
        code: m.code,
        marketplace_id: m.marketplaceId,
        ...(m.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取市场列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据mid获取市场信息
   * @param {number} mid - 站点ID
   * @returns {Promise<Object|null>} 市场信息
   */
  async getMarketplaceByMid(mid) {
    try {
      const marketplace = await prisma.lingXingMarketplace.findFirst({
        where: { mid: mid }
      });

      if (!marketplace) {
        return null;
      }

      return {
        mid: marketplace.mid,
        region: marketplace.region,
        aws_region: marketplace.awsRegion,
        country: marketplace.country,
        code: marketplace.code,
        marketplace_id: marketplace.marketplaceId,
        ...(marketplace.data || {})
      };
    } catch (error) {
      console.error('根据mid获取市场信息失败:', error.message);
      return null;
    }
  }

  /**
   * 根据marketplace_id获取市场信息
   * @param {string} marketplaceId - 亚马逊市场ID
   * @returns {Promise<Object|null>} 市场信息
   */
  async getMarketplaceByMarketplaceId(marketplaceId) {
    try {
      const marketplace = await prisma.lingXingMarketplace.findUnique({
        where: { marketplaceId: marketplaceId }
      });

      if (!marketplace) {
        return null;
      }

      return {
        mid: marketplace.mid,
        region: marketplace.region,
        aws_region: marketplace.awsRegion,
        country: marketplace.country,
        code: marketplace.code,
        marketplace_id: marketplace.marketplaceId,
        ...(marketplace.data || {})
      };
    } catch (error) {
      console.error('根据marketplace_id获取市场信息失败:', error.message);
      return null;
    }
  }

  /**
   * 查询亚马逊国家下地区列表
   * API: POST /erp/sc/data/worldState/lists
   * @param {string} accountId - 领星账户ID
   * @param {string} countryCode - 国家code（如：US, DE等）
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 地区列表数据
   */
  async getWorldStates(accountId, countryCode, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      if (!countryCode) {
        throw new Error('请提供国家code');
      }

      // 如果使用缓存，先尝试从数据库获取
      if (useCache) {
        const cachedStates = await this.getWorldStatesFromDB(countryCode);
        if (cachedStates && cachedStates.length > 0) {
          console.log('从缓存获取地区列表');
          return cachedStates;
        }
      }

      // 调用API获取地区列表（使用通用客户端，成功码为0）
      const response = await this.post(account, '/erp/sc/data/worldState/lists', {
        country_code: countryCode
      }, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取地区列表失败');
      }

      const states = response.data || [];

      // 保存到数据库（因为数据变更频率低，建议本地保留）
      if (states.length > 0) {
        await this.saveWorldStates(states);
      }

      return states;
    } catch (error) {
      console.error('获取亚马逊地区列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedStates = await this.getWorldStatesFromDB(countryCode);
        if (cachedStates && cachedStates.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedStates;
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存地区列表到数据库
   * @param {Array} states - 地区列表数据
   */
  async saveWorldStates(states) {
    try {
      for (const state of states) {
        await prisma.lingXingWorldState.upsert({
          where: {
            countryCode_stateOrProvinceName_code: {
              countryCode: state.country_code,
              stateOrProvinceName: state.state_or_province_name,
              code: state.code
            }
          },
          update: {
            data: state,
            updatedAt: new Date()
          },
          create: {
            countryCode: state.country_code,
            stateOrProvinceName: state.state_or_province_name,
            code: state.code,
            data: state
          }
        });
      }
    } catch (error) {
      console.error('保存地区列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取地区列表（如果存在）
   * @param {string} countryCode - 国家code
   * @returns {Promise<Array>} 地区列表数据
   */
  async getWorldStatesFromDB(countryCode) {
    try {
      const states = await prisma.lingXingWorldState.findMany({
        where: { countryCode: countryCode },
        orderBy: { stateOrProvinceName: 'asc' }
      });

      return states.map(s => ({
        country_code: s.countryCode,
        state_or_province_name: s.stateOrProvinceName,
        code: s.code,
        ...(s.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取地区列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据国家code和地区code获取地区信息
   * @param {string} countryCode - 国家code
   * @param {string} stateCode - 地区code
   * @returns {Promise<Object|null>} 地区信息
   */
  async getWorldStateByCode(countryCode, stateCode) {
    try {
      const state = await prisma.lingXingWorldState.findFirst({
        where: {
          countryCode: countryCode,
          code: stateCode
        }
      });

      if (!state) {
        return null;
      }

      return {
        country_code: state.countryCode,
        state_or_province_name: state.stateOrProvinceName,
        code: state.code,
        ...(state.data || {})
      };
    } catch (error) {
      console.error('根据code获取地区信息失败:', error.message);
      return null;
    }
  }

  /**
   * 查询亚马逊店铺列表
   * API: GET /erp/sc/data/seller/lists
   * @param {string} accountId - 领星账户ID
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 店铺列表数据
   */
  async getSellerLists(accountId, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 如果使用缓存，先尝试从数据库获取该账户下的店铺
      if (useCache) {
        const cachedSellers = await this.getSellerListsFromDB(accountId);
        if (cachedSellers && cachedSellers.length > 0) {
          console.log('从缓存获取店铺列表');
          return cachedSellers;
        }
      }

      // 调用API获取店铺列表（使用通用客户端，成功码为0）
      const response = await this.get(account, '/erp/sc/data/seller/lists', {}, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取店铺列表失败');
      }

      const sellers = response.data || [];

      // 保存到数据库（因为数据变更频率可能较高，但建议本地保留）
      if (sellers.length > 0) {
        await this.saveSellerLists(accountId, sellers);
      }

      return sellers;
    } catch (error) {
      console.error('获取亚马逊店铺列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedSellers = await this.getSellerListsFromDB(accountId);
        if (cachedSellers && cachedSellers.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedSellers;
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存店铺列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} sellers - 店铺列表数据
   */
  async saveSellerLists(accountId, sellers) {
    try {
      for (const seller of sellers) {
        await prisma.lingXingSeller.upsert({
          where: {
            sid: seller.sid
          },
          update: {
            accountId: accountId,
            mid: seller.mid,
            name: seller.name,
            sellerId: seller.seller_id,
            accountName: seller.account_name,
            sellerAccountId: seller.seller_account_id,
            region: seller.region,
            country: seller.country,
            hasAdsSetting: seller.has_ads_setting,
            marketplaceId: seller.marketplace_id,
            status: seller.status,
            data: seller,
            updatedAt: new Date()
          },
          create: {
            sid: seller.sid,
            accountId: accountId,
            mid: seller.mid,
            name: seller.name,
            sellerId: seller.seller_id,
            accountName: seller.account_name,
            sellerAccountId: seller.seller_account_id,
            region: seller.region,
            country: seller.country,
            hasAdsSetting: seller.has_ads_setting,
            marketplaceId: seller.marketplace_id,
            status: seller.status,
            data: seller
          }
        });
      }
    } catch (error) {
      console.error('保存店铺列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取店铺列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Array>} 店铺列表数据
   */
  async getSellerListsFromDB(accountId) {
    try {
      const sellers = await prisma.lingXingSeller.findMany({
        where: { accountId: accountId },
        orderBy: { sid: 'asc' }
      });

      return sellers.map(s => ({
        sid: s.sid,
        mid: s.mid,
        name: s.name,
        seller_id: s.sellerId,
        account_name: s.accountName,
        seller_account_id: s.sellerAccountId,
        region: s.region,
        country: s.country,
        has_ads_setting: s.hasAdsSetting,
        marketplace_id: s.marketplaceId,
        status: s.status,
        ...(s.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据sid获取店铺信息
   * @param {number} sid - 店铺id（领星ERP对企业已授权店铺的唯一标识）
   * @returns {Promise<Object|null>} 店铺信息
   */
  async getSellerBySid(sid) {
    try {
      const seller = await prisma.lingXingSeller.findUnique({
        where: { sid: sid }
      });

      if (!seller) {
        return null;
      }

      return {
        sid: seller.sid,
        mid: seller.mid,
        name: seller.name,
        seller_id: seller.sellerId,
        account_name: seller.accountName,
        seller_account_id: seller.sellerAccountId,
        region: seller.region,
        country: seller.country,
        has_ads_setting: seller.hasAdsSetting,
        marketplace_id: seller.marketplaceId,
        status: seller.status,
        ...(seller.data || {})
      };
    } catch (error) {
      console.error('根据sid获取店铺信息失败:', error.message);
      return null;
    }
  }

  /**
   * 根据状态筛选店铺列表
   * @param {string} accountId - 领星账户ID
   * @param {number} status - 店铺状态：0 停止同步，1 正常，2 授权异常，3 欠费停服
   * @returns {Promise<Array>} 店铺列表数据
   */
  async getSellersByStatus(accountId, status) {
    try {
      const sellers = await prisma.lingXingSeller.findMany({
        where: { 
          accountId: accountId,
          status: status 
        },
        orderBy: { sid: 'asc' }
      });

      return sellers.map(s => ({
        sid: s.sid,
        mid: s.mid,
        name: s.name,
        seller_id: s.sellerId,
        account_name: s.accountName,
        seller_account_id: s.sellerAccountId,
        region: s.region,
        country: s.country,
        has_ads_setting: s.hasAdsSetting,
        marketplace_id: s.marketplaceId,
        status: s.status,
        ...(s.data || {})
      }));
    } catch (error) {
      console.error('根据状态获取店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 查询亚马逊概念店铺列表
   * API: GET /erp/sc/data/seller/conceptLists
   * @param {string} accountId - 领星账户ID
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 概念店铺列表数据
   */
  async getConceptSellerLists(accountId, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 如果使用缓存，先尝试从数据库获取该账户下的概念店铺
      if (useCache) {
        const cachedConceptSellers = await this.getConceptSellerListsFromDB(accountId);
        if (cachedConceptSellers && cachedConceptSellers.length > 0) {
          console.log('从缓存获取概念店铺列表');
          return cachedConceptSellers;
        }
      }

      // 调用API获取概念店铺列表（使用通用客户端，成功码为0）
      const response = await this.get(account, '/erp/sc/data/seller/conceptLists', {}, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取概念店铺列表失败');
      }

      const conceptSellers = response.data || [];

      // 保存到数据库（因为数据变更频率可能较高，但建议本地保留）
      if (conceptSellers.length > 0) {
        await this.saveConceptSellerLists(accountId, conceptSellers);
      }

      return conceptSellers;
    } catch (error) {
      console.error('获取亚马逊概念店铺列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedConceptSellers = await this.getConceptSellerListsFromDB(accountId);
        if (cachedConceptSellers && cachedConceptSellers.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedConceptSellers;
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存概念店铺列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} conceptSellers - 概念店铺列表数据
   */
  async saveConceptSellerLists(accountId, conceptSellers) {
    try {
      for (const conceptSeller of conceptSellers) {
        await prisma.lingXingConceptSeller.upsert({
          where: {
            conceptId: BigInt(conceptSeller.id)
          },
          update: {
            accountId: accountId,
            mid: conceptSeller.mid,
            name: conceptSeller.name,
            sellerId: conceptSeller.seller_id,
            sellerAccountName: conceptSeller.seller_account_name,
            sellerAccountId: conceptSeller.seller_account_id ? BigInt(conceptSeller.seller_account_id) : null,
            region: conceptSeller.region,
            country: conceptSeller.country,
            status: conceptSeller.status,
            data: conceptSeller,
            updatedAt: new Date()
          },
          create: {
            conceptId: BigInt(conceptSeller.id),
            accountId: accountId,
            mid: conceptSeller.mid,
            name: conceptSeller.name,
            sellerId: conceptSeller.seller_id,
            sellerAccountName: conceptSeller.seller_account_name,
            sellerAccountId: conceptSeller.seller_account_id ? BigInt(conceptSeller.seller_account_id) : null,
            region: conceptSeller.region,
            country: conceptSeller.country,
            status: conceptSeller.status,
            data: conceptSeller
          }
        });
      }
    } catch (error) {
      console.error('保存概念店铺列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取概念店铺列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Array>} 概念店铺列表数据
   */
  async getConceptSellerListsFromDB(accountId) {
    try {
      const conceptSellers = await prisma.lingXingConceptSeller.findMany({
        where: { accountId: accountId },
        orderBy: { conceptId: 'asc' }
      });

      return conceptSellers.map(cs => ({
        id: cs.conceptId.toString(),
        mid: cs.mid,
        name: cs.name,
        seller_id: cs.sellerId,
        seller_account_name: cs.sellerAccountName,
        seller_account_id: cs.sellerAccountId ? cs.sellerAccountId.toString() : null,
        region: cs.region,
        country: cs.country,
        status: cs.status,
        ...(cs.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取概念店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据conceptId获取概念店铺信息
   * @param {number} conceptId - 概念店铺ID
   * @returns {Promise<Object|null>} 概念店铺信息
   */
  async getConceptSellerByConceptId(conceptId) {
    try {
      const conceptSeller = await prisma.lingXingConceptSeller.findUnique({
        where: { conceptId: BigInt(conceptId) }
      });

      if (!conceptSeller) {
        return null;
      }

      return {
        id: conceptSeller.conceptId.toString(),
        mid: conceptSeller.mid,
        name: conceptSeller.name,
        seller_id: conceptSeller.sellerId,
        seller_account_name: conceptSeller.sellerAccountName,
        seller_account_id: conceptSeller.sellerAccountId ? conceptSeller.sellerAccountId.toString() : null,
        region: conceptSeller.region,
        country: conceptSeller.country,
        status: conceptSeller.status,
        ...(conceptSeller.data || {})
      };
    } catch (error) {
      console.error('根据conceptId获取概念店铺信息失败:', error.message);
      return null;
    }
  }

  /**
   * 根据状态筛选概念店铺列表
   * @param {string} accountId - 领星账户ID
   * @param {number} status - 概念店铺状态：1 启用，2 禁用
   * @returns {Promise<Array>} 概念店铺列表数据
   */
  async getConceptSellersByStatus(accountId, status) {
    try {
      const conceptSellers = await prisma.lingXingConceptSeller.findMany({
        where: { 
          accountId: accountId,
          status: status 
        },
        orderBy: { conceptId: 'asc' }
      });

      return conceptSellers.map(cs => ({
        id: cs.conceptId.toString(),
        mid: cs.mid,
        name: cs.name,
        seller_id: cs.sellerId,
        seller_account_name: cs.sellerAccountName,
        seller_account_id: cs.sellerAccountId ? cs.sellerAccountId.toString() : null,
        region: cs.region,
        country: cs.country,
        status: cs.status,
        ...(cs.data || {})
      }));
    } catch (error) {
      console.error('根据状态获取概念店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 查询亚马逊概念店铺列表
   * API: GET /erp/sc/data/seller/conceptLists
   * @param {string} accountId - 领星账户ID
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 概念店铺列表数据
   */
  async getConceptSellerLists(accountId, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 如果使用缓存，先尝试从数据库获取该账户下的概念店铺
      if (useCache) {
        const cachedConceptSellers = await this.getConceptSellerListsFromDB(accountId);
        if (cachedConceptSellers && cachedConceptSellers.length > 0) {
          console.log('从缓存获取概念店铺列表');
          return cachedConceptSellers;
        }
      }

      // 调用API获取概念店铺列表（使用通用客户端，成功码为0）
      const response = await this.get(account, '/erp/sc/data/seller/conceptLists', {}, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取概念店铺列表失败');
      }

      const conceptSellers = response.data || [];

      // 保存到数据库（因为数据变更频率可能较高，但建议本地保留）
      if (conceptSellers.length > 0) {
        await this.saveConceptSellerLists(accountId, conceptSellers);
      }

      return conceptSellers;
    } catch (error) {
      console.error('获取亚马逊概念店铺列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedConceptSellers = await this.getConceptSellerListsFromDB(accountId);
        if (cachedConceptSellers && cachedConceptSellers.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedConceptSellers;
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存概念店铺列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} conceptSellers - 概念店铺列表数据
   */
  async saveConceptSellerLists(accountId, conceptSellers) {
    try {
      for (const conceptSeller of conceptSellers) {
        await prisma.lingXingConceptSeller.upsert({
          where: {
            conceptId: BigInt(conceptSeller.id)
          },
          update: {
            accountId: accountId,
            mid: conceptSeller.mid,
            name: conceptSeller.name,
            sellerId: conceptSeller.seller_id,
            sellerAccountName: conceptSeller.seller_account_name,
            sellerAccountId: conceptSeller.seller_account_id ? BigInt(conceptSeller.seller_account_id) : null,
            region: conceptSeller.region,
            country: conceptSeller.country,
            status: conceptSeller.status,
            data: conceptSeller,
            updatedAt: new Date()
          },
          create: {
            conceptId: BigInt(conceptSeller.id),
            accountId: accountId,
            mid: conceptSeller.mid,
            name: conceptSeller.name,
            sellerId: conceptSeller.seller_id,
            sellerAccountName: conceptSeller.seller_account_name,
            sellerAccountId: conceptSeller.seller_account_id ? BigInt(conceptSeller.seller_account_id) : null,
            region: conceptSeller.region,
            country: conceptSeller.country,
            status: conceptSeller.status,
            data: conceptSeller
          }
        });
      }
    } catch (error) {
      console.error('保存概念店铺列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取概念店铺列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Array>} 概念店铺列表数据
   */
  async getConceptSellerListsFromDB(accountId) {
    try {
      const conceptSellers = await prisma.lingXingConceptSeller.findMany({
        where: { accountId: accountId },
        orderBy: { conceptId: 'asc' }
      });

      return conceptSellers.map(cs => ({
        id: cs.conceptId.toString(),
        mid: cs.mid,
        name: cs.name,
        seller_id: cs.sellerId,
        seller_account_name: cs.sellerAccountName,
        seller_account_id: cs.sellerAccountId ? cs.sellerAccountId.toString() : null,
        region: cs.region,
        country: cs.country,
        status: cs.status,
        ...(cs.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取概念店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据conceptId获取概念店铺信息
   * @param {number} conceptId - 概念店铺ID
   * @returns {Promise<Object|null>} 概念店铺信息
   */
  async getConceptSellerByConceptId(conceptId) {
    try {
      const conceptSeller = await prisma.lingXingConceptSeller.findUnique({
        where: { conceptId: BigInt(conceptId) }
      });

      if (!conceptSeller) {
        return null;
      }

      return {
        id: conceptSeller.conceptId.toString(),
        mid: conceptSeller.mid,
        name: conceptSeller.name,
        seller_id: conceptSeller.sellerId,
        seller_account_name: conceptSeller.sellerAccountName,
        seller_account_id: conceptSeller.sellerAccountId ? conceptSeller.sellerAccountId.toString() : null,
        region: conceptSeller.region,
        country: conceptSeller.country,
        status: conceptSeller.status,
        ...(conceptSeller.data || {})
      };
    } catch (error) {
      console.error('根据conceptId获取概念店铺信息失败:', error.message);
      return null;
    }
  }

  /**
   * 根据状态筛选概念店铺列表
   * @param {string} accountId - 领星账户ID
   * @param {number} status - 概念店铺状态：1 启用，2 禁用
   * @returns {Promise<Array>} 概念店铺列表数据
   */
  async getConceptSellersByStatus(accountId, status) {
    try {
      const conceptSellers = await prisma.lingXingConceptSeller.findMany({
        where: { 
          accountId: accountId,
          status: status 
        },
        orderBy: { conceptId: 'asc' }
      });

      return conceptSellers.map(cs => ({
        id: cs.conceptId.toString(),
        mid: cs.mid,
        name: cs.name,
        seller_id: cs.sellerId,
        seller_account_name: cs.sellerAccountName,
        seller_account_id: cs.sellerAccountId ? cs.sellerAccountId.toString() : null,
        region: cs.region,
        country: cs.country,
        status: cs.status,
        ...(cs.data || {})
      }));
    } catch (error) {
      console.error('根据状态获取概念店铺列表失败:', error.message);
      return [];
    }
  }

  /**
   * 查询汇率
   * API: POST /erp/sc/routing/finance/currency/currencyMonth
   * @param {string} accountId - 领星账户ID
   * @param {string} date - 汇率月份 (格式: YYYY-MM)
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 汇率列表数据
   */
  async getCurrencyRates(accountId, date, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId },
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      if (!date || !/^\d{4}-\d{2}$/.test(date)) {
        throw new Error('请提供有效的汇率月份 (格式: YYYY-MM)');
      }

      // 如果使用缓存，先尝试从数据库获取
      if (useCache) {
        const cachedRates = await this.getCurrencyRatesFromDB(date);
        if (cachedRates && cachedRates.length > 0) {
          console.log(`从缓存获取 ${date} 的汇率`);
          return cachedRates;
        }
      }

      // 调用API获取汇率列表
      const response = await this.post(account, '/erp/sc/routing/finance/currency/currencyMonth', {
        date: date
      }, {
        successCode: [0, 200, '200']
      });

      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取汇率列表失败');
      }

      const rates = response.data || [];

      // 保存到数据库
      if (rates.length > 0) {
        await this.saveCurrencyRates(rates);
      }

      return rates;
    } catch (error) {
      console.error(`获取 ${date} 汇率列表失败:`, error.message);

      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedRates = await this.getCurrencyRatesFromDB(date);
        if (cachedRates && cachedRates.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedRates;
        }
      }

      throw error;
    }
  }

  /**
   * 保存汇率列表到数据库
   * @param {Array} rates - 汇率列表数据
   */
  async saveCurrencyRates(rates) {
    try {
      for (const rate of rates) {
        await prisma.lingXingCurrencyRate.upsert({
          where: {
            date_code: {
              date: rate.date,
              code: rate.code,
            },
          },
          update: {
            icon: rate.icon,
            name: rate.name,
            rateOrg: new Prisma.Decimal(rate.rate_org || 0),
            myRate: new Prisma.Decimal(rate.my_rate || 0),
            updateTime: rate.update_time,
            data: rate,
            updatedAt: new Date(),
          },
          create: {
            date: rate.date,
            code: rate.code,
            icon: rate.icon,
            name: rate.name,
            rateOrg: new Prisma.Decimal(rate.rate_org || 0),
            myRate: new Prisma.Decimal(rate.my_rate || 0),
            updateTime: rate.update_time,
            data: rate,
          },
        });
      }
    } catch (error) {
      console.error('保存汇率列表到数据库失败:', error.message);
    }
  }

  /**
   * 从数据库获取指定月份的汇率列表
   * @param {string} date - 汇率月份 (格式: YYYY-MM)
   * @returns {Promise<Array>} 汇率列表数据
   */
  async getCurrencyRatesFromDB(date) {
    try {
      const rates = await prisma.lingXingCurrencyRate.findMany({
        where: { date: date },
        orderBy: { code: 'asc' },
      });
      return rates.map(r => ({ ...(r.data || {}) }));
    } catch (error) {
      console.error(`从数据库获取 ${date} 汇率列表失败:`, error.message);
      return [];
    }
  }

  /**
   * 查询ERP用户信息列表
   * API: GET /erp/sc/data/account/lists
   * @param {string} accountId - 领星账户ID
   * @param {boolean} useCache - 是否优先使用缓存数据（默认true）
   * @returns {Promise<Array>} 用户列表数据
   */
  async getAccountUsers(accountId, useCache = true) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 如果使用缓存，先尝试从数据库获取
      if (useCache) {
        const cachedUsers = await this.getAccountUsersFromDB(accountId);
        if (cachedUsers && cachedUsers.length > 0) {
          console.log('从缓存获取用户列表');
          return cachedUsers;
        }
      }

      // 调用API获取用户列表（使用通用客户端，成功码为0）
      const response = await this.get(account, '/erp/sc/data/account/lists', {}, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取用户列表失败');
      }

      const users = response.data || [];

      // 保存到数据库（因为数据变更频率低，建议本地保留）
      if (users.length > 0) {
        await this.saveAccountUsers(accountId, users);
      }

      return users;
    } catch (error) {
      console.error('获取ERP用户列表失败:', error.message);
      
      // 如果API调用失败，尝试返回缓存数据
      if (useCache) {
        const cachedUsers = await this.getAccountUsersFromDB(accountId);
        if (cachedUsers && cachedUsers.length > 0) {
          console.log('API调用失败，返回缓存数据');
          return cachedUsers;
        }
      }
      
      throw error;
    }
  }

  /**
   * 保存用户列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} users - 用户列表数据
   */
  async saveAccountUsers(accountId, users) {
    try {
      for (const user of users) {
        await prisma.lingXingAccountUser.upsert({
          where: {
            uid: user.uid
          },
          update: {
            accountId: accountId,
            realname: user.realname,
            username: user.username,
            mobile: user.mobile,
            email: user.email,
            loginNum: user.login_num,
            lastLoginTime: user.last_login_time,
            lastLoginIp: user.last_login_ip,
            status: user.status,
            createTime: user.create_time,
            role: user.role,
            seller: user.seller,
            isMaster: user.is_master,
            data: user,
            updatedAt: new Date()
          },
          create: {
            uid: user.uid,
            accountId: accountId,
            realname: user.realname,
            username: user.username,
            mobile: user.mobile,
            email: user.email,
            loginNum: user.login_num,
            lastLoginTime: user.last_login_time,
            lastLoginIp: user.last_login_ip,
            status: user.status,
            createTime: user.create_time,
            role: user.role,
            seller: user.seller,
            isMaster: user.is_master,
            data: user
          }
        });
      }
    } catch (error) {
      console.error('保存用户列表到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 从数据库获取用户列表（如果存在）
   * @param {string} accountId - 领星账户ID
   * @returns {Promise<Array>} 用户列表数据
   */
  async getAccountUsersFromDB(accountId) {
    try {
      const users = await prisma.lingXingAccountUser.findMany({
        where: { accountId: accountId },
        orderBy: { uid: 'asc' }
      });

      return users.map(u => ({
        uid: u.uid,
        realname: u.realname,
        username: u.username,
        mobile: u.mobile,
        email: u.email,
        login_num: u.loginNum,
        last_login_time: u.lastLoginTime,
        last_login_ip: u.lastLoginIp,
        status: u.status,
        create_time: u.createTime,
        role: u.role,
        seller: u.seller,
        is_master: u.isMaster,
        ...(u.data || {})
      }));
    } catch (error) {
      console.error('从数据库获取用户列表失败:', error.message);
      return [];
    }
  }

  /**
   * 根据uid获取用户信息
   * @param {number} uid - 用户ID
   * @returns {Promise<Object|null>} 用户信息
   */
  async getAccountUserByUid(uid) {
    try {
      const user = await prisma.lingXingAccountUser.findUnique({
        where: { uid: uid }
      });

      if (!user) {
        return null;
      }

      return {
        uid: user.uid,
        realname: user.realname,
        username: user.username,
        mobile: user.mobile,
        email: user.email,
        login_num: user.loginNum,
        last_login_time: user.lastLoginTime,
        last_login_ip: user.lastLoginIp,
        status: user.status,
        create_time: user.createTime,
        role: user.role,
        seller: user.seller,
        is_master: user.isMaster,
        ...(user.data || {})
      };
    } catch (error) {
      console.error('根据uid获取用户信息失败:', error.message);
      return null;
    }
  }

  /**
   * 根据状态筛选用户列表
   * @param {string} accountId - 领星账户ID
   * @param {number} status - 状态：0 禁用，1 正常
   * @returns {Promise<Array>} 用户列表数据
   */
  async getAccountUsersByStatus(accountId, status) {
    try {
      const users = await prisma.lingXingAccountUser.findMany({
        where: {
          accountId: accountId,
          status: status
        },
        orderBy: { uid: 'asc' }
      });

      return users.map(u => ({
        uid: u.uid,
        realname: u.realname,
        username: u.username,
        mobile: u.mobile,
        email: u.email,
        login_num: u.loginNum,
        last_login_time: u.lastLoginTime,
        last_login_ip: u.lastLoginIp,
        status: u.status,
        create_time: u.createTime,
        role: u.role,
        seller: u.seller,
        is_master: u.isMaster,
        ...(u.data || {})
      }));
    } catch (error) {
      console.error('根据状态获取用户列表失败:', error.message);
      return [];
    }
  }
}

export default new LingXingBasicDataService();
