import axios from 'axios';
import crypto from 'crypto';
import CryptoJS from 'crypto-js';
import prisma from '../../config/database.js';
import rateLimiter from './rateLimiter.js';
import { createError, getErrorInfo, ErrorCodes } from './errorCodes.js';

/**
 * 领星ERP API 通用客户端
 * 封装了限流、错误处理、自动获取access_token、签名生成等通用逻辑
 */
class LingXingApiClient {
  constructor(baseURL) {
    this.baseURL = baseURL || process.env.LINGXING_API_BASE_URL || 'https://openapi.lingxing.com';
  }

  /**
   * 获取 access_token（带缓存和自动刷新）
   * @param {Object} account - 领星账户对象（包含 appId, appSecret, accessToken 等）
   */
  async getAccessToken(account) {
    // 如果token存在且未过期，直接返回
    if (account.accessToken && account.tokenExpiresAt && new Date(account.tokenExpiresAt) > new Date()) {
      return account.accessToken;
    }

    // 如果 token 过期但有 refresh_token，先尝试刷新
    if (account.refreshToken && (!account.accessToken || !account.tokenExpiresAt || new Date(account.tokenExpiresAt) <= new Date())) {
      try {
        console.log('尝试使用 refresh_token 刷新 access_token');
        return await this.refreshAccessToken(account);
      } catch (error) {
        console.log('刷新 token 失败，将重新获取:', error.message);
        // 如果刷新失败，继续执行下面的逻辑重新获取 token
      }
    }

    // 如果没有 refresh_token 或刷新失败，重新获取 token
    return await this.fetchNewAccessToken(account);
  }

  /**
   * 刷新 access_token（使用 refresh_token）
   * @param {Object} account - 领星账户对象
   */
  async refreshAccessToken(account) {
    if (!account.refreshToken) {
      throw new Error('refresh_token不存在，无法刷新access_token');
    }

    try {
      const FormDataModule = await import('form-data');
      const FormData = FormDataModule.default || FormDataModule;
      const formData = new FormData();
      formData.append('appId', account.appId);
      formData.append('refreshToken', account.refreshToken);

      const response = await axios.post(
        `${this.baseURL}/api/auth-server/oauth/refresh`,
        formData,
        {
          headers: {
            ...formData.getHeaders()
          }
        }
      );

      if (response.data.code && response.data.code !== 200 && response.data.code !== '200') {
        const errorCode = String(response.data.code);
        const error = createError(errorCode);
        error.response = response.data;
        throw error;
      }

      const data = response.data.data;
      const accessToken = data?.access_token;
      const refreshToken = data?.refresh_token;
      const expiresIn = parseInt(data?.expires_in || 7199);

      if (!accessToken) {
        throw new Error('刷新access_token失败：响应中未包含access_token');
      }

      // 更新数据库中的token
      await prisma.lingXingAccount.update({
        where: { id: account.id },
        data: {
          accessToken: accessToken,
          refreshToken: refreshToken,
          tokenExpiresAt: new Date(Date.now() + expiresIn * 1000)
        }
      });

      return accessToken;
    } catch (error) {
      console.error('刷新领星ERP access_token失败:', error.message);

      if (error.code && ErrorCodes[error.code]) {
        // 如果是 refresh_token 相关的错误，清除 refresh_token
        if (error.code === '2001008' || error.code === '2001009') {
          await prisma.lingXingAccount.update({
            where: { id: account.id },
            data: {
              refreshToken: null,
              accessToken: null,
              tokenExpiresAt: null
            }
          }).catch(() => { });
        }
        throw error;
      }

      throw new Error(`刷新access_token失败: ${error.message}`);
    }
  }

  /**
   * 获取新的 access_token
   * @param {Object} account - 领星账户对象
   */
  async fetchNewAccessToken(account) {
    try {
      const FormDataModule = await import('form-data');
      const FormData = FormDataModule.default || FormDataModule;
      const formData = new FormData();
      formData.append('appId', account.appId);
      formData.append('appSecret', account.appSecret);

      const response = await axios.post(
        `${this.baseURL}/api/auth-server/oauth/access-token`,
        formData,
        {
          headers: {
            ...formData.getHeaders()
          }
        }
      );

      if (response.data.code && response.data.code !== 200 && response.data.code !== '200') {
        const errorCode = String(response.data.code);
        const error = createError(errorCode);
        error.response = response.data;
        throw error;
      }

      const data = response.data.data;
      const accessToken = data?.access_token;
      const refreshToken = data?.refresh_token;
      const expiresIn = parseInt(data?.expires_in || 7199);

      if (!accessToken) {
        throw new Error('获取access_token失败：响应中未包含access_token');
      }

      // 更新数据库中的token
      await prisma.lingXingAccount.update({
        where: { id: account.id },
        data: {
          accessToken: accessToken,
          refreshToken: refreshToken,
          tokenExpiresAt: new Date(Date.now() + expiresIn * 1000)
        }
      });

      return accessToken;
    } catch (error) {
      console.error('获取领星ERP access_token失败:', error.message);

      if (error.code && ErrorCodes[error.code]) {
        throw error;
      }

      throw new Error(`获取access_token失败: ${error.message}`);
    }
  }

  /**
   * 生成接口签名
   * @param {Object} account - 领星账户对象
   * @param {Object} params - 业务参数
   */
  generateSign(account, params = {}) {
    // 合并所有参数
    const allParams = {
      ...params
    };

    // 确保固定参数存在
    if (!allParams.app_key) {
      allParams.app_key = account.appId;
    }
    if (!allParams.timestamp) {
      allParams.timestamp = Math.floor(Date.now() / 1000).toString();
    }

    // 按ASCII排序并过滤
    const sortedKeys = Object.keys(allParams)
      .filter(key => key !== 'sign')
      .filter(key => {
        const value = allParams[key];
        return value !== '' && value !== undefined;
      })
      .sort();

    // 拼接为 key1=value1&key2=value2 格式
    const signString = sortedKeys
      .map(key => {
        const value = allParams[key];
        let valueStr;
        if (value === null) {
          valueStr = 'null';
        } else if (Array.isArray(value)) {
          // 数组参数需要转换为 JSON 格式，如 [271237,271238]
          valueStr = JSON.stringify(value);
        } else if (typeof value === 'object') {
          // 对象参数也需要转换为 JSON 格式
          valueStr = JSON.stringify(value);
        } else {
          valueStr = String(value);
        }
        return `${key}=${valueStr}`;
      })
      .join('&');

    // MD5加密并转大写
    const md5Hash = crypto.createHash('md5').update(signString).digest('hex').toUpperCase();

    // AES/ECB/PKCS5PADDING加密
    let key = account.appId;
    if (key.length < 16) {
      key = key.padEnd(16, '0');
    } else if (key.length > 16 && key.length < 24) {
      key = key.padEnd(24, '0');
    } else if (key.length > 24 && key.length < 32) {
      key = key.padEnd(32, '0');
    } else if (key.length > 32) {
      key = key.substring(0, 32);
    }

    const encrypted = CryptoJS.AES.encrypt(
      CryptoJS.enc.Utf8.parse(md5Hash),
      CryptoJS.enc.Utf8.parse(key),
      {
        mode: CryptoJS.mode.ECB,
        padding: CryptoJS.pad.Pkcs7
      }
    ).toString();

    // URL编码
    return encodeURIComponent(encrypted);
  }

  /**
   * 生成 curl 格式的命令（用于调试）
   * @param {string} method - HTTP方法
   * @param {string} url - 完整URL
   * @param {Object} queryParams - 查询参数
   * @param {Object} bodyData - 请求体数据
   * @param {Object} headers - 请求头
   * @returns {string} curl 命令字符串
   */
  generateCurlCommand(url, method, queryParams = {}, bodyData = null, headers = {}) {
    const methodUpper = method.toUpperCase();
    let curlCmd = `curl -X ${methodUpper}`;

    // 添加请求头
    const allHeaders = {
      'Content-Type': 'application/json',
      ...headers
    };
    for (const [key, value] of Object.entries(allHeaders)) {
      curlCmd += ` \\\n  -H '${key}: ${value}'`;
    }

    // 构建查询字符串
    const queryString = Object.entries(queryParams)
      .filter(([_, value]) => value !== undefined && value !== null && value !== '')
      .map(([key, value]) => {
        let valueStr;
        if (Array.isArray(value)) {
          valueStr = JSON.stringify(value);
        } else if (typeof value === 'object') {
          valueStr = JSON.stringify(value);
        } else {
          valueStr = String(value);
        }
        return `${encodeURIComponent(key)}=${encodeURIComponent(valueStr)}`;
      })
      .join('&');

    // 添加URL和查询参数
    if (queryString) {
      curlCmd += ` \\\n  '${url}?${queryString}'`;
    } else {
      curlCmd += ` \\\n  '${url}'`;
    }

    // 添加请求体（POST/PUT/DELETE）
    if (bodyData && (methodUpper === 'POST' || methodUpper === 'PUT' || methodUpper === 'DELETE')) {
      const bodyStr = JSON.stringify(bodyData);
      // 使用单引号包裹，并转义内部的单引号
      const escapedBody = bodyStr.replace(/'/g, "'\\''");
      curlCmd += ` \\\n  -d '${escapedBody}'`;
    }

    return curlCmd;
  }

  /**
   * 通用API调用方法
   * @param {Object} account - 领星账户信息
   * @param {string} method - HTTP方法 (GET, POST, PUT, DELETE)
   * @param {string} path - API路径
   * @param {Object} params - 业务参数
   * @param {Object} options - 选项
   *   - retryCount: 当前重试次数（内部使用）
   *   - maxRetries: 最大重试次数（默认2）
   *   - successCode: 成功状态码（默认[0, 200, '200']）
   *   - skipRateLimit: 是否跳过限流（默认false）
   * @returns {Promise<Object>} API响应数据
   */
  async callApi(account, method, path, params = {}, options = {}) {
    const {
      retryCount = 0,
      maxRetries = 5,
      successCode = [0, 200, '200'],
      skipRateLimit = false
    } = options;

    const fullUrl = `${this.baseURL}${path}`;
    let requestId = null;
    let requestConfig = null; // 保存请求配置，用于错误时生成curl命令

    try {
      // 限流：尝试获取令牌
      if (!skipRateLimit) {
        const tokenResult = rateLimiter.acquireToken(account.appId, fullUrl);

        if (!tokenResult.success) {
          throw createError('3001008');
        }

        requestId = tokenResult.requestId;
      }

      // 自动获取access_token
      const accessToken = await this.getAccessToken(account);

      // 生成时间戳（实时生成，不缓存）
      const timestamp = Math.floor(Date.now() / 1000).toString();

      // 构建所有参数（用于签名生成）
      const allParams = {
        access_token: accessToken,
        app_key: account.appId,
        timestamp: timestamp,
        ...params // 业务参数
      };

      // 生成签名（基于所有参数）
      const sign = this.generateSign(account, allParams);

      // 发送请求
      const config = {
        method: method.toLowerCase(),
        url: fullUrl,
        headers: {
          'Content-Type': 'application/json'
        }
      };

      // GET 请求：所有参数都在查询字符串中
      if (method.toUpperCase() === 'GET') {
        config.params = {
          ...allParams,
          sign: sign
        };
        requestConfig = {
          method: method,
          url: fullUrl,
          queryParams: config.params,
          bodyData: null,
          headers: config.headers
        };
      } else {
        // POST/PUT/DELETE 请求：
        // - 查询参数：只包含公共参数（access_token、app_key、timestamp、sign）
        // - 请求体：只包含业务参数（params）
        config.params = {
          access_token: accessToken,
          app_key: account.appId,
          timestamp: timestamp,
          sign: sign
        };
        config.data = params; // 只包含业务参数
        requestConfig = {
          method: method,
          url: fullUrl,
          queryParams: config.params,
          bodyData: config.data,
          headers: config.headers
        };
      }

      const response = await axios(config);

      // 检查响应状态
      const responseCode = response.data.code;
      const isSuccess = successCode.includes(responseCode) ||
        (typeof responseCode === 'number' && responseCode === 0) ||
        (typeof responseCode === 'string' && responseCode === '0');

      if (!isSuccess && responseCode !== undefined) {
        // 如果是限流错误码，释放令牌
        if (responseCode === '3001008' || responseCode === 3001008) {
          if (requestId) {
            rateLimiter.releaseToken(account.appId, fullUrl, requestId);
          }
        }

        // 打印完整响应，便于调试
        console.error('=== API响应错误 ===');
        console.error('HTTP状态码:', response.status);
        console.error('错误码:', responseCode);
        console.error('完整响应数据:', JSON.stringify(response.data, null, 2));
        console.error('==================');

        const errorCode = String(responseCode);
        const error = createError(errorCode);
        error.response = response.data;
        throw error;
      }

      // 请求成功，释放令牌
      if (requestId) {
        rateLimiter.releaseToken(account.appId, fullUrl, requestId);
      }
      requestId = null;

      return response.data;
    } catch (error) {
      // 处理需要重试的错误
      if (error.shouldRetry && retryCount < maxRetries) {
        // 如果是token过期，先刷新token
        if (error.shouldRefreshToken) {
          await prisma.lingXingAccount.update({
            where: { id: account.id },
            data: {
              accessToken: null,
              tokenExpiresAt: null
            }
          }).catch(() => { });
        }

        // 如果是限流错误，等待指定时间后重试
        if (error.code === '3001008' && error.retryAfter) {
          await new Promise(resolve => setTimeout(resolve, error.retryAfter));
        }

        // 递归重试
        return this.callApi(account, method, path, params, {
          ...options,
          retryCount: retryCount + 1
        });
      }

      // 生成并打印 curl 格式的请求命令
      if (requestConfig) {
        try {
          const curlCommand = this.generateCurlCommand(
            requestConfig.url,
            requestConfig.method,
            requestConfig.queryParams,
            requestConfig.bodyData,
            requestConfig.headers
          );
          console.error('\n=== 请求信息 (curl格式) ===');
          console.error(curlCommand);
          console.error('===========================\n');
        } catch (curlError) {
          console.error('生成curl命令失败:', curlError.message);
        }
      } else {
        // 如果请求配置不存在（可能在获取token之前就失败了），尝试生成基本信息
        try {
          console.error('\n=== 请求信息 ===');
          console.error(`方法: ${method}`);
          console.error(`URL: ${fullUrl}`);
          console.error(`路径: ${path}`);
          console.error(`业务参数:`, JSON.stringify(params, null, 2));
          console.error('===========================\n');
        } catch (infoError) {
          // 忽略
        }
      }
      
      


      // 处理axios错误响应
      if (error.response && error.response.data) {
        const errorCode = String(error.response.data.code || error.code || '');
        if (errorCode && ErrorCodes[errorCode]) {
          // 释放令牌（除了需要重试的错误）
          if (requestId && (errorCode !== '3001008' || !ErrorCodes[errorCode].shouldRetry)) {
            rateLimiter.releaseToken(account.appId, fullUrl, requestId);
          }

          // 创建友好的错误对象
          const friendlyError = createError(errorCode, error);
          friendlyError.response = error.response.data;

          // 如果是token过期，清除缓存的token
          if (errorCode === '2001003' || errorCode === '2001005') {
            await prisma.lingXingAccount.update({
              where: { id: account.id },
              data: {
                accessToken: null,
                tokenExpiresAt: null
              }
            }).catch(() => { });
          }

          throw friendlyError;
        }
      }

      // 请求异常，释放令牌
      if (requestId) {
        rateLimiter.releaseToken(account.appId, fullUrl, requestId);
      }


      if (error.response) {
        console.error('响应数据:', error.response.data);
      }
      
      console.error('调用领星ERP API失败:', error.message);
      
      

      throw error;
    }
  }

  /**
   * GET请求快捷方法
   * @param {Object} account - 领星账户对象
   */
  async get(account, path, params = {}, options = {}) {
    return this.callApi(account, 'GET', path, params, options);
  }

  /**
   * POST请求快捷方法
   * @param {Object} account - 领星账户对象
   */
  async post(account, path, params = {}, options = {}) {
    return this.callApi(account, 'POST', path, params, options);
  }

  /**
   * PUT请求快捷方法
   * @param {Object} account - 领星账户对象
   */
  async put(account, path, params = {}, options = {}) {
    return this.callApi(account, 'PUT', path, params, options);
  }

  /**
   * DELETE请求快捷方法
   * @param {Object} account - 领星账户对象
   */
  async delete(account, path, params = {}, options = {}) {
    return this.callApi(account, 'DELETE', path, params, options);
  }
}

export default LingXingApiClient;

