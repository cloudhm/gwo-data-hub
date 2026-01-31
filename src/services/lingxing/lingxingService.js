import prisma from '../../config/database.js';
import LingXingApiClient from './lingxingApiClient.js';

/**
 * 领星ERP数据服务
 * API文档: https://openapi.lingxing.com
 * 继承通用API客户端，复用限流、错误处理、token管理等逻辑
 */
class LingXingService extends LingXingApiClient {
  constructor() {
    super();
  }

 
}

export default new LingXingService();
