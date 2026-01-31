import prisma from '../../config/database.js';

/**
 * 亚马逊数据服务 - 使用官方 SP-API SDK
 */
class AmazonService {
  /**
   * 获取或创建 SP-API 客户端实例
   */
  async getClient(store) {
    // 区域映射
    const regionMap = {
      'US': 'us-east-1',
      'CA': 'us-east-1',
      'MX': 'us-east-1',
      'BR': 'us-east-1',
      'ES': 'eu-west-1',
      'UK': 'eu-west-1',
      'FR': 'eu-west-1',
      'DE': 'eu-west-1',
      'IT': 'eu-west-1',
      'JP': 'us-west-2',
      'AU': 'us-west-2',
      'IN': 'eu-west-1',
      'SG': 'us-west-2'
    };

    const credentials = {
      SELLING_PARTNER_APP_CLIENT_ID: store.accessKey,
      SELLING_PARTNER_APP_CLIENT_SECRET: store.secretKey,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      AWS_SELLING_PARTNER_ROLE: process.env.AWS_SELLING_PARTNER_ROLE
    };

    // 如果店铺有 refreshToken，使用它；否则需要先获取
    if (store.refreshToken) {
      credentials.refresh_token = store.refreshToken;
    }

    return new SellingPartnerAPI({
      region: regionMap[store.region] || 'us-east-1',
      credentials: credentials,
      options: {
        auto_request_tokens: true,
        version: 'v0'
      }
    });
  }

}

export default new AmazonService();
