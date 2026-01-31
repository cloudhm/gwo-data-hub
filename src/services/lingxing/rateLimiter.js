/**
 * 令牌桶限流器
 * 改进的令牌桶算法：
 * - 为每一个请求提供一个令牌
 * - 当请求到达时，如果桶中有足够的令牌，则会消耗一个令牌并允许请求通过
 * - 如果没有令牌，则请求被限流（错误码：3001008）
 * - 令牌回收是基于请求完成、异常、超时（2min）
 * - 令牌桶的维度：appId + 接口url
 */
class RateLimiter {
  constructor(options = {}) {
    // 令牌桶存储：key = `${appId}:${url}`, value = { tokens: number, maxTokens: number, requests: Map<requestId, requestInfo> }
    this.buckets = new Map();
    
    // 每个桶的初始令牌数（可配置，默认10个）
    this.initialTokens = options.initialTokens || 10;
    
    // 请求超时时间（2分钟）
    this.requestTimeout = options.requestTimeout || (2 * 60 * 1000);
    
    // 定期清理超时请求
    this.cleanupInterval = setInterval(() => {
      this.cleanupTimeoutRequests();
    }, 60000); // 每分钟清理一次
  }

  /**
   * 获取桶的key
   */
  getBucketKey(appId, url) {
    return `${appId}:${url}`;
  }

  /**
   * 获取或创建令牌桶
   */
  getBucket(appId, url) {
    const key = this.getBucketKey(appId, url);
    if (!this.buckets.has(key)) {
      this.buckets.set(key, {
        tokens: this.initialTokens, // 初始令牌数
        maxTokens: this.initialTokens, // 最大令牌数（用于限制并发）
        requests: new Map() // 存储进行中的请求 { requestId: { timestamp, timeout } }
      });
    }
    return this.buckets.get(key);
  }

  /**
   * 尝试获取令牌
   * @param {string} appId - APP ID
   * @param {string} url - 接口URL
   * @returns {Object} { success: boolean, requestId?: string, error?: string }
   */
  acquireToken(appId, url) {
    const bucket = this.getBucket(appId, url);
    
    // 检查是否有可用令牌
    if (bucket.tokens > 0) {
      // 消耗一个令牌
      bucket.tokens--;
      
      // 生成请求ID
      const requestId = `${Date.now()}-${Math.random().toString(36).substring(7)}`;
      
      // 记录请求
      const timeout = setTimeout(() => {
        // 超时后释放令牌
        this.releaseToken(appId, url, requestId);
      }, this.requestTimeout);
      
      bucket.requests.set(requestId, {
        timestamp: Date.now(),
        timeout: timeout
      });
      
      return {
        success: true,
        requestId: requestId
      };
    } else {
      // 没有可用令牌，请求被限流
      return {
        success: false,
        error: '请求被限流',
        errorCode: '3001008'
      };
    }
  }

  /**
   * 释放令牌（请求完成、异常、超时）
   * @param {string} appId - APP ID
   * @param {string} url - 接口URL
   * @param {string} requestId - 请求ID
   */
  releaseToken(appId, url, requestId) {
    const key = this.getBucketKey(appId, url);
    const bucket = this.buckets.get(key);
    
    // 如果桶不存在，说明这个请求ID无效，直接返回
    if (!bucket) {
      return;
    }
    
    // 查找并移除请求
    const request = bucket.requests.get(requestId);
    if (request) {
      // 清除超时定时器
      if (request.timeout) {
        clearTimeout(request.timeout);
      }
      
      // 移除请求记录
      bucket.requests.delete(requestId);
      
      // 回收令牌（不超过最大令牌数）
      if (bucket.tokens < bucket.maxTokens) {
        bucket.tokens++;
      }
    }
  }

  /**
   * 清理超时请求
   */
  cleanupTimeoutRequests() {
    const now = Date.now();
    
    for (const [key, bucket] of this.buckets.entries()) {
      const [appId, url] = key.split(':');
      const timeoutRequests = [];
      
      // 找出超时的请求
      for (const [requestId, request] of bucket.requests.entries()) {
        if (now - request.timestamp > this.requestTimeout) {
          timeoutRequests.push(requestId);
        }
      }
      
      // 释放超时请求的令牌
      for (const requestId of timeoutRequests) {
        this.releaseToken(appId, url, requestId);
      }
    }
  }

  /**
   * 销毁限流器（清理资源）
   */
  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    
    // 清理所有定时器
    for (const bucket of this.buckets.values()) {
      for (const request of bucket.requests.values()) {
        if (request.timeout) {
          clearTimeout(request.timeout);
        }
      }
    }
    
    this.buckets.clear();
  }
}

// 单例模式
export default new RateLimiter();

