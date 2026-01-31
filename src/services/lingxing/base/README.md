# 领星ERP通用API客户端

## 概述

`LingXingApiClient` 是一个通用的API客户端基类，封装了所有领星ERP API调用的通用逻辑，包括：

- ✅ 自动获取和刷新 access_token
- ✅ 自动生成签名
- ✅ 令牌桶限流
- ✅ 错误处理和自动重试
- ✅ 友好的错误信息

## 使用方法

### 1. 继承基类

```javascript
import LingXingApiClient from './base/lingxingApiClient.js';

class YourService extends LingXingApiClient {
  constructor() {
    super(); // 调用父类构造函数
  }

  // 你的业务方法
  async yourBusinessMethod(storeId) {
    const store = await prisma.lingXingStore.findUnique({
      where: { id: storeId }
    });

    // 使用继承的方法调用API
    const response = await this.get(store, '/api/your-endpoint', {
      // 业务参数
    });

    return response.data;
  }
}
```

### 2. 使用快捷方法

基类提供了以下快捷方法：

```javascript
// GET 请求
await this.get(store, '/api/endpoint', { param1: 'value1' });

// POST 请求
await this.post(store, '/api/endpoint', { param1: 'value1' });

// PUT 请求
await this.put(store, '/api/endpoint', { param1: 'value1' });

// DELETE 请求
await this.delete(store, '/api/endpoint', { param1: 'value1' });
```

### 3. 使用通用 callApi 方法

如果需要更多控制，可以使用 `callApi` 方法：

```javascript
const response = await this.callApi(store, 'GET', '/api/endpoint', {
  // 业务参数
}, {
  // 选项
  successCode: [0, 200, '200'], // 成功状态码（默认：[0, 200, '200']）
  maxRetries: 2,                // 最大重试次数（默认：2）
  skipRateLimit: false          // 是否跳过限流（默认：false）
});
```

## 功能特性

### 1. 自动Token管理

- 自动检查token是否过期
- 如果过期，优先使用 refresh_token 刷新
- 如果刷新失败，自动重新获取新token
- 所有token操作自动保存到数据库

### 2. 自动签名生成

- 自动添加公共参数（access_token, app_key, timestamp）
- 自动生成签名并URL编码
- 符合领星ERP的签名规则

### 3. 令牌桶限流

- 每个 `appId + 接口url` 组合有独立的令牌桶
- 自动获取和释放令牌
- 请求超时（2分钟）自动释放令牌

### 4. 错误处理

- 自动识别错误码并创建友好错误对象
- 支持自动重试（针对可重试错误）
- 自动刷新token（针对token过期错误）

### 5. 自动重试

支持自动重试的错误码：
- `2001003`: access_token过期 → 自动刷新后重试
- `2001007`: 签名过期 → 重新生成签名后重试
- `3001008`: 请求限流 → 等待1秒后重试

## 示例

### 完整示例

```javascript
import LingXingApiClient from './base/lingxingApiClient.js';
import prisma from '../../../config/database.js';

class OrderService extends LingXingApiClient {
  constructor() {
    super();
  }

  async fetchOrders(storeId, startDate, endDate) {
    const store = await prisma.lingXingStore.findUnique({
      where: { id: storeId }
    });

    if (!store) {
      throw new Error(`店铺不存在: ${storeId}`);
    }

    // 使用继承的 post 方法
    const response = await this.post(store, '/api/orders/list', {
      start_date: startDate,
      end_date: endDate,
      page: 1,
      page_size: 100
    });

    // 检查响应（注意：某些接口返回code=0表示成功）
    if (response.code !== 0 && response.code !== 200) {
      throw new Error(response.message || '获取订单失败');
    }

    return response.data?.list || [];
  }
}

export default new OrderService();
```

## 注意事项

1. **成功状态码**: 不同接口可能使用不同的成功状态码（0、200等），可以通过 `successCode` 选项自定义
2. **限流**: 每个接口都有独立的令牌桶，令牌桶容量由API文档指定
3. **错误重试**: 某些错误会自动重试，重试次数由 `maxRetries` 控制（默认2次）
4. **Token刷新**: Token过期时会自动刷新，无需手动处理

