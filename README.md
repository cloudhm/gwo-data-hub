# GwoDataHub - 数据中台服务

一个基于 Node.js 和 PostgreSQL 的数据中台服务，用于从亚马逊和领星ERP拉取并存储店铺数据。

## 功能特性

- ✅ 从亚马逊店铺拉取订单、产品、库存数据
- ✅ 从领星ERP拉取订单、产品、库存数据
- ✅ 使用 Prisma ORM 管理数据库
- ✅ RESTful API 接口
- ✅ 数据同步日志记录
- ✅ 支持多店铺管理

## 技术栈

- **运行时**: Node.js (ES Modules)
- **数据库**: PostgreSQL
- **ORM**: Prisma
- **Web框架**: Fastify
- **HTTP客户端**: Axios

## 项目结构

```
GwoDataHub/
├── prisma/
│   └── schema.prisma          # Prisma数据库模型
├── src/
│   ├── config/
│   │   └── database.js        # Prisma客户端配置
│   ├── services/
│   │   ├── amazonService.js   # 亚马逊数据服务
│   │   └── lingxingService.js # 领星ERP数据服务
│   ├── routes/
│   │   ├── amazonRoutes.js    # 亚马逊API路由
│   │   ├── lingxingRoutes.js   # 领星ERP API路由
│   │   └── storeRoutes.js     # 店铺管理路由
│   └── index.js               # 应用入口
├── .env.example               # 环境变量示例
├── .gitignore
├── package.json
└── README.md
```

## 快速开始

### 1. 安装依赖

```bash
npm install
```

### 2. 配置环境变量

复制 `env.example` 为 `.env` 并填写配置：

```bash
cp env.example .env
```

编辑 `.env` 文件，配置数据库连接和API密钥：

```env
DATABASE_URL="postgresql://username:password@localhost:5432/gwodatahub?schema=public"
PORT=3000
NODE_ENV=development
```

### 3. 初始化数据库

```bash
# 生成 Prisma Client
npm run prisma:generate

# 运行数据库迁移
npm run prisma:migrate

# 或者直接推送schema到数据库（开发环境）
npm run prisma:push
```

### 4. 启动服务

```bash
# 开发模式（带热重载）
npm run dev

# 生产模式
npm start
```

服务将在 `http://localhost:3000` 启动。

## API 接口

### 健康检查

```
GET /health
```

### 店铺管理

#### 创建亚马逊店铺
```
POST /api/stores/amazon
Body: {
  "storeName": "店铺名称",
  "storeId": "店铺ID",
  "region": "US",
  "accessKey": "访问密钥",
  "secretKey": "密钥"
}
```

#### 创建领星ERP店铺
```
POST /api/stores/lingxing
Body: {
  "storeName": "店铺名称",
  "storeId": "店铺ID",
  "apiKey": "API密钥",
  "apiSecret": "API密钥"
}
```

#### 获取所有店铺
```
GET /api/stores/amazon
GET /api/stores/lingxing
```

#### 获取同步日志
```
GET /api/stores/sync-logs?source=amazon&storeId=xxx&limit=50
```

### 亚马逊数据拉取

#### 拉取订单
```
POST /api/amazon/orders/:storeId
Body: {
  "startDate": "2024-01-01",
  "endDate": "2024-01-31"
}
```

#### 拉取产品
```
POST /api/amazon/products/:storeId
```

#### 拉取库存
```
POST /api/amazon/inventory/:storeId
```

### 领星ERP数据拉取

#### 拉取订单
```
POST /api/lingxing/orders/:storeId
Body: {
  "startDate": "2024-01-01",
  "endDate": "2024-01-31"
}
```

#### 拉取产品
```
POST /api/lingxing/products/:storeId
```

#### 拉取库存
```
POST /api/lingxing/inventory/:storeId
```

## 数据库模型

### 亚马逊数据模型
- `AmazonStore` - 亚马逊店铺
- `AmazonOrder` - 订单
- `AmazonOrderItem` - 订单项
- `AmazonProduct` - 产品
- `AmazonInventory` - 库存

### 领星ERP数据模型
- `LingXingStore` - 领星ERP店铺
- `LingXingOrder` - 订单
- `LingXingOrderItem` - 订单项
- `LingXingProduct` - 产品
- `LingXingInventory` - 库存

### 通用模型
- `SyncLog` - 数据同步日志

## 开发说明

### Prisma 命令

```bash
# 生成 Prisma Client
npm run prisma:generate

# 创建并应用迁移
npm run prisma:migrate

# 推送schema到数据库（开发环境）
npm run prisma:push

# 打开 Prisma Studio（数据库可视化工具）
npm run prisma:studio
```

### API集成说明

1. **亚马逊SP-API**: 
   - ✅ 已集成官方 `amazon-sp-api` SDK
   - SDK 自动处理 OAuth 认证和请求签名
   - 需要配置以下凭证：
     - `SELLING_PARTNER_APP_CLIENT_ID` 和 `SELLING_PARTNER_APP_CLIENT_SECRET` (从 Seller Central 获取)
     - `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY` (用于请求签名)
     - `AWS_SELLING_PARTNER_ROLE` (IAM 角色 ARN)
     - `refresh_token` (首次授权后获取，存储在店铺表中)
   - 参考: https://developer-docs.amazon.com/sp-api/
   - SDK 文档: https://github.com/amz-tools/amazon-sp-api

2. **领星ERP API**:
   - ✅ 已实现API调用框架，使用正确的API域名: `https://openapi.lingxing.com`
   - ✅ 实现了公共请求参数: `access_token`, `app_key`, `timestamp`, `sign`
   - ✅ 实现了签名算法（MD5，可根据实际文档调整为SHA256）
   - ✅ 实现了access_token自动获取和缓存机制
   - ⚠️ 需要根据实际API文档调整：
     - token获取接口路径（当前为 `/auth/token`）
     - 签名算法细节（当前使用MD5，可能需要调整参数顺序）
     - 业务接口路径（订单、产品、库存接口路径）
   - 参考: 领星ERP开放平台文档

## 注意事项

1. 确保 PostgreSQL 数据库已安装并运行
2. 根据实际的API文档调整服务中的API调用逻辑
3. 生产环境请使用环境变量管理敏感信息
4. 建议配置定时任务自动同步数据

## License

ISC

