console.log('[1] 开始导入模块...');
import Fastify from 'fastify';
console.log('[2] Fastify 导入完成');
import dotenv from 'dotenv';
console.log('[3] dotenv 导入完成');
import amazonRoutes from './routes/amazonRoutes.js';
console.log('[4] amazonRoutes 导入完成');
import lingxingRoutes from './routes/lingxing/lingxingRoutes.js';
console.log('[5] lingxingRoutes 导入完成');
import lingxingBasicRoutes from './routes/lingxing/lingxingBasicRoutes.js';
console.log('[6] lingxingBasicRoutes 导入完成');
import lingxingProductRoutes from './routes/lingxing/lingxingProductRoutes.js';
console.log('[7] lingxingProductRoutes 导入完成');
import lingxingSalesRoutes from './routes/lingxing/lingxingSalesRoutes.js';
console.log('[8] lingxingSalesRoutes 导入完成');
import lingxingPurchaseRoutes from './routes/lingxing/lingxingPurchaseRoutes.js';
console.log('[9] lingxingPurchaseRoutes 导入完成');
import lingxingLogisticsRoutes from './routes/lingxing/lingxingLogisticsRoutes.js';
console.log('[10] lingxingLogisticsRoutes 导入完成');
import lingxingWarehouseRoutes from './routes/lingxing/lingxingWarehouseRoutes.js';
console.log('[11] lingxingWarehouseRoutes 导入完成');
import lingxingToolsRoutes from './routes/lingxing/lingxingToolsRoutes.js';
console.log('[12] lingxingToolsRoutes 导入完成');
import lingxingAmazonRoutes from './routes/lingxing/lingxingAmazonRoutes.js';
console.log('[13] lingxingAmazonRoutes 导入完成');
import lingxingFinanceRoutes from './routes/lingxing/lingxingFinanceRoutes.js';
console.log('[14] lingxingFinanceRoutes 导入完成');
import accountRoutes from './routes/accountRoutes.js';
console.log('[15] accountRoutes 导入完成');

// 加载环境变量
console.log('[15] 加载环境变量...');
dotenv.config();
console.log('[16] 环境变量加载完成');

const PORT = process.env.PORT || 3000;
console.log(`[17] 端口设置为: ${PORT}`);

// 启动服务器
const start = async () => {
  console.log('[18] 开始创建 Fastify 实例...');
  const fastify = Fastify({
    logger: process.env.NODE_ENV === 'development'
    // logger: true
  });
  console.log('[19] Fastify 实例创建完成');

  try {
    console.log('[20] 开始注册 CORS 插件...');
    // 注册 CORS 插件
    await fastify.register(import('@fastify/cors'), {
      origin: true
    });
    console.log('[21] CORS 插件注册完成');

    // 健康检查
    fastify.get('/health', async (request, reply) => {
      return {
        status: 'ok',
        timestamp: new Date().toISOString(),
        service: 'GwoDataHub'
      };
    });

    // 注册路由
    console.log('[22] 开始注册路由...');
    await fastify.register(amazonRoutes, { prefix: '/api/amazon' });
    console.log('[23] amazonRoutes 注册完成');
    await fastify.register(lingxingRoutes, { prefix: '/api/lingxing' });
    console.log('[24] lingxingRoutes 注册完成');
    await fastify.register(lingxingBasicRoutes, { prefix: '/api/lingxing/basic' });
    console.log('[25] lingxingBasicRoutes 注册完成');
    await fastify.register(lingxingProductRoutes, { prefix: '/api/lingxing/products' });
    console.log('[26] lingxingProductRoutes 注册完成');
    await fastify.register(lingxingSalesRoutes, { prefix: '/api/lingxing/sales' });
    console.log('[27] lingxingSalesRoutes 注册完成');
    await fastify.register(lingxingPurchaseRoutes, { prefix: '/api/lingxing/purchase' });
    console.log('[28] lingxingPurchaseRoutes 注册完成');
    await fastify.register(lingxingLogisticsRoutes, { prefix: '/api/lingxing/logistics' });
    console.log('[29] lingxingLogisticsRoutes 注册完成');
    await fastify.register(lingxingWarehouseRoutes, { prefix: '/api/lingxing/warehouse' });
    console.log('[30] lingxingWarehouseRoutes 注册完成');
    await fastify.register(lingxingToolsRoutes, { prefix: '/api/lingxing/tools' });
    console.log('[31] lingxingToolsRoutes 注册完成');
    await fastify.register(lingxingAmazonRoutes, { prefix: '/api/lingxing/amazon' });
    console.log('[32] lingxingAmazonRoutes 注册完成');
    await fastify.register(lingxingFinanceRoutes, { prefix: '/api/lingxing/finance' });
    console.log('[33] lingxingFinanceRoutes 注册完成');
    await fastify.register(accountRoutes, { prefix: '/api/accounts' });
    console.log('[34] accountRoutes 注册完成');

    // 404处理
    fastify.setNotFoundHandler(async (request, reply) => {
      reply.code(404).send({
        success: false,
        message: '接口不存在'
      });
    });

    // 错误处理
    fastify.setErrorHandler(async (error, request, reply) => {
      fastify.log.error(error);
      reply.code(error.statusCode || 500).send({
        success: false,
        message: error.message || '服务器内部错误'
      });
    });

    // 启动服务器
    console.log(`[35] 开始监听端口 ${PORT}...`);
    await fastify.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`[36] 服务器启动成功！`);
    console.log(`🚀 服务器运行在端口 ${PORT}`);
    console.log(`📊 健康检查: http://localhost:${PORT}/health`);
    console.log(`📚 API文档:`);
    console.log(`   - 亚马逊: http://localhost:${PORT}/api/amazon`);
    console.log(`   - 领星ERP: http://localhost:${PORT}/api/lingxing`);
    console.log(`   - 领星基础数据: http://localhost:${PORT}/api/lingxing/basic`);
    console.log(`   - 领星产品管理: http://localhost:${PORT}/api/lingxing/products`);
    console.log(`   - 领星销售管理: http://localhost:${PORT}/api/lingxing/sales`);
    console.log(`   - 领星采购管理: http://localhost:${PORT}/api/lingxing/purchase`);
    console.log(`   - 领星物流管理: http://localhost:${PORT}/api/lingxing/logistics`);
    console.log(`   - 领星仓库管理: http://localhost:${PORT}/api/lingxing/warehouse`);
    console.log(`   - 领星亚马逊原表: http://localhost:${PORT}/api/lingxing/amazon`);
    console.log(`   - 领星工具管理: http://localhost:${PORT}/api/lingxing/tools`);
    console.log(`   - 领星财务管理: http://localhost:${PORT}/api/lingxing/finance`);
    console.log(`   - 账户管理: http://localhost:${PORT}/api/accounts`);
  } catch (err) {
    console.error('启动服务器失败:', err);
    fastify.log.error(err);
    process.exit(1);
  }
};

console.log('[36] 调用 start() 函数...');
start().catch(err => {
  console.error('[ERROR] start() 函数执行失败:', err);
  process.exit(1);
});

