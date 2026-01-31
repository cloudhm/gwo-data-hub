import Fastify from 'fastify';
import dotenv from 'dotenv';
import amazonRoutes from './routes/amazonRoutes.js';
import lingxingRoutes from './routes/lingxingRoutes.js';
import lingxingBasicRoutes from './routes/lingxingBasicRoutes.js';
import lingxingProductRoutes from './routes/lingxingProductRoutes.js';
import accountRoutes from './routes/accountRoutes.js';

// 加载环境变量
dotenv.config();

const PORT = process.env.PORT || 3000;

// 启动服务器
const start = async () => {
  const fastify = Fastify({
    logger: process.env.NODE_ENV === 'development'
  });

  try {
    // 注册 CORS 插件
    await fastify.register(import('@fastify/cors'), {
      origin: true
    });

    // 健康检查
    fastify.get('/health', async (request, reply) => {
      return {
        status: 'ok',
        timestamp: new Date().toISOString(),
        service: 'GwoDataHub'
      };
    });

    // 注册路由
    await fastify.register(amazonRoutes, { prefix: '/api/amazon' });
    await fastify.register(lingxingRoutes, { prefix: '/api/lingxing' });
    await fastify.register(lingxingBasicRoutes, { prefix: '/api/lingxing/basic' });
    await fastify.register(lingxingProductRoutes, { prefix: '/api/lingxing/products' });
    await fastify.register(accountRoutes, { prefix: '/api/accounts' });

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
    await fastify.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`🚀 服务器运行在端口 ${PORT}`);
    console.log(`📊 健康检查: http://localhost:${PORT}/health`);
    console.log(`📚 API文档:`);
    console.log(`   - 亚马逊: http://localhost:${PORT}/api/amazon`);
    console.log(`   - 领星ERP: http://localhost:${PORT}/api/lingxing`);
    console.log(`   - 领星基础数据: http://localhost:${PORT}/api/lingxing/basic`);
    console.log(`   - 领星产品管理: http://localhost:${PORT}/api/lingxing/products`);
    console.log(`   - 账户管理: http://localhost:${PORT}/api/accounts`);
  } catch (err) {
    console.error('启动服务器失败:', err);
    fastify.log.error(err);
    process.exit(1);
  }
};

start();

