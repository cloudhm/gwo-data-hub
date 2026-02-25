/**
 * Worker 进程入口 - 定时任务调度
 * 独立于 API 服务运行，由 PM2 单独管理
 */
import dotenv from 'dotenv';
import { startScheduler, stopScheduler } from './workers/scheduler.js';

dotenv.config();

const log = (msg) => console.log(`[Worker] ${new Date().toISOString()} ${msg}`);

process.on('SIGINT', async () => {
  log('收到 SIGINT，正在停止调度器...');
  await stopScheduler();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  log('收到 SIGTERM，正在停止调度器...');
  await stopScheduler();
  process.exit(0);
});

const run = async () => {
  log('Worker 进程启动');
  await startScheduler();
  log('调度器已启动，等待定时任务执行');
};

run().catch((err) => {
  console.error('[Worker] 启动失败:', err);
  process.exit(1);
});
