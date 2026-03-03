/**
 * Worker 进程入口 - 定时任务调度
 * 独立于 API 服务运行，由 PM2 单独管理
 *
 * 调试：按需执行单个 job（不启动 cron）
 *   node src/worker.js --run <taskType>        # 按 SYNC_TASKS 定义执行增量/全量
 *   node src/worker.js --run <taskType> --full # 强制全量
 *   node src/worker.js --run list              # 列出所有 taskType
 */
import dotenv from 'dotenv';
import jobTaskStatusService from './services/jobTaskStatusService.js';
import { startScheduler, stopScheduler, enqueueJob } from './workers/scheduler.js';
import { runSyncJobByTaskType, SYNC_TASKS } from './workers/jobs/syncJob.js';

dotenv.config();

const log = (msg) => console.log(`[Worker] ${new Date().toISOString()} ${msg}`);

/** 解析 --run <taskType> 和 --full */
function parseRunArgs() {
  const argv = process.argv.slice(2);
  const runIdx = argv.indexOf('--run');
  if (runIdx === -1 || !argv[runIdx + 1]) return null;
  const taskType = argv[runIdx + 1];
  const full = argv.includes('--full');
  return { taskType, full };
}

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
  const runArgs = parseRunArgs();

  if (runArgs) {
    // CLI 模式：执行单个 job 后退出
    const { taskType, full } = runArgs;
    if (taskType === 'list') {
      console.log('可用 taskType（--run <taskType>）：');
      SYNC_TASKS.forEach(([t, label, fullFlag]) => {
        console.log(`  ${t}  ${fullFlag ? '[全量]' : '[增量]'} ${label}`);
      });
      process.exit(0);
      return;
    }
    log(`按需执行: ${taskType}${full ? ' (全量)' : ''}`);
    try {
      await runSyncJobByTaskType(taskType, { full });
      log(`${taskType} 执行完成`);
      process.exit(0);
    } catch (err) {
      console.error('[Worker] 执行失败:', err?.message ?? err);
      process.exit(1);
    }
    return;
  }

  // 常驻模式：启动 cron 调度器
  log('Worker 进程启动');
  await startScheduler();

  // PM2 内存重启后恢复：本轮未完成/待执行的任务重新入队（不修改表结构）
  try {
    const roundStart = jobTaskStatusService.getRoundStart(new Date());
    const taskTypes = SYNC_TASKS.map(([t]) => t);
    const toRecover = await jobTaskStatusService.listTaskTypesNotCompletedInRound(roundStart, taskTypes);
    if (toRecover.length > 0) {
      log(`恢复本轮未完成任务: ${toRecover.length} 个 (roundStart: ${roundStart.toISOString()})`);
      for (const taskType of toRecover) {
        enqueueJob(`sync-job-${taskType}`, () => runSyncJobByTaskType(taskType, {}));
      }
    }
  } catch (err) {
    console.error('[Worker] 恢复未完成任务失败:', err?.message ?? err);
  }

  log('调度器已启动，等待定时任务执行');
};

run().catch((err) => {
  console.error('[Worker] 启动失败:', err);
  process.exit(1);
});
