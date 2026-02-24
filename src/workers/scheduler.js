/**
 * 定时任务调度器 - 基于 node-cron
 * 在 workers/jobs 下定义任务，在此统一注册。
 * 调度层串行：同一时刻触发的多个任务会依次执行，避免并发调用领星 API 触发限流。
 */
import cron from 'node-cron';
import { jobs } from './jobs/index.js';

const scheduledTasks = [];

/** 待执行任务队列 { name, handler } */
const jobQueue = [];
/** 是否正在执行某个任务 */
let isRunning = false;

async function processQueue() {
  if (isRunning || jobQueue.length === 0) return;
  const { name, handler } = jobQueue.shift();
  isRunning = true;
  const start = Date.now();
  try {
    await handler();
    console.log(`[Scheduler] ${name} 执行完成，耗时 ${Date.now() - start}ms`);
  } catch (err) {
    console.error(`[Scheduler] ${name} 执行失败:`, err);
  } finally {
    isRunning = false;
    if (jobQueue.length > 0) {
      setImmediate(processQueue);
    }
  }
}

export async function startScheduler() {
  for (const job of jobs) {
    const { name, cronExpression, handler, enabled = true } = job;
    if (!enabled) {
      console.log(`[Scheduler] 任务已禁用: ${name}`);
      continue;
    }
    if (!cron.validate(cronExpression)) {
      console.error(`[Scheduler] 无效的 cron 表达式 "${cronExpression}"，任务: ${name}`);
      continue;
    }
    const task = cron.schedule(cronExpression, () => {
      jobQueue.push({ name, handler });
      processQueue();
    });
    scheduledTasks.push({ name, task });
    console.log(`[Scheduler] 已注册（串行）: ${name} (${cronExpression})`);
  }
}

export async function stopScheduler() {
  for (const { name, task } of scheduledTasks) {
    task.stop();
    console.log(`[Scheduler] 已停止: ${name}`);
  }
  scheduledTasks.length = 0;
  jobQueue.length = 0;
  isRunning = false;
}
