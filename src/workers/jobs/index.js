/**
 * 定时任务注册表
 * 新增任务：在 jobs 目录下新建文件并在此引入，格式见 exampleJob.js
 */
import { syncJobs } from './syncJob.js';

export const jobs = [
  ...syncJobs
];
