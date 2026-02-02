/**
 * 定时任务注册表
 * 新增任务：在 jobs 目录下新建文件并在此引入，格式见 exampleJob.js
 */
import { exampleJob } from './exampleJob.js';

export const jobs = [
  exampleJob,
  // 后续定时任务在此追加，例如：
  // syncAmazonDataJob,
  // syncLingxingDataJob,
];
