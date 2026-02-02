/**
 * 示例定时任务 - 仅作占位与心跳，后续可替换为真实业务
 * Cron 表达式: 分 时 日 月 周
 * 例如: '1 * * * *' 每分钟, '0 0 * * *' 每天 0 点
 */
export const exampleJob = {
  name: 'example-heartbeat',
  cronExpression: '*/5 * * * *', // 每 5 分钟执行一次
  enabled: true, // 设为 false 可关闭此任务
  async handler() {
    // 占位逻辑，后续在此写实际任务，如拉取亚马逊/领星数据
    console.log('[Job] example-heartbeat 执行');
  },
};
