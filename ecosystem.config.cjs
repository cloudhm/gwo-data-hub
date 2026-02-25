/**
 * PM2 进程管理配置 - gwo-data-hub 数据中台服务
 * 部署: pm2 start ecosystem.config.cjs
 * 重启: pm2 reload ecosystem.config.cjs
 * 查看: pm2 status / pm2 logs
 */
module.exports = {
  apps: [
    {
      name: 'gwo-data-hub',
      script: 'src/index.js',
      interpreter: 'node',
      cwd: __dirname,

      instances: 1,
      exec_mode: 'fork',

      env: { NODE_ENV: 'development' },
      env_production: { NODE_ENV: 'production' },

      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: './logs/out.log',
      error_file: './logs/error.log',
      merge_logs: true,

      autorestart: true,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 4000,
      max_memory_restart: '1G',

      watch: false,
      ignore_watch: ['node_modules', 'logs', '.git', 'prisma'],
    },
    {
      name: 'gwo-data-hub-worker',
      script: 'src/worker.js',
      interpreter: 'node',
      cwd: __dirname,

      instances: 1,
      exec_mode: 'fork',

      env: { NODE_ENV: 'development' },
      env_production: { NODE_ENV: 'production' },

      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      out_file: './logs/worker-out.log',
      error_file: './logs/worker-error.log',
      merge_logs: true,

      autorestart: true,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 4000,
      max_memory_restart: '500M',

      watch: false,
      ignore_watch: ['node_modules', 'logs', '.git', 'prisma'],
    },
  ],
};
