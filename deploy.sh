#!/usr/bin/env bash
#
# 部署脚本：拉取代码、安装依赖、Prisma 生成、数据库同步、PM2 重启
# 用法: ./deploy.sh [--env production]
#

set -e
cd "$(dirname "$0")"

ENV_ARG=""
if [[ "$1" == "--env" && -n "$2" ]]; then
  ENV_ARG="--env $2"
fi

echo "[Deploy] 1/5 拉取代码..."
git pull

echo "[Deploy] 2/5 安装依赖..."
npm install --production=false

echo "[Deploy] 3/5 Prisma 生成..."
npx prisma generate

echo "[Deploy] 4/5 数据库同步 (prisma db push)..."
npm run prisma:push

echo "[Deploy] 5/5 PM2 重启..."
if pm2 list | grep -q "gwo-data-hub"; then
  pm2 reload ecosystem.config.cjs $ENV_ARG
  echo "[Deploy] 完成，已零停机重载。"
else
  pm2 start ecosystem.config.cjs $ENV_ARG
  echo "[Deploy] 完成，已首次启动。"
fi

pm2 save 2>/dev/null || true
