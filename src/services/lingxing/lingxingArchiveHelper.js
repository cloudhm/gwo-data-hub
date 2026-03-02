/**
 * 领星归档辅助：将即将归档的数据迁移到对应的 History 表后从主表删除，避免唯一键冲突。
 * 原逻辑为 updateMany({ archived: true })，会导致主表存在 archived=true 与后续 upsert 的 archived=false 同业务键冲突。
 * 新逻辑：先 INSERT 到 xxx_history，再 DELETE 主表，主表只保留当前有效数据。
 */

const BATCH_SIZE = 500;
const LOG_PREFIX = '[LingXingArchive]';

/**
 * 将主表中符合 where 条件的记录迁移到对应 History 表，然后从主表删除。
 * @param {object} prisma - Prisma 实例
 * @param {string} modelName - 主表 Prisma 模型名（camelCase），如 'lingXingSeller'
 * @param {string} historyModelName - History 表 Prisma 模型名，如 'lingXingSellerHistory'
 * @param {object} where - 查询条件（与主表 findMany 一致）
 * @param {{ skipDelete?: boolean }} options - skipDelete: 仅迁移不删除（用于数据迁移脚本）
 * @returns {{ moved: number, deleted: number }}
 */
async function moveToHistoryAndDelete(prisma, modelName, historyModelName, where, options = {}) {
  const client = prisma[modelName];
  const historyClient = prisma[historyModelName];
  if (!client || typeof client.findMany !== 'function') {
    throw new Error(`Invalid model: ${modelName}`);
  }
  if (!historyClient || typeof historyClient.createMany !== 'function') {
    throw new Error(`Invalid history model: ${historyModelName}`);
  }

  let moved = 0;
  let offset = 0;

  // 先批量复制到 History，不删除，避免分页错位
  while (true) {
    const batch = await client.findMany({
      where,
      skip: offset,
      take: BATCH_SIZE,
      orderBy: { id: 'asc' }
    });
    if (batch.length === 0) break;

    const now = new Date();
    const historyRows = batch.map((row) => {
      const { id, createdAt, updatedAt, ...rest } = row;
      return {
        id,
        ...rest,
        createdAt,
        updatedAt,
        archivedAt: updatedAt || now
      };
    });

    await historyClient.createMany({
      data: historyRows,
      skipDuplicates: true
    });
    moved += batch.length;
    offset += batch.length;

    if (batch.length < BATCH_SIZE) break;
  }

  let deleted = 0;
  if (!options.skipDelete && moved > 0) {
    const delResult = await client.deleteMany({ where });
    deleted = delResult.count;
  }

  return { moved, deleted };
}

/**
 * 将主表中已标记为 archived=true 的记录迁移到 History 表后删除（用于一次性数据迁移）。
 * @param {object} prisma
 * @param {string} modelName
 * @param {string} historyModelName
 * @returns {{ moved: number, deleted: number }}
 */
async function moveExistingArchivedToHistory(prisma, modelName, historyModelName) {
  return moveToHistoryAndDelete(prisma, modelName, historyModelName, { archived: true });
}

export { moveToHistoryAndDelete, moveExistingArchivedToHistory, BATCH_SIZE, LOG_PREFIX };
