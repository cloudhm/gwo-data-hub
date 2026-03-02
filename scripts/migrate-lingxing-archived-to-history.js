/**
 * 一次性迁移：将主表中已标记为 archived=true 的记录迁移到对应 History 表后删除。
 * 使用前请确保已执行 prisma migrate 创建所有 xxx_history 表。
 * 运行: node scripts/migrate-lingxing-archived-to-history.js
 */

import prisma from '../src/config/database.js';
import { moveExistingArchivedToHistory } from '../src/services/lingxing/lingxingArchiveHelper.js';

const MODEL_HISTORY_PAIRS = [
  ['lingXingSeller', 'lingXingSellerHistory'],
  ['lingXingConceptSeller', 'lingXingConceptSellerHistory'],
  ['lingXingAccountUser', 'lingXingAccountUserHistory'],
  ['lingXingMarketplace', 'lingXingMarketplaceHistory'],
  ['lingXingWorldState', 'lingXingWorldStateHistory'],
  ['lingXingLogisticsChannel', 'lingXingLogisticsChannelHistory'],
  ['lingXingHeadLogisticsProvider', 'lingXingHeadLogisticsProviderHistory'],
  ['lingXingTransportMethod', 'lingXingTransportMethodHistory'],
  ['lingXingSupplier', 'lingXingSupplierHistory'],
  ['lingXingPurchaser', 'lingXingPurchaserHistory'],
  ['lingXingVcSeller', 'lingXingVcSellerHistory'],
  ['lingXingVcListing', 'lingXingVcListingHistory'],
  ['lingXingVcOrder', 'lingXingVcOrderHistory'],
  ['lingXingVcInvoice', 'lingXingVcInvoiceHistory'],
  ['lingXingWarehouse', 'lingXingWarehouseHistory'],
  ['lingXingWarehouseBin', 'lingXingWarehouseBinHistory'],
  ['lingXingFbaWarehouseDetail', 'lingXingFbaWarehouseDetailHistory'],
  ['lingXingInventoryDetail', 'lingXingInventoryDetailHistory'],
  ['lingXingInventoryBinDetail', 'lingXingInventoryBinDetailHistory'],
  ['lingXingWmsOrder', 'lingXingWmsOrderHistory'],
  ['lingXingOverseasWarehouseStockOrder', 'lingXingOverseasWarehouseStockOrderHistory'],
  ['lingXingFeeType', 'lingXingFeeTypeHistory'],
  ['lingXingFeeDetail', 'lingXingFeeDetailHistory'],
  ['lingXingRequestFundsOrder', 'lingXingRequestFundsOrderHistory'],
  ['lingXingRequestFundsPoolPurchase', 'lingXingRequestFundsPoolPurchaseHistory'],
  ['lingXingRequestFundsPoolInbound', 'lingXingRequestFundsPoolInboundHistory'],
  ['lingXingRequestFundsPoolPrepay', 'lingXingRequestFundsPoolPrepayHistory'],
  ['lingXingRequestFundsPoolLogistics', 'lingXingRequestFundsPoolLogisticsHistory'],
  ['lingXingRequestFundsPoolCustomFee', 'lingXingRequestFundsPoolCustomFeeHistory'],
  ['lingXingRequestFundsPoolOtherFee', 'lingXingRequestFundsPoolOtherFeeHistory'],
  ['lingXingReceivableReport', 'lingXingReceivableReportHistory'],
  ['lingXingReceivableReportDetail', 'lingXingReceivableReportDetailHistory'],
  ['lingXingReceivableReportDetailInfo', 'lingXingReceivableReportDetailInfoHistory'],
  ['lingXingSettlementSummary', 'lingXingSettlementSummaryHistory'],
  ['lingXingSettlementTransactionDetail', 'lingXingSettlementTransactionDetailHistory'],
  ['lingXingInventoryLedgerDetail', 'lingXingInventoryLedgerDetailHistory'],
  ['lingXingInventoryLedgerSummary', 'lingXingInventoryLedgerSummaryHistory'],
  ['lingXingSettlementReport', 'lingXingSettlementReportHistory'],
  ['lingXingFbaCostStream', 'lingXingFbaCostStreamHistory'],
  ['lingXingAdsInvoice', 'lingXingAdsInvoiceHistory'],
  ['lingXingProfitReportOrder', 'lingXingProfitReportOrderHistory'],
  ['lingXingProfitReportOrderTransaction', 'lingXingProfitReportOrderTransactionHistory'],
  ['lingXingMskuProfitReport', 'lingXingMskuProfitReportHistory'],
  ['lingXingSellerProfitReport', 'lingXingSellerProfitReportHistory'],
  ['lingXingLocalProduct', 'lingXingLocalProductHistory'],
  ['lingXingAmazonOrder', 'lingXingAmazonOrderHistory'],
  ['lingXingAmazonListing', 'lingXingAmazonListingHistory'],
  ['lingXingAmazonReport', 'lingXingAmazonReportHistory'],
  ['lingXingStoreSummarySales', 'lingXingStoreSummarySalesHistory'],
  ['lingXingReimbursementReport', 'lingXingReimbursementReportHistory'],
  ['lingXingPurchaseReportProduct', 'lingXingPurchaseReportProductHistory'],
  ['lingXingPurchaseReportSupplier', 'lingXingPurchaseReportSupplierHistory'],
  ['lingXingPurchaseReportBuyer', 'lingXingPurchaseReportBuyerHistory'],
  ['lingXingReturnOrderAnalysis', 'lingXingReturnOrderAnalysisHistory'],
  ['lingXingOperateLog', 'lingXingOperateLogHistory'],
  ['lingXingFbaStorageFeeMonth', 'lingXingFbaStorageFeeMonthHistory'],
  // 报表/产品表现/ASIN360/利润统计/库存报表/关键词（按 schema 中 History 表补全）
  ['lingXingSalesReport', 'lingXingSalesReportHistory'],
  ['lingXingSalesReportItem', 'lingXingSalesReportItemHistory'],
  ['lingXingProductPerformance', 'lingXingProductPerformanceHistory'],
  ['lingXingProductPerformancePage', 'lingXingProductPerformancePageHistory'],
  ['lingXingAsin360HourData', 'lingXingAsin360HourDataHistory'],
  ['lingXingMskuProfitStatistics', 'lingXingMskuProfitStatisticsHistory'],
  ['lingXingMskuProfitStatisticsItem', 'lingXingMskuProfitStatisticsItemHistory'],
  ['lingXingStorageReportLocalAggregate', 'lingXingStorageReportLocalAggregateHistory'],
  ['lingXingStorageReportLocalDetail', 'lingXingStorageReportLocalDetailHistory'],
  ['lingXingStorageReportOverseasAggregate', 'lingXingStorageReportOverseasAggregateHistory'],
  ['lingXingStorageReportOverseasDetail', 'lingXingStorageReportOverseasDetailHistory'],
  ['lingXingStorageReportFbaGather', 'lingXingStorageReportFbaGatherHistory'],
  ['lingXingStorageReportFbaDetail', 'lingXingStorageReportFbaDetailHistory'],
  ['lingXingKeywordRank', 'lingXingKeywordRankHistory'],
  ['lingXingAsinProfitReport', 'lingXingAsinProfitReportHistory'],
  ['lingXingParentAsinProfitReport', 'lingXingParentAsinProfitReportHistory']
];

async function main() {
  console.log('[migrate-lingxing-archived] 开始迁移已归档数据到 History 表...');
  let totalMoved = 0;
  let totalDeleted = 0;
  const errors = [];

  for (const [modelName, historyModelName] of MODEL_HISTORY_PAIRS) {
    if (!prisma[modelName] || !prisma[historyModelName]) {
      console.log(`  跳过 ${modelName}（主表或 History 表未在 schema 中）`);
      continue;
    }
    try {
      const { moved, deleted } = await moveExistingArchivedToHistory(prisma, modelName, historyModelName);
      if (moved > 0) {
        console.log(`  ${modelName}: 迁移 ${moved} 条, 删除 ${deleted} 条`);
        totalMoved += moved;
        totalDeleted += deleted;
      }
    } catch (err) {
      errors.push({ model: modelName, error: err?.message || String(err) });
      console.error(`  ${modelName} 失败:`, err?.message || err);
    }
  }

  console.log(`[migrate-lingxing-archived] 完成. 总迁移 ${totalMoved} 条, 总删除 ${totalDeleted} 条. 错误数: ${errors.length}`);
  if (errors.length > 0) {
    console.error('错误详情:', errors);
    process.exit(1);
  }
  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
