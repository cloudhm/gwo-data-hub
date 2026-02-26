import prisma from '../../../config/database.js';
import lingXingAmazonService from '../amazon/lingXingAmazonService.js';
import lingXingReportService from '../reports/lingXingReportService.js';
import lingXingFinanceService from '../finance/lingXingFinanceService.js';
import lingXingWarehouseService from '../warehouse/lingXingWarehouseService.js';
import lingXingVcService from '../vc/lingXingVcService.js';
import lingXingPurchaseService from '../purchase/lingXingPurchaseService.js';
import lingxingProductService from '../products/lingxingProductService.js';
import lingXingSalesService from '../sales/lingXingSalesService.js';
import lingxingBasicDataService from '../basic/lingxingBasicDataService.js';
import lingXingLogisticsService from '../logistics/lingXingLogisticsService.js';
import lingXingToolsService from '../tools/lingXingToolsService.js';

const LOG_PREFIX = '[LingXingUnifiedSync]';

/**
 * 支持增量拉取的任务注册表
 * taskType -> { service, methodName, description }
 */
const INCREMENTAL_TASK_REGISTRY = {
  // 亚马逊报表
  allOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncAllOrdersReport', description: '所有订单' },
  fbaOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaOrdersReport', description: 'FBA订单' },
  fbaExchangeOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaExchangeOrdersReport', description: 'FBA换货订单' },
  fbaRefundOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaRefundOrdersReport', description: 'FBA退货订单' },
  fbmReturnOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncFbmReturnOrdersReport', description: 'FBM退货订单' },
  removalOrders: { service: lingXingAmazonService, methodName: 'incrementalSyncRemovalOrdersReport', description: '移除订单' },
  removalShipment: { service: lingXingAmazonService, methodName: 'incrementalSyncRemovalShipmentReport', description: '移除货件' },
  transaction: { service: lingXingAmazonService, methodName: 'incrementalSyncTransactionReport', description: '交易明细' },
  amazonFulfilledShipments: { service: lingXingAmazonService, methodName: 'incrementalSyncAmazonFulfilledShipmentsReport', description: 'Amazon Fulfilled Shipments' },
  fbaInventoryEventDetail: { service: lingXingAmazonService, methodName: 'incrementalSyncFbaInventoryEventDetailReport', description: 'FBA库存动销明细' },
  adjustmentList: { service: lingXingAmazonService, methodName: 'incrementalSyncAdjustmentListReport', description: '盘存记录' },
  reimbursementReport: { service: lingXingAmazonService, methodName: 'incrementalSyncReimbursementReport', description: '亚马逊赔偿报告' },

  // 报表
  salesReport: { service: lingXingReportService, methodName: 'incrementalSyncSalesReport', description: '销量报表' },
  storeSummarySales: { service: lingXingReportService, methodName: 'incrementalSyncStoreSummarySales', description: '店铺汇总销量' },
  productPerformance: { service: lingXingReportService, methodName: 'incrementalSyncProductPerformance', description: '产品表现' },
  asin360HourData: { service: lingXingReportService, methodName: 'incrementalSyncAsin360HourData', description: 'ASIN360小时数据' },
  mskuProfitStatistics: { service: lingXingReportService, methodName: 'incrementalSyncMskuProfitStatistics', description: '利润统计MSKU' },
  profitReportOrder: { service: lingXingFinanceService, methodName: 'incrementalSyncProfitReportOrders', description: '利润报表-订单' },
  profitReportOrderTransaction: { service: lingXingFinanceService, methodName: 'incrementalSyncProfitReportOrderTransaction', description: '利润报表-订单transaction' },
  asinProfitReport: { service: lingXingFinanceService, methodName: 'incrementalSyncAsinProfitReport', description: '利润报表-ASIN' },
  parentAsinProfitReport: { service: lingXingFinanceService, methodName: 'incrementalSyncParentAsinProfitReport', description: '利润报表-父ASIN' },
  sellerProfitReport: { service: lingXingFinanceService, methodName: 'incrementalSyncSellerProfitReport', description: '利润报表-店铺' },
  returnOrderAnalysis: { service: lingXingReportService, methodName: 'incrementalSyncReturnOrderAnalysis', description: '退货分析' },

  // 财务
  feeDetail: { service: lingXingFinanceService, methodName: 'incrementalSyncFeeDetails', description: '费用明细' },
  requestFundsOrder: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsOrders', description: '请款单' },
  requestFundsPoolPurchase: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsPoolPurchase', description: '请款池-货款现结' },
  requestFundsPoolInbound: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsPoolInbound', description: '请款池-货款月结' },
  requestFundsPoolPrepay: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsPoolPrepay', description: '请款池-货款预付款' },
  requestFundsPoolLogistics: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsPoolLogistics', description: '请款池-物流请款' },
  requestFundsPoolCustomFee: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsPoolCustomFee', description: '请款池-其他应付款' },
  requestFundsPoolOtherFee: { service: lingXingFinanceService, methodName: 'incrementalSyncRequestFundsPoolOtherFee', description: '请款池-其他费用' },
  receivableReport: { service: lingXingFinanceService, methodName: 'incrementalSyncReceivableReport', description: '应收报告(按月)' },
  receivableReportDetail: { service: lingXingFinanceService, methodName: 'incrementalSyncReceivableReportDetailList', description: '应收报告-详情-列表(按月)' },
  receivableReportDetailInfo: { service: lingXingFinanceService, methodName: 'incrementalSyncReceivableReportDetailInfo', description: '应收报告-详情-基础信息(按月)' },
  settlementSummary: { service: lingXingFinanceService, methodName: 'incrementalSyncSettlementSummary', description: '结算中心-结算汇总(按日)' },
  settlementTransactionDetail: { service: lingXingFinanceService, methodName: 'incrementalSyncSettlementTransactionDetail', description: '结算中心-交易明细(按sid按日)' },
  inventoryLedgerDetail: { service: lingXingFinanceService, methodName: 'incrementalSyncInventoryLedgerDetail', description: '库存分类账detail(按sellerId按日)' },
  inventoryLedgerSummary: { service: lingXingFinanceService, methodName: 'incrementalSyncInventoryLedgerSummary', description: '库存分类账summary(按sellerId按日)' },
  settlementReport: { service: lingXingFinanceService, methodName: 'incrementalSyncSettlementReport', description: '发货结算报告(按sid按日,更新时间)' },
  fbaCostStream: { service: lingXingFinanceService, methodName: 'incrementalSyncFbaCostStream', description: 'FBA成本流水(按shopName按日)' },
  adsInvoice: { service: lingXingFinanceService, methodName: 'incrementalSyncAdsInvoice', description: '广告发票(按sid按日)' },

  // 仓库
  purchaseReceiptOrder: { service: lingXingWarehouseService, methodName: 'incrementalSyncPurchaseReceiptOrders', description: '收货单' },
  inboundOrder: { service: lingXingWarehouseService, methodName: 'incrementalSyncInboundOrders', description: '入库单' },
  outboundOrder: { service: lingXingWarehouseService, methodName: 'incrementalSyncOutboundOrders', description: '出库单' },
  storageReportLocalAggregate: { service: lingXingReportService, methodName: 'incrementalSyncStorageReportLocalAggregate', description: '库存报表-本地仓-汇总' },
  storageReportLocalDetail: { service: lingXingReportService, methodName: 'incrementalSyncStorageReportLocalDetail', description: '库存报表-本地仓-明细' },
  storageReportOverseasAggregate: { service: lingXingReportService, methodName: 'incrementalSyncStorageReportOverseasAggregate', description: '库存报表-海外仓-汇总' },
  storageReportOverseasDetail: { service: lingXingReportService, methodName: 'incrementalSyncStorageReportOverseasDetail', description: '库存报表-海外仓-明细' },
  storageReportFbaGather: { service: lingXingReportService, methodName: 'incrementalSyncStorageReportFbaGather', description: '库存报表-FBA-汇总' },
  storageReportFbaDetail: { service: lingXingReportService, methodName: 'incrementalSyncStorageReportFbaDetail', description: '库存报表-FBA-明细' },
  fbaStorageFeeMonth: { service: lingXingReportService, methodName: 'incrementalSyncFbaStorageFeeMonth', description: 'FBA月仓储费' },

  // VC
  vcOrder: { service: lingXingVcService, methodName: 'incrementalSyncVcOrders', description: 'VC订单' },
  vcInvoice: { service: lingXingVcService, methodName: 'incrementalSyncVcInvoices', description: 'VC发货单' },

  // 采购
  purchaseOrder: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseOrders', description: '采购单' },
  purchasePlan: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchasePlans', description: '采购计划' },
  purchaseReturnOrder: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseReturnOrders', description: '采购退货单' },
  purchaseChangeOrder: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseChangeOrders', description: '采购变更单' },
  purchaseReportProduct: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseReportProduct', description: '采购报表-产品' },
  purchaseReportSupplier: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseReportSupplier', description: '采购报表-供应商' },
  purchaseReportBuyer: { service: lingXingPurchaseService, methodName: 'incrementalSyncPurchaseReportBuyer', description: '采购报表-采购员' },

  // 产品
  localProduct: { service: lingxingProductService, methodName: 'incrementalSyncLocalProducts', description: '本地产品' },

  // 销售
  salesAmazonOrder: { service: lingXingSalesService, methodName: 'incrementalSyncAmazonOrders', description: '销售-亚马逊订单' },

  // 工具
  operateLog: { service: lingXingToolsService, methodName: 'incrementalSyncOperateLog', description: '运营日志(新)' }
};

/**
 * 非增量（全量/按需）拉取任务注册表
 * 方法签名为 (accountId, options) 或 (accountId, listParams, options)，不要求传入 accountId，由服务内遍历启用账户执行
 * taskType -> { service, methodName, description, hasListParams?: boolean }  hasListParams 为 true 时调用为 (accountId, options.listParams||{}, options)
 */
const FULL_TASK_REGISTRY = {
  // 基础数据（第二参为 useCache，从 options.useCache 取，默认 true）
  sellerLists: { service: lingxingBasicDataService, methodName: 'getSellerLists', description: '亚马逊店铺列表', secondArg: 'useCache' },
  conceptSellerLists: { service: lingxingBasicDataService, methodName: 'getConceptSellerLists', description: '概念店铺列表', secondArg: 'useCache' },
  accountUsers: { service: lingxingBasicDataService, methodName: 'getAccountUsers', description: '账户用户', secondArg: 'useCache' },
  marketplaces: { service: lingxingBasicDataService, methodName: 'getAllMarketplaces', description: '亚马逊所有市场列表', secondArg: 'useCache' },
  worldStates: { service: lingxingBasicDataService, methodName: 'getWorldStates', description: '亚马逊国家下地区列表', secondArg: 'countryCode', thirdArg: 'useCache' },
  currencyRates: { service: lingxingBasicDataService, methodName: 'getCurrencyRates', description: '汇率(当前月份)', secondArg: 'currentMonthDate', thirdArg: 'useCache' },

  // 物流
  channels: { service: lingXingLogisticsService, methodName: 'fetchAllChannels', description: '物流渠道' },
  headLogisticsProviders: { service: lingXingLogisticsService, methodName: 'fetchAllHeadLogisticsProviders', description: '头程物流商', hasListParams: true },
  transportMethods: { service: lingXingLogisticsService, methodName: 'fetchAllTransportMethods', description: '运输方式' },

  // 采购（采购计划/退货单/变更单已支持增量，见 INCREMENTAL_TASK_REGISTRY）
  suppliers: { service: lingXingPurchaseService, methodName: 'fetchAllSuppliers', description: '供应商' },
  purchasers: { service: lingXingPurchaseService, methodName: 'fetchAllPurchasers', description: '采购方' },

  // VC
  vcSellers: { service: lingXingVcService, methodName: 'fetchAllVcSellers', description: 'VC店铺' },
  vcListings: { service: lingXingVcService, methodName: 'fetchAllVcListings', description: 'VC Listing', middleParam: 'vcStoreIds' },
  vcOrdersFull: { service: lingXingVcService, methodName: 'fetchAllVcOrders', description: 'VC订单(全量)', hasListParams: true },
  vcInvoicesFull: { service: lingXingVcService, methodName: 'fetchAllVcInvoices', description: 'VC发货单(全量)', hasListParams: true },

  // 仓库
  warehouses: { service: lingXingWarehouseService, methodName: 'fetchAllWarehouses', description: '仓库列表', hasListParams: true },
  warehouseBins: { service: lingXingWarehouseService, methodName: 'fetchAllWarehouseBins', description: '仓库库位', hasListParams: true },
  fbaWarehouseDetails: { service: lingXingWarehouseService, methodName: 'fetchAllFbaWarehouseDetails', description: 'FBA仓库明细', hasListParams: true },
  inventoryDetails: { service: lingXingWarehouseService, methodName: 'fetchAllInventoryDetails', description: '库存明细', hasListParams: true },
  inventoryBinDetails: { service: lingXingWarehouseService, methodName: 'fetchAllInventoryBinDetails', description: '库位库存明细', hasListParams: true },
  wmsOrders: { service: lingXingWarehouseService, methodName: 'fetchAllWmsOrders', description: 'WMS订单', hasListParams: true },
  overseasWarehouseStockOrders: { service: lingXingWarehouseService, methodName: 'fetchAllOverseasWarehouseStockOrders', description: '海外仓备货单', hasListParams: true },

  // 财务
  feeTypes: { service: lingXingFinanceService, methodName: 'fetchAllFeeTypes', description: '费用类型' },
  feeDetailsFull: { service: lingXingFinanceService, methodName: 'fetchAllFeeDetails', description: '费用明细(全量)', hasListParams: true },
  requestFundsOrdersFull: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsOrders', description: '请款单(全量)', hasListParams: true },
  requestFundsPoolPurchase: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsPoolPurchase', description: '请款池-货款现结(全量)', hasListParams: true },
  requestFundsPoolInbound: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsPoolInbound', description: '请款池-货款月结(全量)', hasListParams: true },
  requestFundsPoolPrepay: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsPoolPrepay', description: '请款池-货款预付款(全量)', hasListParams: true },
  requestFundsPoolLogistics: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsPoolLogistics', description: '请款池-物流请款(全量)', hasListParams: true },
  requestFundsPoolCustomFee: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsPoolCustomFee', description: '请款池-其他应付款(全量)', hasListParams: true },
  requestFundsPoolOtherFee: { service: lingXingFinanceService, methodName: 'fetchAllRequestFundsPoolOtherFee', description: '请款池-其他费用(全量)', hasListParams: true },
  receivableReport: { service: lingXingFinanceService, methodName: 'fetchAllReceivableReportList', description: '应收报告(全量)', hasListParams: true },
  receivableReportDetail: { service: lingXingFinanceService, methodName: 'fetchAllReceivableReportDetailList', description: '应收报告详情(全量)', hasListParams: true },
  settlementSummaryFull: { service: lingXingFinanceService, methodName: 'fetchAllSettlementSummaryList', description: '结算中心-结算汇总(全量)', hasListParams: true },
  settlementTransactionDetailFull: { service: lingXingFinanceService, methodName: 'fetchAllSettlementTransactionDetailList', description: '结算中心-交易明细(全量)', hasListParams: true },
  inventoryLedgerDetailFull: { service: lingXingFinanceService, methodName: 'fetchAllInventoryLedgerDetailList', description: '库存分类账detail(全量)', hasListParams: true },
  inventoryLedgerSummaryFull: { service: lingXingFinanceService, methodName: 'fetchAllInventoryLedgerSummaryList', description: '库存分类账summary(全量)', hasListParams: true },
  settlementReportFull: { service: lingXingFinanceService, methodName: 'fetchAllSettlementReportList', description: '发货结算报告(全量)', hasListParams: true },
  fbaCostStreamFull: { service: lingXingFinanceService, methodName: 'fetchAllFbaCostStreamByDay', description: 'FBA成本流水(全量按日)', hasListParams: true },
  adsInvoiceFull: { service: lingXingFinanceService, methodName: 'fetchAllAdsInvoiceListByDay', description: '广告发票(全量按日)', hasListParams: true },
  profitReportOrderFull: { service: lingXingFinanceService, methodName: 'fetchAllProfitReportOrders', description: '利润报表-订单(全量)', hasListParams: true },
  profitReportOrderTransactionFull: { service: lingXingFinanceService, methodName: 'fetchAllProfitReportOrdersTransaction', description: '利润报表-订单transaction(全量)', hasListParams: true },
  mskuProfitReport: { service: lingXingFinanceService, methodName: 'fetchAllMskuProfitReport', description: 'MSKU利润报表', hasListParams: true },
  sellerProfitReport: { service: lingXingFinanceService, methodName: 'fetchAllSellerProfitReport', description: '卖家利润报表', hasListParams: true },

  // 产品
  localProductsFull: { service: lingxingProductService, methodName: 'fetchAllLocalProducts', description: '本地产品(全量)', hasListParams: true },

  // 销售
  amazonOrdersFull: { service: lingXingSalesService, methodName: 'fetchAllAmazonOrders', description: '亚马逊订单(全量)', hasListParams: true },
  listings: { service: lingXingSalesService, methodName: 'fetchAllListings', description: '亚马逊Listing(全量)' },

  // 亚马逊报表（全量拉取）
  allOrdersReportFull: { service: lingXingAmazonService, methodName: 'fetchAllOrdersReport', description: '所有订单报表(全量)', hasListParams: true },
  storeSummarySalesFull: { service: lingXingReportService, methodName: 'fetchAllStoreSummarySalesByDay', description: '店铺汇总销量(全量)', hasListParams: true },
  reimbursementReportFull: { service: lingXingAmazonService, methodName: 'fetchAllReimbursementReportByDateRange', description: '亚马逊赔偿报告(全量)', hasListParams: true },
  purchaseReportProductFull: { service: lingXingPurchaseService, methodName: 'fetchAllPurchaseReportProductByDay', description: '采购报表-产品(全量)', hasListParams: true },
  purchaseReportSupplierFull: { service: lingXingPurchaseService, methodName: 'fetchAllPurchaseReportSupplierByDay', description: '采购报表-供应商(全量)', hasListParams: true },
  purchaseReportBuyerFull: { service: lingXingPurchaseService, methodName: 'fetchAllPurchaseReportBuyerByDay', description: '采购报表-采购员(全量)', hasListParams: true },
  returnOrderAnalysisFull: { service: lingXingReportService, methodName: 'fetchAllReturnOrderAnalysisByDay', description: '退货分析(全量)', hasListParams: true },
  operateLogFull: { service: lingXingToolsService, methodName: 'fetchAllOperateLogByDay', description: '运营日志(全量)', hasListParams: true },
  fbaStorageFeeMonthFull: { service: lingXingReportService, methodName: 'fetchAllFbaStorageFeeMonthByMonth', description: 'FBA月仓储费(全量)', hasListParams: true },

  // 工具
  keywords: { service: lingXingToolsService, methodName: 'fetchAllKeywords', description: '关键词列表', hasListParams: true }
};

/**
 * 全量同步前需软删除（归档）的领星表：taskType -> [{ model: Prisma 模型名(camelCase), accountField: 账户字段名，null 表示全表归档 }]
 */
const FULL_SYNC_ARCHIVE_MAP = {
  sellerLists: [{ model: 'lingXingSeller', accountField: 'accountId' }],
  conceptSellerLists: [{ model: 'lingXingConceptSeller', accountField: 'accountId' }],
  accountUsers: [{ model: 'lingXingAccountUser', accountField: 'accountId' }],
  marketplaces: [{ model: 'lingXingMarketplace', accountField: null }],
  worldStates: [{ model: 'lingXingWorldState', accountField: null }],
  channels: [{ model: 'lingXingLogisticsChannel', accountField: 'accountId' }],
  headLogisticsProviders: [{ model: 'lingXingHeadLogisticsProvider', accountField: 'accountId' }],
  transportMethods: [{ model: 'lingXingTransportMethod', accountField: 'accountId' }],
  suppliers: [{ model: 'lingXingSupplier', accountField: 'accountId' }],
  purchasers: [{ model: 'lingXingPurchaser', accountField: 'accountId' }],
  vcSellers: [{ model: 'lingXingVcSeller', accountField: 'accountId' }],
  vcListings: [{ model: 'lingXingVcListing', accountField: 'accountId' }],
  vcOrdersFull: [{ model: 'lingXingVcOrder', accountField: 'accountId' }],
  vcInvoicesFull: [{ model: 'lingXingVcInvoice', accountField: 'accountId' }],
  warehouses: [{ model: 'lingXingWarehouse', accountField: 'accountId' }],
  warehouseBins: [{ model: 'lingXingWarehouseBin', accountField: 'accountId' }],
  fbaWarehouseDetails: [{ model: 'lingXingFbaWarehouseDetail', accountField: 'accountId' }],
  inventoryDetails: [{ model: 'lingXingInventoryDetail', accountField: 'accountId' }],
  inventoryBinDetails: [{ model: 'lingXingInventoryBinDetail', accountField: 'accountId' }],
  wmsOrders: [{ model: 'lingXingWmsOrder', accountField: 'accountId' }],
  overseasWarehouseStockOrders: [{ model: 'lingXingOverseasWarehouseStockOrder', accountField: 'accountId' }],
  feeTypes: [{ model: 'lingXingFeeType', accountField: 'accountId' }],
  feeDetailsFull: [{ model: 'lingXingFeeDetail', accountField: 'accountId' }],
  requestFundsOrdersFull: [{ model: 'lingXingRequestFundsOrder', accountField: 'accountId' }],
  requestFundsPoolPurchase: [{ model: 'lingXingRequestFundsPoolPurchase', accountField: 'accountId' }],
  requestFundsPoolInbound: [{ model: 'lingXingRequestFundsPoolInbound', accountField: 'accountId' }],
  requestFundsPoolPrepay: [{ model: 'lingXingRequestFundsPoolPrepay', accountField: 'accountId' }],
  requestFundsPoolLogistics: [{ model: 'lingXingRequestFundsPoolLogistics', accountField: 'accountId' }],
  requestFundsPoolCustomFee: [{ model: 'lingXingRequestFundsPoolCustomFee', accountField: 'accountId' }],
  requestFundsPoolOtherFee: [{ model: 'lingXingRequestFundsPoolOtherFee', accountField: 'accountId' }],
  receivableReport: [{ model: 'lingXingReceivableReport', accountField: 'accountId' }],
  receivableReportDetail: [{ model: 'lingXingReceivableReportDetail', accountField: 'accountId' }],
  receivableReportDetailInfo: [{ model: 'lingXingReceivableReportDetailInfo', accountField: 'accountId' }],
  settlementSummaryFull: [{ model: 'lingXingSettlementSummary', accountField: 'accountId' }],
  settlementTransactionDetailFull: [{ model: 'lingXingSettlementTransactionDetail', accountField: 'accountId' }],
  inventoryLedgerDetailFull: [{ model: 'lingXingInventoryLedgerDetail', accountField: 'accountId' }],
  inventoryLedgerSummaryFull: [{ model: 'lingXingInventoryLedgerSummary', accountField: 'accountId' }],
  settlementReportFull: [{ model: 'lingXingSettlementReport', accountField: 'accountId' }],
  fbaCostStreamFull: [{ model: 'lingXingFbaCostStream', accountField: 'accountId' }],
  adsInvoiceFull: [{ model: 'lingXingAdsInvoice', accountField: 'accountId' }],
  profitReportOrderFull: [{ model: 'lingXingProfitReportOrder', accountField: 'accountId' }],
  profitReportOrderTransactionFull: [{ model: 'lingXingProfitReportOrderTransaction', accountField: 'accountId' }],
  mskuProfitReport: [{ model: 'lingXingMskuProfitReport', accountField: 'accountId' }],
  sellerProfitReport: [{ model: 'lingXingSellerProfitReport', accountField: 'accountId' }],
  localProductsFull: [{ model: 'lingXingLocalProduct', accountField: 'accountId' }],
  amazonOrdersFull: [{ model: 'lingXingAmazonOrder', accountField: 'accountId' }],
  listings: [{ model: 'lingXingAmazonListing', accountField: 'accountId' }],
  allOrdersReportFull: [{ model: 'lingXingAmazonReport', accountField: 'accountId' }],
  storeSummarySalesFull: [{ model: 'lingXingStoreSummarySales', accountField: 'accountId' }],
  reimbursementReportFull: [{ model: 'lingXingReimbursementReport', accountField: 'accountId' }],
  purchaseReportProductFull: [{ model: 'lingXingPurchaseReportProduct', accountField: 'accountId' }],
  purchaseReportSupplierFull: [{ model: 'lingXingPurchaseReportSupplier', accountField: 'accountId' }],
  purchaseReportBuyerFull: [{ model: 'lingXingPurchaseReportBuyer', accountField: 'accountId' }],
  returnOrderAnalysisFull: [{ model: 'lingXingReturnOrderAnalysis', accountField: 'accountId' }],
  operateLogFull: [{ model: 'lingXingOperateLog', accountField: 'accountId' }],
  fbaStorageFeeMonthFull: [{ model: 'lingXingFbaStorageFeeMonth', accountField: 'accountId' }]
};

/**
 * 领星统一同步服务
 * - 不接收 accountId，从数据表遍历所有启用账户
 * - 仅对支持增量拉取的任务使用增量方法
 * - 全量同步前将该任务对应的领星表下该账户（或全表）数据软删除（archived = true），upsert 时由各服务写入 archived = false
 */
class LingXingUnifiedSyncService {
  /**
   * 全量同步前将对应表的数据归档（archived = true）
   * @param {string} accountId - 账户ID
   * @param {string} taskType - 任务类型
   */
  async archiveBeforeFullSync(accountId, taskType) {
    const configs = FULL_SYNC_ARCHIVE_MAP[taskType];
    if (!configs || !Array.isArray(configs)) return;
    for (const { model, accountField } of configs) {
      const client = prisma[model];
      if (!client || typeof client.updateMany !== 'function') continue;
      try {
        const where = accountField ? { [accountField]: accountId } : {};
        const result = await client.updateMany({ where, data: { archived: true } });
        if (result.count > 0) {
          console.log(`${LOG_PREFIX} [full] [${taskType}] 已归档 ${model} accountId=${accountId} 共 ${result.count} 条`);
        }
      } catch (err) {
        console.warn(`${LOG_PREFIX} [full] [${taskType}] 归档 ${model} 失败:`, err?.message || err);
      }
    }
  }

  /**
   * 从数据表获取所有启用账户
   * @returns {Promise<Array<{ id: string, name: string }>>}
   */
  async getActiveAccounts() {
    const accounts = await prisma.lingXingAccount.findMany({
      where: { isActive: true },
      select: { id: true, name: true },
      orderBy: { name: 'asc' }
    });
    return accounts;
  }

  /**
   * 获取所有支持增量拉取的任务类型及说明
   * @returns {Array<{ taskType: string, description: string }>}
   */
  getSupportedIncrementalTaskTypes() {
    return Object.entries(INCREMENTAL_TASK_REGISTRY).map(([taskType, { description }]) => ({
      taskType,
      description
    }));
  }

  /**
   * 判断某任务类型是否支持增量
   */
  isIncrementalSupported(taskType) {
    return !!INCREMENTAL_TASK_REGISTRY[taskType];
  }

  /**
   * 对单个账户执行指定任务类型的增量同步
   * @param {string} accountId - 账户ID
   * @param {string} taskType - 任务类型
   * @param {Object} options - 透传给该任务增量方法的选项
   * @returns {Promise<{ accountId, accountName?, taskType, success, summary?, error? }>}
   */
  async runIncrementalSyncForAccount(accountId, taskType, options = {}) {
    const entry = INCREMENTAL_TASK_REGISTRY[taskType];
    if (!entry) {
      return { accountId, taskType, success: false, error: `不支持的增量任务类型: ${taskType}` };
    }
    const { service, methodName } = entry;
    const method = service[methodName];
    if (typeof method !== 'function') {
      return { accountId, taskType, success: false, error: `服务方法不存在: ${methodName}` };
    }
    try {
      const result = await method.call(service, accountId, options);
      return {
        accountId,
        taskType,
        success: true,
        summary: result?.summary,
        results: result?.results
      };
    } catch (err) {
      const message = err?.message || String(err);
      console.error(`${LOG_PREFIX} accountId=${accountId} taskType=${taskType} 失败:`, message);
      return { accountId, taskType, success: false, error: message };
    }
  }

  /**
   * 按任务类型执行增量同步：从数据表获取所有启用账户，逐账户执行该任务的增量方法
   * @param {string} taskType - 任务类型（见 getSupportedIncrementalTaskTypes）
   * @param {Object} options - 透传给各账户的增量方法选项（如 endDate, defaultLookbackDays, delayBetweenShops 等）
   * @returns {Promise<{ taskType, description, accountCount, results: Array<{ accountId, accountName?, success, summary?, error? }>, summary }>}
   */
  async runIncrementalSyncByTaskType(taskType, options = {}) {
    const entry = INCREMENTAL_TASK_REGISTRY[taskType];
    if (!entry) {
      throw new Error(`不支持的增量任务类型: ${taskType}，可选: ${Object.keys(INCREMENTAL_TASK_REGISTRY).join(', ')}`);
    }

    const accounts = await this.getActiveAccounts();
    if (accounts.length === 0) {
      console.warn(`${LOG_PREFIX} 无启用账户，跳过 taskType=${taskType}`);
      return {
        taskType,
        description: entry.description,
        accountCount: 0,
        results: [],
        summary: { successCount: 0, failCount: 0, totalRecords: 0, message: '无启用账户' }
      };
    }

    console.log(`${LOG_PREFIX} [${taskType}] 开始 共 ${accounts.length} 个账户`);
    const results = [];
    let successCount = 0;
    let failCount = 0;
    let totalRecords = 0;

    for (const account of accounts) {
      const one = await this.runIncrementalSyncForAccount(account.id, taskType, options);
      results.push({ ...one, accountName: account.name });
      if (one.success) {
        successCount++;
        const s = one.summary || {};
        totalRecords += s.totalRecords ?? s.recordCount ?? 0;
      } else {
        failCount++;
      }
    }

    const summary = { accountCount: accounts.length, successCount, failCount, totalRecords };
    console.log(`${LOG_PREFIX} [${taskType}] 结束`, summary);
    return {
      taskType,
      description: entry.description,
      accountCount: accounts.length,
      results,
      summary
    };
  }

  /**
   * 执行全部支持增量拉取的任务：按任务类型依次执行，每种类型会遍历所有启用账户
   * @param {Object} options - 透传给各任务增量方法的选项
   * @param {Array<string>} taskTypes - 仅执行这些任务类型；不传则执行全部
   * @returns {Promise<{ accountCount, taskResults: Array<{ taskType, description, results, summary }>, summary }>}
   */
  async runAllIncrementalSync(options = {}, taskTypes = null) {
    const list = taskTypes && taskTypes.length > 0
      ? taskTypes.filter(t => this.isIncrementalSupported(t))
      : Object.keys(INCREMENTAL_TASK_REGISTRY);

    if (list.length === 0) {
      return {
        accountCount: 0,
        taskResults: [],
        summary: { message: '没有可执行的增量任务', taskCount: 0 }
      };
    }

    const accounts = await this.getActiveAccounts();
    const taskResults = [];

    for (const taskType of list) {
      try {
        const taskResult = await this.runIncrementalSyncByTaskType(taskType, options);
        taskResults.push(taskResult);
      } catch (err) {
        console.error(`${LOG_PREFIX} runAllIncrementalSync taskType=${taskType} 异常:`, err?.message);
        taskResults.push({
          taskType,
          description: INCREMENTAL_TASK_REGISTRY[taskType]?.description || taskType,
          accountCount: accounts.length,
          results: [],
          summary: { successCount: 0, failCount: accounts.length, error: err?.message }
        });
      }
    }

    const totalSuccess = taskResults.reduce((s, r) => s + (r.summary?.successCount ?? 0), 0);
    const totalFail = taskResults.reduce((s, r) => s + (r.summary?.failCount ?? 0), 0);
    const totalRecords = taskResults.reduce((s, r) => s + (r.summary?.totalRecords ?? 0), 0);

    return {
      accountCount: accounts.length,
      taskResults,
      summary: {
        taskCount: list.length,
        totalSuccess,
        totalFail,
        totalRecords
      }
    };
  }

  // ---------- 非增量（全量/按需）任务 ----------

  /**
   * 获取所有非增量任务类型及说明
   * @returns {Array<{ taskType: string, description: string }>}
   */
  getSupportedFullTaskTypes() {
    return Object.entries(FULL_TASK_REGISTRY).map(([taskType, { description }]) => ({
      taskType,
      description
    }));
  }

  /**
   * 判断某任务类型是否在非增量注册表中
   */
  isFullTaskSupported(taskType) {
    return !!FULL_TASK_REGISTRY[taskType];
  }

  /**
   * 对单个账户执行指定任务类型的全量/按需拉取
   * @param {string} accountId - 账户ID
   * @param {string} taskType - 任务类型（FULL_TASK_REGISTRY 的 key）
   * @param {Object} options - 透传选项；若任务有 hasListParams，则 options.listParams 作为第二参数
   * @returns {Promise<{ accountId, taskType, success, result?, error? }>}
   */
  async runFullSyncForAccount(accountId, taskType, options = {}) {
    const entry = FULL_TASK_REGISTRY[taskType];
    if (!entry) {
      return { accountId, taskType, success: false, error: `不支持的全量任务类型: ${taskType}` };
    }
    const { service, methodName, hasListParams, secondArg, thirdArg, middleParam } = entry;
    const method = service[methodName];
    if (typeof method !== 'function') {
      return { accountId, taskType, success: false, error: `服务方法不存在: ${methodName}` };
    }
    try {
      await this.archiveBeforeFullSync(accountId, taskType);
      let args;
      if (hasListParams) {
        args = [accountId, options.listParams ?? options.searchParams ?? {}, options];
      } else if (secondArg === 'useCache') {
        args = [accountId, options.useCache !== false];
      } else if (secondArg === 'currentMonthDate' && thirdArg === 'useCache') {
        const now = new Date();
        const date = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
        args = [accountId, date, options.useCache !== false];
      } else if (secondArg === 'countryCode' && thirdArg === 'useCache') {
        args = [accountId, options.countryCode ?? 'US', options.useCache !== false];
      } else if (middleParam) {
        args = [accountId, options[middleParam] ?? null, options];
      } else {
        args = [accountId, options];
      }
      const result = await method.apply(service, args);
      return { accountId, taskType, success: true, result };
    } catch (err) {
      const message = err?.message || String(err);
      console.error(`${LOG_PREFIX} [full] accountId=${accountId} taskType=${taskType} 失败:`, message);
      return { accountId, taskType, success: false, error: message };
    }
  }

  /**
   * 按任务类型执行全量/按需拉取：从数据表获取所有启用账户，逐账户执行
   * @param {string} taskType - 任务类型
   * @param {Object} options - 透传选项（可含 listParams/searchParams 等）
   * @returns {Promise<{ taskType, description, accountCount, results, summary }>}
   */
  async runFullSyncByTaskType(taskType, options = {}) {
    const entry = FULL_TASK_REGISTRY[taskType];
    if (!entry) {
      throw new Error(`不支持的全量任务类型: ${taskType}，可选: ${Object.keys(FULL_TASK_REGISTRY).join(', ')}`);
    }

    const accounts = await this.getActiveAccounts();
    if (accounts.length === 0) {
      console.warn(`${LOG_PREFIX} [full] 无启用账户，跳过 taskType=${taskType}`);
      return {
        taskType,
        description: entry.description,
        accountCount: 0,
        results: [],
        summary: { successCount: 0, failCount: 0, message: '无启用账户' }
      };
    }

    console.log(`${LOG_PREFIX} [full] [${taskType}] 开始 共 ${accounts.length} 个账户`);
    const results = [];
    let successCount = 0;
    let failCount = 0;

    for (const account of accounts) {
      const one = await this.runFullSyncForAccount(account.id, taskType, options);
      results.push({ ...one, accountName: account.name });
      if (one.success) successCount++;
      else failCount++;
    }

    const summary = { accountCount: accounts.length, successCount, failCount };
    console.log(`${LOG_PREFIX} [full] [${taskType}] 结束`, summary);
    return {
      taskType,
      description: entry.description,
      accountCount: accounts.length,
      results,
      summary
    };
  }

  /**
   * 执行全部（或指定）非增量任务：按任务类型依次执行，每种类型遍历所有启用账户
   * @param {Object} options - 透传给各任务的选项
   * @param {Array<string>} taskTypes - 仅执行这些任务类型；不传则执行全部
   * @returns {Promise<{ accountCount, taskResults, summary }>}
   */
  async runAllFullSync(options = {}, taskTypes = null) {
    const list = taskTypes && taskTypes.length > 0
      ? taskTypes.filter(t => this.isFullTaskSupported(t))
      : Object.keys(FULL_TASK_REGISTRY);

    if (list.length === 0) {
      return {
        accountCount: 0,
        taskResults: [],
        summary: { message: '没有可执行的全量任务', taskCount: 0 }
      };
    }

    const accounts = await this.getActiveAccounts();
    const taskResults = [];

    for (const taskType of list) {
      try {
        const taskResult = await this.runFullSyncByTaskType(taskType, options);
        taskResults.push(taskResult);
      } catch (err) {
        console.error(`${LOG_PREFIX} runAllFullSync taskType=${taskType} 异常:`, err?.message);
        taskResults.push({
          taskType,
          description: FULL_TASK_REGISTRY[taskType]?.description || taskType,
          accountCount: accounts.length,
          results: [],
          summary: { successCount: 0, failCount: accounts.length, error: err?.message }
        });
      }
    }

    const totalSuccess = taskResults.reduce((s, r) => s + (r.summary?.successCount ?? 0), 0);
    const totalFail = taskResults.reduce((s, r) => s + (r.summary?.failCount ?? 0), 0);

    return {
      accountCount: accounts.length,
      taskResults,
      summary: { taskCount: list.length, totalSuccess, totalFail }
    };
  }
}

export default new LingXingUnifiedSyncService();
export { INCREMENTAL_TASK_REGISTRY, FULL_TASK_REGISTRY, FULL_SYNC_ARCHIVE_MAP };
