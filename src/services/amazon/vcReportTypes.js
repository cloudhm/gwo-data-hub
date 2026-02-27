/**
 * VC 报表类型配置（参考 Report Type Values - Analytics Vendor）
 * https://developer-docs.amazon.com/sp-api/docs/report-type-values
 * https://developer-docs.amazon.com/sp-api/docs/report-type-values-analytics
 */
export const VC_REPORT_TYPES = [
  {
    reportType: 'GET_VENDOR_REAL_TIME_INVENTORY_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    maxSpanDays: 7,
    lookbackDays: 30,
    reportOptions: null
  },
  {
    reportType: 'GET_VENDOR_REAL_TIME_TRAFFIC_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    maxSpanDays: 14,
    lookbackDays: 30,
    reportOptions: null
  },
  {
    reportType: 'GET_VENDOR_REAL_TIME_SALES_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    maxSpanDays: 14,
    lookbackDays: 30,
    reportOptions: null
  },
  {
    reportType: 'GET_VENDOR_SALES_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    reportOptions: { reportPeriod: 'DAY', sellingProgram: 'RETAIL', distributorView: 'SOURCING' },
    maxSpanDays: 15,
    lookbackDays: 365
  },
  {
    reportType: 'GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    reportOptions: { reportPeriod: 'DAY' },
    maxSpanDays: 15,
    lookbackDays: 365
  },
  {
    reportType: 'GET_VENDOR_TRAFFIC_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    reportOptions: { reportPeriod: 'DAY' },
    maxSpanDays: 15,
    lookbackDays: 365
  },
  {
    reportType: 'GET_VENDOR_FORECASTING_REPORT',
    supportsDateRange: false,
    supportsMarketplaceIds: true,
    reportOptions: { sellingProgram: 'RETAIL' },
    oneMarketplaceOnly: true
  },
  {
    reportType: 'GET_VENDOR_INVENTORY_REPORT',
    supportsDateRange: true,
    supportsMarketplaceIds: true,
    reportOptions: { reportPeriod: 'DAY', sellingProgram: 'RETAIL', distributorView: 'SOURCING' },
    maxSpanDays: 15,
    lookbackDays: 365
  }
];
