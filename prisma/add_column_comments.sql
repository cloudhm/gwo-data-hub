-- 为产品开发定价表 - 普通型添加列注释
COMMENT ON COLUMN ads_dev_pricing_normal.product_name IS '产品名称';
COMMENT ON COLUMN ads_dev_pricing_normal.competitor_price IS '竞品单价';
COMMENT ON COLUMN ads_dev_pricing_normal.destination_country IS '目的国';
COMMENT ON COLUMN ads_dev_pricing_normal.transport_method IS '运输方式';
COMMENT ON COLUMN ads_dev_pricing_normal.real_selling_price IS '真实售价（前端）';
COMMENT ON COLUMN ads_dev_pricing_normal.price_without_tax IS '不含税售价（前端）';
COMMENT ON COLUMN ads_dev_pricing_normal.amazon_fba IS '亚马逊FBA';
COMMENT ON COLUMN ads_dev_pricing_normal.amazon_commission IS '亚马逊佣金';
COMMENT ON COLUMN ads_dev_pricing_normal.amazon_gross_profit IS '亚马逊毛利';
COMMENT ON COLUMN ads_dev_pricing_normal.amazon_gross_profit_rate IS '亚马逊毛利率';
COMMENT ON COLUMN ads_dev_pricing_normal.amazon_net_profit IS '亚马逊净利润';
COMMENT ON COLUMN ads_dev_pricing_normal.amazon_net_profit_rate IS '亚马逊净利润率';
COMMENT ON COLUMN ads_dev_pricing_normal.company_gross_profit IS '公司毛利(EUR)';
COMMENT ON COLUMN ads_dev_pricing_normal.company_gross_profit_rate IS '公司毛利率(%)';
COMMENT ON COLUMN ads_dev_pricing_normal.sc_profit IS 'SC利润';
COMMENT ON COLUMN ads_dev_pricing_normal.supply_price IS '供货价（欧元）';
COMMENT ON COLUMN ads_dev_pricing_normal.purchase_price IS '采购价(RMB)';
COMMENT ON COLUMN ads_dev_pricing_normal.purchase_ratio IS '采购占比（27%）';
COMMENT ON COLUMN ads_dev_pricing_normal.first_leg_cost IS '头程(RMB)';
COMMENT ON COLUMN ads_dev_pricing_normal.transfer_fee IS '转运费(EUR)';
COMMENT ON COLUMN ads_dev_pricing_normal.logistics_ratio IS '物流占比（15%）';
COMMENT ON COLUMN ads_dev_pricing_normal.avs_commission IS 'AVS佣金(EUR)';
COMMENT ON COLUMN ads_dev_pricing_normal.advertising_fee IS '广告推广费(EUR)';
COMMENT ON COLUMN ads_dev_pricing_normal.eur_exchange_rate IS '欧元汇率';
COMMENT ON COLUMN ads_dev_pricing_normal.packing_quantity IS '装箱数';
COMMENT ON COLUMN ads_dev_pricing_normal.weight_max IS '重量 MAX（实重或体积重）';
COMMENT ON COLUMN ads_dev_pricing_normal.freight_unit_price IS '运费单价(RMB)';
COMMENT ON COLUMN ads_dev_pricing_normal.transfer_fee_eur IS '转运费（EUR）';

-- 为产品开发定价表 - 活动型添加列注释
COMMENT ON COLUMN ads_dev_pricing_promotion.product_name IS '产品名称';
COMMENT ON COLUMN ads_dev_pricing_promotion.competitor_price IS '竞品价格';
COMMENT ON COLUMN ads_dev_pricing_promotion.destination_country IS '目的国';
COMMENT ON COLUMN ads_dev_pricing_promotion.transport_method IS '运输方式';
COMMENT ON COLUMN ads_dev_pricing_promotion.actual_price IS '实际价格（前端）';
COMMENT ON COLUMN ads_dev_pricing_promotion.deal_discount IS 'Deal 折扣（BD 85折）';
COMMENT ON COLUMN ads_dev_pricing_promotion.discounted_price IS '折后价（前端）';
COMMENT ON COLUMN ads_dev_pricing_promotion.discounted_price_without_tax IS '折后不含税价（前端）';
COMMENT ON COLUMN ads_dev_pricing_promotion.amazon_fba IS '亚马逊FBA';
COMMENT ON COLUMN ads_dev_pricing_promotion.amazon_commission IS '亚马逊佣金';
COMMENT ON COLUMN ads_dev_pricing_promotion.amazon_gross_profit IS '亚马逊毛利';
COMMENT ON COLUMN ads_dev_pricing_promotion.amazon_gross_profit_rate IS '亚马逊毛利率';
COMMENT ON COLUMN ads_dev_pricing_promotion.amazon_net_profit IS '亚马逊净利润';
COMMENT ON COLUMN ads_dev_pricing_promotion.amazon_net_profit_rate IS '亚马逊净利润率';
COMMENT ON COLUMN ads_dev_pricing_promotion.company_gross_profit IS '公司毛利(EUR)';
COMMENT ON COLUMN ads_dev_pricing_promotion.company_gross_profit_rate IS '公司毛利率(%)';
COMMENT ON COLUMN ads_dev_pricing_promotion.supply_price IS '供货价（欧元）';
COMMENT ON COLUMN ads_dev_pricing_promotion.purchase_price IS '采购价(RMB)';
COMMENT ON COLUMN ads_dev_pricing_promotion.first_leg_cost IS '头程(RMB)';
COMMENT ON COLUMN ads_dev_pricing_promotion.transfer_fee IS '转运费(EUR)';
COMMENT ON COLUMN ads_dev_pricing_promotion.avs_commission IS 'AVS佣金(EUR)';
COMMENT ON COLUMN ads_dev_pricing_promotion.advertising_fee IS '广告推广费(EUR)';
COMMENT ON COLUMN ads_dev_pricing_promotion.eur_exchange_rate IS '欧元汇率';
COMMENT ON COLUMN ads_dev_pricing_promotion.packing_quantity IS '装箱数';
COMMENT ON COLUMN ads_dev_pricing_promotion.weight IS '重量';
COMMENT ON COLUMN ads_dev_pricing_promotion.freight_unit_price IS '运费单价';
COMMENT ON COLUMN ads_dev_pricing_promotion.transfer_fee_eur IS '转运费（EUR）';

-- 为每日产品信息表添加列注释
COMMENT ON COLUMN ads_purchase_daily_product_info.sku IS 'SKU';
COMMENT ON COLUMN ads_purchase_daily_product_info.product_name IS '品名';
COMMENT ON COLUMN ads_purchase_daily_product_info.outer_box_length IS '外箱规格长';
COMMENT ON COLUMN ads_purchase_daily_product_info.outer_box_width IS '外箱规格宽';
COMMENT ON COLUMN ads_purchase_daily_product_info.outer_box_height IS '外箱规格高';
COMMENT ON COLUMN ads_purchase_daily_product_info.outer_box_unit IS '外箱规格单位';
COMMENT ON COLUMN ads_purchase_daily_product_info.quantity_per_box IS '单箱数量(pcs)';
COMMENT ON COLUMN ads_purchase_daily_product_info.weight_per_box IS '单箱重量';
COMMENT ON COLUMN ads_purchase_daily_product_info.weight_unit IS '单箱重量单位';

-- 为含税账号统计表添加列注释
COMMENT ON COLUMN ads_purchase_tax_account_statistics.sku IS 'SKU';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.product_name IS '产品名称';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.supplier IS '供应商';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.account_name IS '户名';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.account_number IS '账号';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.bank IS '银行';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.opening_bank IS '开户行';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.invoice_name IS '发票名称';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.unit_specification IS '单位规格';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.invoice_source IS '发票源地';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.invoice_code IS '开票编码';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.customs_code_supplier IS '海关编码【供应商提供】';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.customs_code_recent IS '海关编码【近期使用】';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.invoice_tax_rate IS '发票税点';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.tax_deduction_note IS '备注（扣的税点）';
COMMENT ON COLUMN ads_purchase_tax_account_statistics.remark IS '备注';

-- 为新品时间表添加列注释
COMMENT ON COLUMN ads_purchase_new_product_timeline.sku IS 'SKU';
COMMENT ON COLUMN ads_purchase_new_product_timeline.product_name IS '品名';
COMMENT ON COLUMN ads_purchase_new_product_timeline.developer IS '开发人';
COMMENT ON COLUMN ads_purchase_new_product_timeline.operator IS '运营';
COMMENT ON COLUMN ads_purchase_new_product_timeline.create_time IS '创建时间';
COMMENT ON COLUMN ads_purchase_new_product_timeline.order_time IS '下单时间';
COMMENT ON COLUMN ads_purchase_new_product_timeline.arrival_shenzhen_time IS '到深仓时间';
COMMENT ON COLUMN ads_purchase_new_product_timeline.shenzhen_departure_time IS '深圳发出时间';
COMMENT ON COLUMN ads_purchase_new_product_timeline.logistics_method IS '物流方式';
COMMENT ON COLUMN ads_purchase_new_product_timeline.arrival_warehouse_date IS '到库日期';

-- 为产品采购汇总表添加列注释
COMMENT ON COLUMN ads_purchase_summary.order_number IS '订单编号';
COMMENT ON COLUMN ads_purchase_summary.supplier IS '供应商';
COMMENT ON COLUMN ads_purchase_summary.order_date IS '下单日期';
COMMENT ON COLUMN ads_purchase_summary.product_name IS '品名';
COMMENT ON COLUMN ads_purchase_summary.sku IS 'SKU';
COMMENT ON COLUMN ads_purchase_summary.unit_price IS '单价';
COMMENT ON COLUMN ads_purchase_summary.purchase_quantity IS '采购量';
COMMENT ON COLUMN ads_purchase_summary.freight IS '运费';
COMMENT ON COLUMN ads_purchase_summary.tax_amount IS '税额';
COMMENT ON COLUMN ads_purchase_summary.purchase_amount IS '采购金额';
COMMENT ON COLUMN ads_purchase_summary.payment_method IS '付款方式';
COMMENT ON COLUMN ads_purchase_summary.order_type IS '订单类型';
COMMENT ON COLUMN ads_purchase_summary.system_po_number IS '系统PO号';
COMMENT ON COLUMN ads_purchase_summary.document_type IS '单据类型';
COMMENT ON COLUMN ads_purchase_summary.payment_period_days IS '账期天数';
COMMENT ON COLUMN ads_purchase_summary.last_arrival_date IS '最后到货日';
COMMENT ON COLUMN ads_purchase_summary.due_payment_date IS '到期付款日';
COMMENT ON COLUMN ads_purchase_summary.payment_amount1 IS '付款金额1';
COMMENT ON COLUMN ads_purchase_summary.payment_time1 IS '付款时间1';
COMMENT ON COLUMN ads_purchase_summary.payment_amount2 IS '付款金额2';
COMMENT ON COLUMN ads_purchase_summary.payment_time2 IS '付款时间2';
COMMENT ON COLUMN ads_purchase_summary.discount_amount IS '折扣金额';
COMMENT ON COLUMN ads_purchase_summary.paid_amount IS '已付金额';
COMMENT ON COLUMN ads_purchase_summary.refund_amount IS '退款金额';
COMMENT ON COLUMN ads_purchase_summary.unpaid_amount IS '未付金额';
COMMENT ON COLUMN ads_purchase_summary.other_remark IS '其他备注';
COMMENT ON COLUMN ads_purchase_summary.order_number2 IS '订单编号2';
COMMENT ON COLUMN ads_purchase_summary.xt_request_payment IS 'xt是否请款';
COMMENT ON COLUMN ads_purchase_summary.jw_price_without_tax IS 'jw不含税单价';
COMMENT ON COLUMN ads_purchase_summary.total_unit_price IS '总单价';

-- 为样品采购汇总表添加列注释
COMMENT ON COLUMN ads_purchase_sample_summary.order_number IS '订单编号';
COMMENT ON COLUMN ads_purchase_sample_summary.supplier IS '供应商';
COMMENT ON COLUMN ads_purchase_sample_summary.order_time IS '下单时间';
COMMENT ON COLUMN ads_purchase_sample_summary.product_name IS '品名';
COMMENT ON COLUMN ads_purchase_sample_summary.unit_price IS '单价';
COMMENT ON COLUMN ads_purchase_sample_summary.purchase_quantity IS '采购量';
COMMENT ON COLUMN ads_purchase_sample_summary.freight IS '运费';
COMMENT ON COLUMN ads_purchase_sample_summary.purchase_amount IS '采购金额';
COMMENT ON COLUMN ads_purchase_sample_summary.payment_method IS '付款方式';
COMMENT ON COLUMN ads_purchase_sample_summary.is_refund IS '是否退款';
COMMENT ON COLUMN ads_purchase_sample_summary.refund_amount IS '退款金额';
COMMENT ON COLUMN ads_purchase_sample_summary.request_payment_time IS '请款时间';
COMMENT ON COLUMN ads_purchase_sample_summary.requested_amount IS '已请款金额';
COMMENT ON COLUMN ads_purchase_sample_summary.unrequested_amount IS '未请款金额';
COMMENT ON COLUMN ads_purchase_sample_summary.remark IS '备注';

-- 为付款信息表添加列注释
COMMENT ON COLUMN ads_purchase_payment_info.sku IS 'SKU';
COMMENT ON COLUMN ads_purchase_payment_info.product_name IS '产品名称';
COMMENT ON COLUMN ads_purchase_payment_info.supplier IS '供应商';
COMMENT ON COLUMN ads_purchase_payment_info.account_name IS '户名';
COMMENT ON COLUMN ads_purchase_payment_info.account_number IS '账号';
COMMENT ON COLUMN ads_purchase_payment_info.opening_bank IS '开户行';
COMMENT ON COLUMN ads_purchase_payment_info.invoice_name IS '发票名称';
COMMENT ON COLUMN ads_purchase_payment_info.unit_specification IS '单位规格';
COMMENT ON COLUMN ads_purchase_payment_info.invoice_source IS '发票源地';
COMMENT ON COLUMN ads_purchase_payment_info.invoice_code IS '开票编码';
COMMENT ON COLUMN ads_purchase_payment_info.customs_code_supplier IS '海关编码【供应商提供】';
COMMENT ON COLUMN ads_purchase_payment_info.customs_code_recent IS '海关编码【近期使用】';
COMMENT ON COLUMN ads_purchase_payment_info.invoice_tax_rate IS '发票税点';
COMMENT ON COLUMN ads_purchase_payment_info.tax_deduction_note IS '备注（扣的税点）';
COMMENT ON COLUMN ads_purchase_payment_info.remark IS '备注';
COMMENT ON COLUMN ads_purchase_payment_info.id_card_number IS '身份证号码';

-- 为采购订单到货追踪表添加列注释
COMMENT ON COLUMN ads_purchase_order_tracking.system_po_number IS '系统PO号';
COMMENT ON COLUMN ads_purchase_order_tracking.operator IS '运营';
COMMENT ON COLUMN ads_purchase_order_tracking.order_date IS '下单日期';
COMMENT ON COLUMN ads_purchase_order_tracking.product_name IS '品名';
COMMENT ON COLUMN ads_purchase_order_tracking.sku IS 'SKU';
COMMENT ON COLUMN ads_purchase_order_tracking.unit_price IS '单价';
COMMENT ON COLUMN ads_purchase_order_tracking.purchase_quantity IS '采购量';
COMMENT ON COLUMN ads_purchase_order_tracking.freight IS '运费';
COMMENT ON COLUMN ads_purchase_order_tracking.purchase_amount IS '采购金额';
COMMENT ON COLUMN ads_purchase_order_tracking.payment_method IS '付款方式';
COMMENT ON COLUMN ads_purchase_order_tracking.order_type IS '订单类型';
COMMENT ON COLUMN ads_purchase_order_tracking.quantity_per_box IS '单箱数量';
COMMENT ON COLUMN ads_purchase_order_tracking.box_count IS '箱数';
COMMENT ON COLUMN ads_purchase_order_tracking.expected_arrival_time_pmc IS '预计到货时间(PMC)';
COMMENT ON COLUMN ads_purchase_order_tracking.expected_arrival_time_supplier IS '预计到货时间(供应商）';
COMMENT ON COLUMN ads_purchase_order_tracking.receipt_time IS '签收时间';
COMMENT ON COLUMN ads_purchase_order_tracking.remark IS '备注';

-- 为帐期货款待付明细表添加列注释
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.order_number IS '订单编号';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.product_name IS '产品名称';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.purchase_quantity IS '采购数量';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.order_total_amount IS '订单总金额';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.payment_method IS '付款方式';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.payment_period_days IS '账期天数';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.last_arrival_date IS '最后到货日';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.settlement_date IS '结算日期';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.payable_amount IS '应付金额';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.supplier IS '供应商';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.po_number IS 'PO号';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.remark IS '备注';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.supplier_account_name IS '供应商-收款户名';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.supplier_opening_bank IS '供应商-开户银行';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.supplier_account_number IS '供应商-收款账号';
COMMENT ON COLUMN ads_purchase_account_period_payment_detail.payer IS '付款方';

-- 为汇率信息表添加列注释
COMMENT ON COLUMN ads_ops_currency_exchange_rate.currency IS '货币名称（如：欧元、英镑）';
COMMENT ON COLUMN ads_ops_currency_exchange_rate.exchange_rate IS '汇率';
COMMENT ON COLUMN ads_ops_currency_exchange_rate.time IS '时间（如：2025年12月）';

-- 为VAT计算参考表添加列注释
COMMENT ON COLUMN ads_ops_vat_calculation_reference.site IS '站点（如：DE、FR、IT、ES、UK、NL）';
COMMENT ON COLUMN ads_ops_vat_calculation_reference.vat_ratio IS '【增值税】VAT占比（如：15.96%）';
COMMENT ON COLUMN ads_ops_vat_calculation_reference.price_after_tax IS '税后价（如：84.04%）';

-- 为账号佣金表添加列注释
COMMENT ON COLUMN ads_ops_account_commission.account_name IS '账号（如：Dhohoo、AKASO）';
COMMENT ON COLUMN ads_ops_account_commission.commission IS '佣金（如：16.80%）';

-- 为COGS普通型定价表添加列注释
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.update_time IS '更新时间';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.store_number_po_type IS '店铺编号-PO类型';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.operator IS '运营';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.asin IS 'ASIN';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.upc IS 'UPC';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.product_name IS '产品名称';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.destination_country IS '目的国';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.selling_price IS '售价（前端）';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.price_without_tax IS '不含税售价（前端）';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.amazon_fba_shipping IS '亚马逊FBA运费';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.amazon_commission IS '亚马逊佣金';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.amazon_gross_profit IS '亚马逊毛利';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.amazon_gross_profit_rate IS '亚马逊毛利率';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.amazon_net_profit IS '亚马逊净利润';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.amazon_net_profit_rate IS '亚马逊净利润率';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.company_gross_profit IS '公司毛利(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.company_gross_profit_rate IS '公司毛利率(%)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.sc_profit IS 'SC利润';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.supply_price IS '供货价（欧元）';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.purchase_price IS '采购价(RMB)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.purchase_ratio IS '采购占比';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.first_leg_cost IS '头程(RMB)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.transfer_fee IS '转运费(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.logistics_ratio IS '物流占比';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.avs_commission IS 'AVS佣金(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.advertising_fee IS '广告推广费(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.eur_exchange_rate IS '欧元汇率';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.packing_quantity IS '装箱数';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.weight_max IS '重量 MAX（实重或体积重）';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.freight_unit_price IS '运费单价(RMB/kg)';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.transfer_fee_per_box IS '转运费（EUR/箱）';
COMMENT ON COLUMN ads_ops_cogs_pricing_normal.financial_verification IS '财务核对';

-- 为COGS活动型定价表添加列注释
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.update_time IS '更新时间';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.store_number_po_type IS '店铺编号-PO类型';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.operator IS '运营';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.asin IS 'ASIN';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.upc IS 'UPC';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.product_name IS '产品名称';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.destination_country IS '目的国';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.suggested_price IS '建议售价（前端）';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.deal_discount IS 'Deal 折扣（BD 85折）';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.discounted_price IS '折后价（前端）';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.discounted_price_without_tax IS '折后不含税价（前端）';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.amazon_fba IS '亚马逊FBA';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.commission IS '佣金';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.amazon_gross_profit IS '亚马逊毛利';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.amazon_gross_profit_rate IS '亚马逊毛利率';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.amazon_net_profit IS '亚马逊净利润';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.amazon_net_profit_rate IS '亚马逊净利润率';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.company_gross_profit IS '公司毛利(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.company_gross_profit_rate IS '公司毛利率(%)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.supply_price IS '供货价（欧元）';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.purchase_price IS '采购价(RMB)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.first_leg_cost IS '头程(RMB)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.transfer_fee_per_unit IS '单件转运费(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.avs_commission IS 'AVS佣金(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.advertising_fee IS '广告推广费(EUR)';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.eur_exchange_rate IS '欧元汇率';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.packing_quantity IS '装箱数';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.weight_kg IS '重量kg';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.freight_unit_price IS '运费单价';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.transfer_fee IS '转运费（EUR）';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.transfer_fee_verification_by_site IS '转运费（EUR）财务核对分站点';
COMMENT ON COLUMN ads_ops_cogs_pricing_promotion.transfer_fee_verification_all_sites IS '转运费（EUR）财务核对不分站点';

-- 为EU-SC店铺定价表添加列注释
COMMENT ON COLUMN ads_ops_eu_sc_pricing.update_time IS '更新时间';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.store IS '店铺';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.asin IS 'ASIN';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.upc IS 'UPC';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.product_name IS '产品名称';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.destination_country IS '目的国';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.frontend_price IS '前台售价';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.amazon_commission IS '亚马逊佣金';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.vat_tax IS 'VAT税金';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.amazon_fba IS '亚马逊FBA';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.purchase_price IS '采购价';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.first_leg_cost IS '头程';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.advertising_fee IS '广告推广费';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.gross_profit_amount IS '毛利额(RMB)';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.gross_profit_rate IS '毛利率';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.eur_exchange_rate IS '欧元汇率';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.packing_quantity IS '装箱数';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.weight_max_kg IS '重量 MAX kg（实重或体积重）';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.purchase_unit_price IS '采购单价(RMB)';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.first_leg_unit_price IS '头程单价(RMB)';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.freight_unit_price IS '运费单价(RMB/kg)';
COMMENT ON COLUMN ads_ops_eu_sc_pricing.transfer_unit_price IS '中转单价(RMB)';

-- 为US-SC店铺定价表添加列注释
COMMENT ON COLUMN ads_ops_us_sc_pricing.update_time IS '更新时间';
COMMENT ON COLUMN ads_ops_us_sc_pricing.store IS '店铺';
COMMENT ON COLUMN ads_ops_us_sc_pricing.asin IS 'ASIN';
COMMENT ON COLUMN ads_ops_us_sc_pricing.upc IS 'UPC';
COMMENT ON COLUMN ads_ops_us_sc_pricing.product_name IS '产品名称';
COMMENT ON COLUMN ads_ops_us_sc_pricing.destination_country IS '目的国';
COMMENT ON COLUMN ads_ops_us_sc_pricing.frontend_price IS '前台售价';
COMMENT ON COLUMN ads_ops_us_sc_pricing.amazon_commission IS '亚马逊佣金';
COMMENT ON COLUMN ads_ops_us_sc_pricing.vat_tax IS 'VAT税金';
COMMENT ON COLUMN ads_ops_us_sc_pricing.amazon_fba IS '亚马逊FBA';
COMMENT ON COLUMN ads_ops_us_sc_pricing.purchase_price IS '采购价';
COMMENT ON COLUMN ads_ops_us_sc_pricing.first_leg_cost IS '头程';
COMMENT ON COLUMN ads_ops_us_sc_pricing.advertising_fee IS '广告推广费';
COMMENT ON COLUMN ads_ops_us_sc_pricing.gross_profit_amount IS '毛利额(RMB)';
COMMENT ON COLUMN ads_ops_us_sc_pricing.gross_profit_rate IS '毛利率';
COMMENT ON COLUMN ads_ops_us_sc_pricing.usd_exchange_rate IS '美金汇率';
COMMENT ON COLUMN ads_ops_us_sc_pricing.packing_quantity IS '装箱数';
COMMENT ON COLUMN ads_ops_us_sc_pricing.weight_max_kg IS '重量 MAX kg（实重或体积重）';
COMMENT ON COLUMN ads_ops_us_sc_pricing.purchase_unit_price IS '采购单价(RMB)';
COMMENT ON COLUMN ads_ops_us_sc_pricing.first_leg_unit_price IS '头程单价(RMB)';
COMMENT ON COLUMN ads_ops_us_sc_pricing.freight_unit_price IS '运费单价(RMB/kg)';
COMMENT ON COLUMN ads_ops_us_sc_pricing.transfer_unit_price IS '中转单价(RMB)';

-- 为接单价表添加列注释
COMMENT ON COLUMN ads_ops_received_price.month IS '月份';
COMMENT ON COLUMN ads_ops_received_price.product_name IS '产品名称';
COMMENT ON COLUMN ads_ops_received_price.operator IS '运营';
COMMENT ON COLUMN ads_ops_received_price.asin IS 'ASIN';
COMMENT ON COLUMN ads_ops_received_price.country IS '国家';
COMMENT ON COLUMN ads_ops_received_price.received_price IS '接单价（€）';
COMMENT ON COLUMN ads_ops_received_price.operator_confirmation IS '运营确认记录';
COMMENT ON COLUMN ads_ops_received_price.update IS '更新';

-- 为产品规格及箱规表添加列注释
COMMENT ON COLUMN ads_ops_product_specification.month IS '月份';
COMMENT ON COLUMN ads_ops_product_specification.asin IS 'ASIN';
COMMENT ON COLUMN ads_ops_product_specification.upc IS 'UPC';
COMMENT ON COLUMN ads_ops_product_specification.product IS '产品';
COMMENT ON COLUMN ads_ops_product_specification.responsible_person IS '负责人';
COMMENT ON COLUMN ads_ops_product_specification.packaging_weight IS '包装重量（g）';
COMMENT ON COLUMN ads_ops_product_specification.product_length IS '产品尺寸-长（CM）';
COMMENT ON COLUMN ads_ops_product_specification.product_width IS '产品尺寸-宽（CM）';
COMMENT ON COLUMN ads_ops_product_specification.product_height IS '产品尺寸-高（CM）';
COMMENT ON COLUMN ads_ops_product_specification.box_specification IS '箱规';
COMMENT ON COLUMN ads_ops_product_specification.packing_quantity IS '装箱(PCS)';

