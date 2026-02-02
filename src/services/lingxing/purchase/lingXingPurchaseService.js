import prisma from '../../../config/database.js';
import LingXingApiClient from '../lingxingApiClient.js';

/**
 * 领星ERP采购服务
 * 供应商相关接口
 */
class LingXingPurchaseService extends LingXingApiClient {
  constructor() {
    super();
  }

  /**
   * 查询供应商列表
   * API: POST /erp/sc/data/local_inventory/supplier
   * 支持查询【采购】>【供应商】中的供应商信息
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0
   *   - length: 分页长度，默认1000
   * @returns {Promise<Object>} 供应商列表数据 { data: [], total: 0 }
   */
  async getSupplierList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {
        offset: params.offset || 0,
        length: params.length || 1000
      };

      // 调用API获取供应商列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/data/local_inventory/supplier', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取供应商列表失败');
      }

      const suppliers = response.data || [];
      const total = response.total || 0;

      // 保存供应商列表到数据库
      if (suppliers.length > 0) {
        await this.saveSuppliers(accountId, suppliers);
      }

      return {
        data: suppliers,
        total: total
      };
    } catch (error) {
      console.error('获取供应商列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存供应商列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} suppliers - 供应商列表数据
   */
  async saveSuppliers(accountId, suppliers) {
    try {
      for (const supplier of suppliers) {
        if (!supplier.supplier_id) {
          continue;
        }

        await prisma.lingXingSupplier.upsert({
          where: {
            supplierId: supplier.supplier_id
          },
          update: {
            accountId: accountId,
            supplierName: supplier.supplier_name || null,
            supplierCode: supplier.supplier_code || null,
            employees: supplier.employees !== undefined && supplier.employees !== null ? parseInt(supplier.employees) : null,
            url: supplier.url || null,
            contactPerson: supplier.contact_person || null,
            contactNumber: supplier.contact_number || null,
            qq: supplier.qq || null,
            email: supplier.email || null,
            fax: supplier.fax || null,
            accountName: supplier.account_name || null,
            openBank: supplier.open_bank || null,
            bankCardNumber: supplier.bank_card_number || null,
            remark: supplier.remark || null,
            purchaser: supplier.purchaser || null,
            isDelete: supplier.is_delete !== undefined && supplier.is_delete !== null ? parseInt(supplier.is_delete) : null,
            addressFull: supplier.address_full || null,
            paymentMethodText: supplier.payment_method_text || null,
            pcName: supplier.pc_name || null,
            settlementMethodText: supplier.settlement_method_text || null,
            settlementDescription: supplier.settlement_description || null,
            employeesText: supplier.employees_text || null,
            levelText: supplier.level_text || null,
            creditCode: supplier.credit_code || null,
            prepayPercent: supplier.prepay_percent || null,
            paymentAccountGroup: supplier.payment_account_group || null,
            periodConfigKey: supplier.period_config_key || null,
            periodConfigText: supplier.period_config_text || null,
            wid: supplier.wid !== undefined && supplier.wid !== null ? parseInt(supplier.wid) : null,
            wName: supplier.w_name || null,
            templateId: supplier.template_id || null,
            templateName: supplier.template_name || null,
            purchaserId: supplier.purchaser_id !== undefined && supplier.purchaser_id !== null ? parseInt(supplier.purchaser_id) : null,
            purchaserIdText: supplier.purchaser_id_text || null,
            receiptWid: supplier.receipt_wid !== undefined && supplier.receipt_wid !== null ? parseInt(supplier.receipt_wid) : null,
            receiptWidText: supplier.receipt_wid_text || null,
            supplierData: supplier, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            supplierId: supplier.supplier_id,
            accountId: accountId,
            supplierName: supplier.supplier_name || null,
            supplierCode: supplier.supplier_code || null,
            employees: supplier.employees !== undefined && supplier.employees !== null ? parseInt(supplier.employees) : null,
            url: supplier.url || null,
            contactPerson: supplier.contact_person || null,
            contactNumber: supplier.contact_number || null,
            qq: supplier.qq || null,
            email: supplier.email || null,
            fax: supplier.fax || null,
            accountName: supplier.account_name || null,
            openBank: supplier.open_bank || null,
            bankCardNumber: supplier.bank_card_number || null,
            remark: supplier.remark || null,
            purchaser: supplier.purchaser || null,
            isDelete: supplier.is_delete !== undefined && supplier.is_delete !== null ? parseInt(supplier.is_delete) : null,
            addressFull: supplier.address_full || null,
            paymentMethodText: supplier.payment_method_text || null,
            pcName: supplier.pc_name || null,
            settlementMethodText: supplier.settlement_method_text || null,
            settlementDescription: supplier.settlement_description || null,
            employeesText: supplier.employees_text || null,
            levelText: supplier.level_text || null,
            creditCode: supplier.credit_code || null,
            prepayPercent: supplier.prepay_percent || null,
            paymentAccountGroup: supplier.payment_account_group || null,
            periodConfigKey: supplier.period_config_key || null,
            periodConfigText: supplier.period_config_text || null,
            wid: supplier.wid !== undefined && supplier.wid !== null ? parseInt(supplier.wid) : null,
            wName: supplier.w_name || null,
            templateId: supplier.template_id || null,
            templateName: supplier.template_name || null,
            purchaserId: supplier.purchaser_id !== undefined && supplier.purchaser_id !== null ? parseInt(supplier.purchaser_id) : null,
            purchaserIdText: supplier.purchaser_id_text || null,
            receiptWid: supplier.receipt_wid !== undefined && supplier.receipt_wid !== null ? parseInt(supplier.receipt_wid) : null,
            receiptWidText: supplier.receipt_wid_text || null,
            supplierData: supplier // 保存完整数据
          }
        });
      }

      console.log(`供应商列表已保存到数据库: 共 ${suppliers.length} 个供应商`);
    } catch (error) {
      console.error('保存供应商列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 查询采购方列表
   * API: POST /erp/sc/routing/data/purchaser/lists
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - offset: 分页偏移量，默认0
   *   - length: 分页长度，默认500
   * @returns {Promise<Object>} 采购方列表数据 { data: [], total: 0 }
   */
  async getPurchaserList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数
      const requestParams = {
        offset: params.offset || 0,
        length: params.length || 500
      };

      // 调用API获取采购方列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/purchaser/lists', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取采购方列表失败');
      }

      // 注意：这个接口返回的数据结构是 { data: { total: 64, list: [...] } }
      const responseData = response.data || {};
      const purchasers = responseData.list || [];
      const total = responseData.total || response.total || 0;

      // 保存采购方列表到数据库
      if (purchasers.length > 0) {
        await this.savePurchasers(accountId, purchasers);
      }

      return {
        data: purchasers,
        total: total
      };
    } catch (error) {
      console.error('获取采购方列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存采购方列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} purchasers - 采购方列表数据
   */
  async savePurchasers(accountId, purchasers) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingPurchaser) {
        console.error('Prisma Client 中未找到 lingXingPurchaser 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const purchaser of purchasers) {
        if (!purchaser.purchaser_id) {
          continue;
        }

        await prisma.lingXingPurchaser.upsert({
          where: {
            purchaserId: purchaser.purchaser_id
          },
          update: {
            accountId: accountId,
            name: purchaser.name || null,
            address: purchaser.address || null,
            contactPhone: purchaser.contact_phone || null,
            contacter: purchaser.contacter || null,
            email: purchaser.email || null,
            purchaserData: purchaser, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            purchaserId: purchaser.purchaser_id,
            accountId: accountId,
            name: purchaser.name || null,
            address: purchaser.address || null,
            contactPhone: purchaser.contact_phone || null,
            contacter: purchaser.contacter || null,
            email: purchaser.email || null,
            purchaserData: purchaser // 保存完整数据
          }
        });
      }

      console.log(`采购方列表已保存到数据库: 共 ${purchasers.length} 个采购方`);
    } catch (error) {
      console.error('保存采购方列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 查询采购计划列表
   * API: POST /erp/sc/routing/data/local_inventory/getPurchasePlans
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - search_field_time: 时间搜索维度（creator_time/expect_arrive_time/update_time）（必填）
   *   - start_date: 开始日期，Y-m-d 或 Y-m-d H:i:s（必填）
   *   - end_date: 结束日期，Y-m-d 或 Y-m-d H:i:s（必填）
   *   - plan_sns: 采购计划编号数组（可选）
   *   - is_combo: 是否为组合商品：0 否，1 是（可选）
   *   - is_related_process_plan: 是否关联加工计划，0：否，1：是（可选）
   *   - status: 状态数组（可选）
   *   - sids: 店铺id数组（可选）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认500，上限500（可选）
   * @returns {Promise<Object>} 采购计划列表数据 { data: [], total: 0 }
   */
  async getPurchasePlanList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.search_field_time || !params.start_date || !params.end_date) {
        throw new Error('search_field_time、start_date 和 end_date 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        search_field_time: params.search_field_time,
        start_date: params.start_date,
        end_date: params.end_date,
        ...(params.plan_sns && params.plan_sns.length > 0 && { plan_sns: params.plan_sns }),
        ...(params.is_combo !== undefined && { is_combo: params.is_combo }),
        ...(params.is_related_process_plan !== undefined && { is_related_process_plan: params.is_related_process_plan }),
        ...(params.status && params.status.length > 0 && { status: params.status }),
        ...(params.sids && params.sids.length > 0 && { sids: params.sids }),
        ...(params.offset !== undefined && { offset: params.offset }),
        ...(params.length !== undefined && { length: params.length })
      };

      // 调用API获取采购计划列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/getPurchasePlans', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取采购计划列表失败');
      }

      const plans = response.data || [];
      const total = response.total || 0;

      // 保存采购计划列表到数据库
      if (plans.length > 0) {
        await this.savePurchasePlans(accountId, plans);
      }

      return {
        data: plans,
        total: total
      };
    } catch (error) {
      console.error('获取采购计划列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存采购计划列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} plans - 采购计划列表数据
   */
  async savePurchasePlans(accountId, plans) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingPurchasePlan) {
        console.error('Prisma Client 中未找到 lingXingPurchasePlan 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const plan of plans) {
        if (!plan.plan_sn) {
          continue;
        }

        await prisma.lingXingPurchasePlan.upsert({
          where: {
            planSn: plan.plan_sn
          },
          update: {
            accountId: accountId,
            ppgSn: plan.ppg_sn || null,
            statusText: plan.status_text || null,
            status: plan.status !== undefined && plan.status !== null ? parseInt(plan.status) : null,
            creatorRealName: plan.creator_real_name || null,
            creatorUid: plan.creator_uid !== undefined && plan.creator_uid !== null ? parseInt(plan.creator_uid) : null,
            createTime: plan.create_time || null,
            file: plan.file || null,
            planRemark: plan.plan_remark || null,
            picUrl: plan.pic_url || null,
            spuName: plan.spu_name || null,
            spu: plan.spu || null,
            productName: plan.product_name || null,
            productId: plan.product_id !== undefined && plan.product_id !== null ? BigInt(plan.product_id) : null,
            sku: plan.sku || null,
            attribute: plan.attribute || null,
            sid: plan.sid !== undefined && plan.sid !== null ? parseFloat(plan.sid) : null,
            sellerName: plan.seller_name || null,
            marketplace: plan.marketplace || null,
            fnsku: plan.fnsku || null,
            msku: plan.msku || null,
            supplierId: plan.supplier_id !== undefined && plan.supplier_id !== null ? (typeof plan.supplier_id === 'object' ? plan.supplier_id : parseInt(plan.supplier_id)) : null,
            supplierName: plan.supplier_name || null,
            wid: plan.wid !== undefined && plan.wid !== null ? BigInt(plan.wid) : null,
            warehouseName: plan.warehouse_name || null,
            purchaserId: plan.purchaser_id !== undefined && plan.purchaser_id !== null ? BigInt(plan.purchaser_id) : null,
            purchaserName: plan.purchaser_name || null,
            cgBoxPcs: plan.cg_box_pcs !== undefined && plan.cg_box_pcs !== null ? parseInt(plan.cg_box_pcs) : null,
            quantityPlan: plan.quantity_plan !== undefined && plan.quantity_plan !== null ? parseInt(plan.quantity_plan) : null,
            expectArriveTime: plan.expect_arrive_time || null,
            cgUid: plan.cg_uid !== undefined && plan.cg_uid !== null ? parseInt(plan.cg_uid) : null,
            cgOptUsername: plan.cg_opt_username || null,
            remark: plan.remark || null,
            isCombo: plan.is_combo !== undefined && plan.is_combo !== null ? parseInt(plan.is_combo) : null,
            isAux: plan.is_aux !== undefined && plan.is_aux !== null ? parseInt(plan.is_aux) : null,
            isRelatedProcessPlan: plan.is_related_process_plan !== undefined && plan.is_related_process_plan !== null ? parseInt(plan.is_related_process_plan) : null,
            groupId: plan.group_id !== undefined && plan.group_id !== null ? parseInt(plan.group_id) : null,
            planData: plan, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            planSn: plan.plan_sn,
            accountId: accountId,
            ppgSn: plan.ppg_sn || null,
            statusText: plan.status_text || null,
            status: plan.status !== undefined && plan.status !== null ? parseInt(plan.status) : null,
            creatorRealName: plan.creator_real_name || null,
            creatorUid: plan.creator_uid !== undefined && plan.creator_uid !== null ? parseInt(plan.creator_uid) : null,
            createTime: plan.create_time || null,
            file: plan.file || null,
            planRemark: plan.plan_remark || null,
            picUrl: plan.pic_url || null,
            spuName: plan.spu_name || null,
            spu: plan.spu || null,
            productName: plan.product_name || null,
            productId: plan.product_id !== undefined && plan.product_id !== null ? BigInt(plan.product_id) : null,
            sku: plan.sku || null,
            attribute: plan.attribute || null,
            sid: plan.sid !== undefined && plan.sid !== null ? parseFloat(plan.sid) : null,
            sellerName: plan.seller_name || null,
            marketplace: plan.marketplace || null,
            fnsku: plan.fnsku || null,
            msku: plan.msku || null,
            supplierId: plan.supplier_id !== undefined && plan.supplier_id !== null ? (typeof plan.supplier_id === 'object' ? plan.supplier_id : parseInt(plan.supplier_id)) : null,
            supplierName: plan.supplier_name || null,
            wid: plan.wid !== undefined && plan.wid !== null ? BigInt(plan.wid) : null,
            warehouseName: plan.warehouse_name || null,
            purchaserId: plan.purchaser_id !== undefined && plan.purchaser_id !== null ? BigInt(plan.purchaser_id) : null,
            purchaserName: plan.purchaser_name || null,
            cgBoxPcs: plan.cg_box_pcs !== undefined && plan.cg_box_pcs !== null ? parseInt(plan.cg_box_pcs) : null,
            quantityPlan: plan.quantity_plan !== undefined && plan.quantity_plan !== null ? parseInt(plan.quantity_plan) : null,
            expectArriveTime: plan.expect_arrive_time || null,
            cgUid: plan.cg_uid !== undefined && plan.cg_uid !== null ? parseInt(plan.cg_uid) : null,
            cgOptUsername: plan.cg_opt_username || null,
            remark: plan.remark || null,
            isCombo: plan.is_combo !== undefined && plan.is_combo !== null ? parseInt(plan.is_combo) : null,
            isAux: plan.is_aux !== undefined && plan.is_aux !== null ? parseInt(plan.is_aux) : null,
            isRelatedProcessPlan: plan.is_related_process_plan !== undefined && plan.is_related_process_plan !== null ? parseInt(plan.is_related_process_plan) : null,
            groupId: plan.group_id !== undefined && plan.group_id !== null ? parseInt(plan.group_id) : null,
            planData: plan // 保存完整数据
          }
        });
      }

      console.log(`采购计划列表已保存到数据库: 共 ${plans.length} 个采购计划`);
    } catch (error) {
      console.error('保存采购计划列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 查询采购单列表
   * API: POST /erp/sc/routing/data/local_inventory/purchaseOrderList
   * 支持查询【采购】>【采购单】数据
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - start_date: 开始时间，格式：Y-m-d 或 Y-m-d H:i:s（必填）
   *   - end_date: 结束时间，格式：Y-m-d 或 Y-m-d H:i:s（必填）
   *   - search_field_time: 时间搜索维度（可选，默认create_time）：create_time/expect_arrive_time/update_time
   *   - order_sn: 采购单号数组，上限500（可选）
   *   - custom_order_sn: 自定义采购单号数组，上限500（可选）
   *   - purchase_type: 采购类型，1：普通采购，2:1688采购（可选）
   *   - offset: 分页偏移量，默认0（可选）
   *   - length: 分页长度，默认500，上限500（可选）
   * @returns {Promise<Object>} 采购单列表数据 { data: [], total: 0 }
   */
  async getPurchaseOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (!params.start_date || !params.end_date) {
        throw new Error('start_date 和 end_date 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        start_date: params.start_date,
        end_date: params.end_date,
        search_field_time: params.search_field_time || 'create_time',
        ...(params.order_sn && params.order_sn.length > 0 && { order_sn: params.order_sn }),
        ...(params.custom_order_sn && params.custom_order_sn.length > 0 && { custom_order_sn: params.custom_order_sn }),
        ...(params.purchase_type !== undefined && { purchase_type: params.purchase_type }),
        ...(params.offset !== undefined && { offset: params.offset }),
        ...(params.length !== undefined && { length: params.length })
      };

      // 调用API获取采购单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/data/local_inventory/purchaseOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取采购单列表失败');
      }

      const orders = response.data || [];
      const total = response.total || 0;

      // 保存采购单列表到数据库
      if (orders.length > 0) {
        await this.savePurchaseOrders(accountId, orders);
      }

      return {
        data: orders,
        total: total
      };
    } catch (error) {
      console.error('获取采购单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存采购单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} orders - 采购单列表数据
   */
  async savePurchaseOrders(accountId, orders) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingPurchaseOrder) {
        console.error('Prisma Client 中未找到 lingXingPurchaseOrder 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const order of orders) {
        if (!order.order_sn) {
          continue;
        }

        // 保存采购单主表
        const purchaseOrder = await prisma.lingXingPurchaseOrder.upsert({
          where: {
            orderSn: order.order_sn
          },
          update: {
            accountId: accountId,
            customOrderSn: order.custom_order_sn || null,
            supplierId: order.supplier_id !== undefined && order.supplier_id !== null ? BigInt(order.supplier_id) : null,
            supplierName: order.supplier_name || null,
            optUid: order.opt_uid !== undefined && order.opt_uid !== null ? BigInt(order.opt_uid) : null,
            principalUids: order.principal_uids || null,
            auditorRealname: order.auditor_realname || null,
            optRealname: order.opt_realname || null,
            lastRealname: order.last_realname || null,
            createTime: order.create_time || null,
            orderTime: order.order_time || null,
            payment: order.payment !== undefined && order.payment !== null && order.payment !== '' ? parseFloat(order.payment) : null,
            auditorUid: order.auditor_uid !== undefined && order.auditor_uid !== null ? BigInt(order.auditor_uid) : null,
            auditorTime: order.auditor_time || null,
            lastUid: order.last_uid !== undefined && order.last_uid !== null ? BigInt(order.last_uid) : null,
            lastTime: order.last_time || null,
            reason: order.reason || null,
            isTax: order.is_tax !== undefined && order.is_tax !== null ? parseInt(order.is_tax) : null,
            status: order.status !== undefined && order.status !== null ? parseInt(order.status) : null,
            statusText: order.status_text || null,
            payStatusText: order.pay_status_text || null,
            statusShipped: order.status_shipped !== undefined && order.status_shipped !== null ? parseInt(order.status_shipped) : null,
            statusShippedText: order.status_shipped_text || null,
            amountTotal: order.amount_total !== undefined && order.amount_total !== null ? parseFloat(order.amount_total) : null,
            totalPrice: order.total_price !== undefined && order.total_price !== null ? parseFloat(order.total_price) : null,
            icon: order.icon || null,
            payStatus: order.pay_status !== undefined && order.pay_status !== null ? parseInt(order.pay_status) : null,
            remark: order.remark || null,
            otherFee: order.other_fee !== undefined && order.other_fee !== null ? parseFloat(order.other_fee) : null,
            otherCurrency: order.other_currency || null,
            feePartType: order.fee_part_type !== undefined && order.fee_part_type !== null ? parseInt(order.fee_part_type) : null,
            shippingPrice: order.shipping_price !== undefined && order.shipping_price !== null ? parseFloat(order.shipping_price) : null,
            shippingCurrency: order.shipping_currency || null,
            purchaseCurrency: order.purchase_currency || null,
            purchaseRate: order.purchase_rate !== undefined && order.purchase_rate !== null ? parseFloat(order.purchase_rate) : null,
            quantityTotal: order.quantity_total !== undefined && order.quantity_total !== null ? parseFloat(order.quantity_total) : null,
            wid: order.wid !== undefined && order.wid !== null ? BigInt(order.wid) : null,
            wareHouseName: order.ware_house_name || null,
            wareHouseBakName: order.ware_house_bak_name || null,
            quantityEntry: order.quantity_entry !== undefined && order.quantity_entry !== null ? parseInt(order.quantity_entry) : null,
            quantityReal: order.quantity_real !== undefined && order.quantity_real !== null ? parseInt(order.quantity_real) : null,
            quantityReceive: order.quantity_receive !== undefined && order.quantity_receive !== null ? parseInt(order.quantity_receive) : null,
            updateTime: order.update_time || null,
            purchaserId: order.purchaser_id !== undefined && order.purchaser_id !== null ? BigInt(order.purchaser_id) : null,
            contactPerson: order.contact_person || null,
            contactNumber: order.contact_number || null,
            settlementMethod: order.settlement_method !== undefined && order.settlement_method !== null ? parseInt(order.settlement_method) : null,
            settlementDescription: order.settlement_description || null,
            purchaseType: order.purchase_type !== undefined && order.purchase_type !== null ? String(order.purchase_type) : null,
            purchaseTypeText: order.purchase_type_text || null,
            alibabaOrderSn: order.alibaba_order_sn || null,
            subStatus: order.sub_status !== undefined && order.sub_status !== null ? String(order.sub_status) : null,
            subStatusText: order.sub_status_text || null,
            customFields: order.custom_fields || null,
            paymentMethod: order.payment_method !== undefined && order.payment_method !== null ? BigInt(order.payment_method) : null,
            logisticsInfo: order.logistics_info || null,
            orderData: order, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            orderSn: order.order_sn,
            accountId: accountId,
            customOrderSn: order.custom_order_sn || null,
            supplierId: order.supplier_id !== undefined && order.supplier_id !== null ? BigInt(order.supplier_id) : null,
            supplierName: order.supplier_name || null,
            optUid: order.opt_uid !== undefined && order.opt_uid !== null ? BigInt(order.opt_uid) : null,
            principalUids: order.principal_uids || null,
            auditorRealname: order.auditor_realname || null,
            optRealname: order.opt_realname || null,
            lastRealname: order.last_realname || null,
            createTime: order.create_time || null,
            orderTime: order.order_time || null,
            payment: order.payment !== undefined && order.payment !== null && order.payment !== '' ? parseFloat(order.payment) : null,
            auditorUid: order.auditor_uid !== undefined && order.auditor_uid !== null ? BigInt(order.auditor_uid) : null,
            auditorTime: order.auditor_time || null,
            lastUid: order.last_uid !== undefined && order.last_uid !== null ? BigInt(order.last_uid) : null,
            lastTime: order.last_time || null,
            reason: order.reason || null,
            isTax: order.is_tax !== undefined && order.is_tax !== null ? parseInt(order.is_tax) : null,
            status: order.status !== undefined && order.status !== null ? parseInt(order.status) : null,
            statusText: order.status_text || null,
            payStatusText: order.pay_status_text || null,
            statusShipped: order.status_shipped !== undefined && order.status_shipped !== null ? parseInt(order.status_shipped) : null,
            statusShippedText: order.status_shipped_text || null,
            amountTotal: order.amount_total !== undefined && order.amount_total !== null ? parseFloat(order.amount_total) : null,
            totalPrice: order.total_price !== undefined && order.total_price !== null ? parseFloat(order.total_price) : null,
            icon: order.icon || null,
            payStatus: order.pay_status !== undefined && order.pay_status !== null ? parseInt(order.pay_status) : null,
            remark: order.remark || null,
            otherFee: order.other_fee !== undefined && order.other_fee !== null ? parseFloat(order.other_fee) : null,
            otherCurrency: order.other_currency || null,
            feePartType: order.fee_part_type !== undefined && order.fee_part_type !== null ? parseInt(order.fee_part_type) : null,
            shippingPrice: order.shipping_price !== undefined && order.shipping_price !== null ? parseFloat(order.shipping_price) : null,
            shippingCurrency: order.shipping_currency || null,
            purchaseCurrency: order.purchase_currency || null,
            purchaseRate: order.purchase_rate !== undefined && order.purchase_rate !== null ? parseFloat(order.purchase_rate) : null,
            quantityTotal: order.quantity_total !== undefined && order.quantity_total !== null ? parseFloat(order.quantity_total) : null,
            wid: order.wid !== undefined && order.wid !== null ? BigInt(order.wid) : null,
            wareHouseName: order.ware_house_name || null,
            wareHouseBakName: order.ware_house_bak_name || null,
            quantityEntry: order.quantity_entry !== undefined && order.quantity_entry !== null ? parseInt(order.quantity_entry) : null,
            quantityReal: order.quantity_real !== undefined && order.quantity_real !== null ? parseInt(order.quantity_real) : null,
            quantityReceive: order.quantity_receive !== undefined && order.quantity_receive !== null ? parseInt(order.quantity_receive) : null,
            updateTime: order.update_time || null,
            purchaserId: order.purchaser_id !== undefined && order.purchaser_id !== null ? BigInt(order.purchaser_id) : null,
            contactPerson: order.contact_person || null,
            contactNumber: order.contact_number || null,
            settlementMethod: order.settlement_method !== undefined && order.settlement_method !== null ? parseInt(order.settlement_method) : null,
            settlementDescription: order.settlement_description || null,
            purchaseType: order.purchase_type !== undefined && order.purchase_type !== null ? String(order.purchase_type) : null,
            purchaseTypeText: order.purchase_type_text || null,
            alibabaOrderSn: order.alibaba_order_sn || null,
            subStatus: order.sub_status !== undefined && order.sub_status !== null ? String(order.sub_status) : null,
            subStatusText: order.sub_status_text || null,
            customFields: order.custom_fields || null,
            paymentMethod: order.payment_method !== undefined && order.payment_method !== null ? BigInt(order.payment_method) : null,
            logisticsInfo: order.logistics_info || null,
            orderData: order // 保存完整数据
          }
        });

        // 保存采购单项
        if (order.item_list && order.item_list.length > 0) {
          await this.savePurchaseOrderItems(purchaseOrder.id, order.item_list);
        }
      }

      console.log(`采购单列表已保存到数据库: 共 ${orders.length} 个采购单`);
    } catch (error) {
      console.error('保存采购单列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 保存采购单项到数据库
   * @param {string} orderId - 采购单ID
   * @param {Array} items - 采购单项数组
   */
  async savePurchaseOrderItems(orderId, items) {
    try {
      if (!prisma.lingXingPurchaseOrderItem) {
        console.error('Prisma Client 中未找到 lingXingPurchaseOrderItem 模型');
        return;
      }

      for (const item of items) {
        if (item.id === undefined || item.id === null) {
          continue;
        }

        // 将 itemId 转换为 BigInt
        const itemIdBigInt = item.id !== undefined && item.id !== null ? BigInt(item.id) : null;
        
        await prisma.lingXingPurchaseOrderItem.upsert({
          where: {
            itemId: itemIdBigInt
          },
          update: {
            orderId: orderId,
            wid: item.wid !== undefined && item.wid !== null ? BigInt(item.wid) : null,
            wareHouseName: item.ware_house_name || null,
            relationPurchasePlan: item.relation_purchase_plan || null,
            planSn: item.plan_sn || null,
            productId: item.product_id !== undefined && item.product_id !== null ? BigInt(item.product_id) : null,
            productName: item.product_name || null,
            sku: item.sku || null,
            fnsku: item.fnsku || null,
            sid: item.sid || null,
            model: item.model || null,
            price: item.price !== undefined && item.price !== null && item.price !== '' ? parseFloat(item.price) : null,
            amount: item.amount !== undefined && item.amount !== null && item.amount !== '' ? parseFloat(item.amount) : null,
            quantityPlan: item.quantity_plan !== undefined && item.quantity_plan !== null ? parseInt(item.quantity_plan) : null,
            quantityReal: item.quantity_real !== undefined && item.quantity_real !== null ? parseInt(item.quantity_real) : null,
            quantityEntry: item.quantity_entry !== undefined && item.quantity_entry !== null ? parseInt(item.quantity_entry) : null,
            quantityReceive: item.quantity_receive !== undefined && item.quantity_receive !== null ? parseInt(item.quantity_receive) : null,
            quantityReturn: item.quantity_return !== undefined && item.quantity_return !== null ? parseInt(item.quantity_return) : null,
            quantityExchange: item.quantity_exchange !== undefined && item.quantity_exchange !== null ? parseInt(item.quantity_exchange) : null,
            quantityQc: item.quantity_qc !== undefined && item.quantity_qc !== null ? parseInt(item.quantity_qc) : null,
            quantityQcPrepare: item.quantity_qc_prepare !== undefined && item.quantity_qc_prepare !== null ? parseInt(item.quantity_qc_prepare) : null,
            expectArriveTime: item.expect_arrive_time || null,
            remark: item.remark || null,
            casesNum: item.cases_num !== undefined && item.cases_num !== null ? parseInt(item.cases_num) : null,
            quantityPerCase: item.quantity_per_case !== undefined && item.quantity_per_case !== null ? parseInt(item.quantity_per_case) : null,
            isDelete: item.is_delete !== undefined && item.is_delete !== null ? parseInt(item.is_delete) : null,
            msku: item.msku || null,
            attribute: item.attribute || null,
            taxRate: item.tax_rate || null,
            spu: item.spu || null,
            spuName: item.spu_name || null,
            customFields: item.custom_fields || null,
            itemData: item, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            itemId: itemIdBigInt,
            orderId: orderId,
            wid: item.wid !== undefined && item.wid !== null ? BigInt(item.wid) : null,
            wareHouseName: item.ware_house_name || null,
            relationPurchasePlan: item.relation_purchase_plan || null,
            planSn: item.plan_sn || null,
            productId: item.product_id !== undefined && item.product_id !== null ? BigInt(item.product_id) : null,
            productName: item.product_name || null,
            sku: item.sku || null,
            fnsku: item.fnsku || null,
            sid: item.sid || null,
            model: item.model || null,
            price: item.price !== undefined && item.price !== null && item.price !== '' ? parseFloat(item.price) : null,
            amount: item.amount !== undefined && item.amount !== null && item.amount !== '' ? parseFloat(item.amount) : null,
            quantityPlan: item.quantity_plan !== undefined && item.quantity_plan !== null ? parseInt(item.quantity_plan) : null,
            quantityReal: item.quantity_real !== undefined && item.quantity_real !== null ? parseInt(item.quantity_real) : null,
            quantityEntry: item.quantity_entry !== undefined && item.quantity_entry !== null ? parseInt(item.quantity_entry) : null,
            quantityReceive: item.quantity_receive !== undefined && item.quantity_receive !== null ? parseInt(item.quantity_receive) : null,
            quantityReturn: item.quantity_return !== undefined && item.quantity_return !== null ? parseInt(item.quantity_return) : null,
            quantityExchange: item.quantity_exchange !== undefined && item.quantity_exchange !== null ? parseInt(item.quantity_exchange) : null,
            quantityQc: item.quantity_qc !== undefined && item.quantity_qc !== null ? parseInt(item.quantity_qc) : null,
            quantityQcPrepare: item.quantity_qc_prepare !== undefined && item.quantity_qc_prepare !== null ? parseInt(item.quantity_qc_prepare) : null,
            expectArriveTime: item.expect_arrive_time || null,
            remark: item.remark || null,
            casesNum: item.cases_num !== undefined && item.cases_num !== null ? parseInt(item.cases_num) : null,
            quantityPerCase: item.quantity_per_case !== undefined && item.quantity_per_case !== null ? parseInt(item.quantity_per_case) : null,
            isDelete: item.is_delete !== undefined && item.is_delete !== null ? parseInt(item.is_delete) : null,
            msku: item.msku || null,
            attribute: item.attribute || null,
            taxRate: item.tax_rate || null,
            spu: item.spu || null,
            spuName: item.spu_name || null,
            customFields: item.custom_fields || null,
            itemData: item // 保存完整数据
          }
        });
      }
    } catch (error) {
      console.error('保存采购单项到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 查询采购退货单列表
   * API: POST /erp/sc/routing/purchase/purchase_return_order/getPurchaseReturnOrderList
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - search_field_time: 时间搜索维度（可选，默认create_time）：create_time/last_time
   *   - start_date: 开始时间，格式：Y-m-d 或 Y-m-d H:i:s（可选）
   *   - end_date: 结束时间，格式：Y-m-d 或 Y-m-d H:i:s（可选）
   *   - status: 状态数组（可选）
   *   - offset: 分页偏移量（必填）
   *   - length: 分页长度，上限500（必填）
   * @returns {Promise<Object>} 采购退货单列表数据 { data: [], total: 0 }
   */
  async getPurchaseReturnOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.offset === undefined || params.length === undefined) {
        throw new Error('offset 和 length 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        offset: params.offset,
        length: params.length,
        ...(params.search_field_time && { search_field_time: params.search_field_time }),
        ...(params.start_date && { start_date: params.start_date }),
        ...(params.end_date && { end_date: params.end_date }),
        ...(params.status && params.status.length > 0 && { status: params.status })
      };

      // 调用API获取采购退货单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/purchase/purchase_return_order/getPurchaseReturnOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取采购退货单列表失败');
      }

      // 注意：这个接口返回的数据结构是 { data: { total: 2, list: [...] } }
      const responseData = response.data || {};
      const returnOrders = responseData.list || [];
      const total = responseData.total || response.total || 0;

      // 保存采购退货单列表到数据库
      if (returnOrders.length > 0) {
        await this.savePurchaseReturnOrders(accountId, returnOrders);
      }

      return {
        data: returnOrders,
        total: total
      };
    } catch (error) {
      console.error('获取采购退货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存采购退货单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} returnOrders - 采购退货单列表数据
   */
  async savePurchaseReturnOrders(accountId, returnOrders) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingPurchaseReturnOrder) {
        console.error('Prisma Client 中未找到 lingXingPurchaseReturnOrder 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const returnOrder of returnOrders) {
        if (!returnOrder.order_sn) {
          continue;
        }

        // 保存采购退货单主表
        const purchaseReturnOrder = await prisma.lingXingPurchaseReturnOrder.upsert({
          where: {
            orderSn: returnOrder.order_sn
          },
          update: {
            accountId: accountId,
            wid: returnOrder.wid !== undefined && returnOrder.wid !== null ? BigInt(returnOrder.wid) : null,
            createUid: returnOrder.create_uid !== undefined && returnOrder.create_uid !== null ? BigInt(returnOrder.create_uid) : null,
            createRealname: returnOrder.create_realname || null,
            createTime: returnOrder.create_time || null,
            lastTime: returnOrder.last_time || null,
            buyerUid: returnOrder.buyer_uid !== undefined && returnOrder.buyer_uid !== null ? BigInt(returnOrder.buyer_uid) : null,
            buyerRealname: returnOrder.buyer_realname || null,
            purchaseOrderSn: returnOrder.purchase_order_sn || null,
            supplierId: returnOrder.supplier_id !== undefined && returnOrder.supplier_id !== null ? BigInt(returnOrder.supplier_id) : null,
            supplierName: returnOrder.supplier_name || null,
            returnMethod: returnOrder.return_method !== undefined && returnOrder.return_method !== null ? parseInt(returnOrder.return_method) : null,
            replenishMethod: returnOrder.replenish_method !== undefined && returnOrder.replenish_method !== null ? parseInt(returnOrder.replenish_method) : null,
            receiptFundsOrderSn: returnOrder.receipt_funds_order_sn || null,
            status: returnOrder.status !== undefined && returnOrder.status !== null ? parseInt(returnOrder.status) : null,
            purchaseCurrency: returnOrder.purchase_currency || null,
            purchaseCurrencyIcon: returnOrder.purchase_currency_icon || null,
            feePartType: returnOrder.fee_part_type !== undefined && returnOrder.fee_part_type !== null ? parseInt(returnOrder.fee_part_type) : null,
            shippingCurrency: returnOrder.shipping_currency || null,
            shippingPrice: returnOrder.shipping_price !== undefined && returnOrder.shipping_price !== null && returnOrder.shipping_price !== '' ? parseFloat(returnOrder.shipping_price) : null,
            otherCurrency: returnOrder.other_currency || null,
            otherFee: returnOrder.other_fee !== undefined && returnOrder.other_fee !== null && returnOrder.other_fee !== '' ? parseFloat(returnOrder.other_fee) : null,
            returnReason: returnOrder.return_reason || null,
            returnAmountTotal: returnOrder.return_amount_total !== undefined && returnOrder.return_amount_total !== null && returnOrder.return_amount_total !== '' ? parseFloat(returnOrder.return_amount_total) : null,
            remark: returnOrder.remark || null,
            returnOrderData: returnOrder, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            orderSn: returnOrder.order_sn,
            accountId: accountId,
            wid: returnOrder.wid !== undefined && returnOrder.wid !== null ? BigInt(returnOrder.wid) : null,
            createUid: returnOrder.create_uid !== undefined && returnOrder.create_uid !== null ? BigInt(returnOrder.create_uid) : null,
            createRealname: returnOrder.create_realname || null,
            createTime: returnOrder.create_time || null,
            lastTime: returnOrder.last_time || null,
            buyerUid: returnOrder.buyer_uid !== undefined && returnOrder.buyer_uid !== null ? BigInt(returnOrder.buyer_uid) : null,
            buyerRealname: returnOrder.buyer_realname || null,
            purchaseOrderSn: returnOrder.purchase_order_sn || null,
            supplierId: returnOrder.supplier_id !== undefined && returnOrder.supplier_id !== null ? BigInt(returnOrder.supplier_id) : null,
            supplierName: returnOrder.supplier_name || null,
            returnMethod: returnOrder.return_method !== undefined && returnOrder.return_method !== null ? parseInt(returnOrder.return_method) : null,
            replenishMethod: returnOrder.replenish_method !== undefined && returnOrder.replenish_method !== null ? parseInt(returnOrder.replenish_method) : null,
            receiptFundsOrderSn: returnOrder.receipt_funds_order_sn || null,
            status: returnOrder.status !== undefined && returnOrder.status !== null ? parseInt(returnOrder.status) : null,
            purchaseCurrency: returnOrder.purchase_currency || null,
            purchaseCurrencyIcon: returnOrder.purchase_currency_icon || null,
            feePartType: returnOrder.fee_part_type !== undefined && returnOrder.fee_part_type !== null ? parseInt(returnOrder.fee_part_type) : null,
            shippingCurrency: returnOrder.shipping_currency || null,
            shippingPrice: returnOrder.shipping_price !== undefined && returnOrder.shipping_price !== null && returnOrder.shipping_price !== '' ? parseFloat(returnOrder.shipping_price) : null,
            otherCurrency: returnOrder.other_currency || null,
            otherFee: returnOrder.other_fee !== undefined && returnOrder.other_fee !== null && returnOrder.other_fee !== '' ? parseFloat(returnOrder.other_fee) : null,
            returnReason: returnOrder.return_reason || null,
            returnAmountTotal: returnOrder.return_amount_total !== undefined && returnOrder.return_amount_total !== null && returnOrder.return_amount_total !== '' ? parseFloat(returnOrder.return_amount_total) : null,
            remark: returnOrder.remark || null,
            returnOrderData: returnOrder // 保存完整数据
          }
        });

        // 保存采购退货单项
        if (returnOrder.item_list && returnOrder.item_list.length > 0) {
          await this.savePurchaseReturnOrderItems(purchaseReturnOrder.id, returnOrder.item_list);
        }
      }

      console.log(`采购退货单列表已保存到数据库: 共 ${returnOrders.length} 个采购退货单`);
    } catch (error) {
      console.error('保存采购退货单列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 保存采购退货单项到数据库
   * @param {string} returnOrderId - 采购退货单ID
   * @param {Array} items - 采购退货单项数组
   */
  async savePurchaseReturnOrderItems(returnOrderId, items) {
    try {
      if (!prisma.lingXingPurchaseReturnOrderItem) {
        console.error('Prisma Client 中未找到 lingXingPurchaseReturnOrderItem 模型');
        return;
      }

      for (const item of items) {
        if (item.item_id === undefined || item.item_id === null) {
          continue;
        }

        // 将 itemId 转换为 BigInt
        const returnItemIdBigInt = item.item_id !== undefined && item.item_id !== null ? BigInt(item.item_id) : null;
        
        await prisma.lingXingPurchaseReturnOrderItem.upsert({
          where: {
            itemId: returnItemIdBigInt
          },
          update: {
            returnOrderId: returnOrderId,
            spuName: item.spu_name || null,
            spu: item.spu || null,
            productName: item.product_name || null,
            sku: item.sku || null,
            fnsku: item.fnsku || null,
            msku: item.msku || null,
            attribute: item.attribute || null,
            price: item.price !== undefined && item.price !== null && item.price !== '' ? parseFloat(item.price) : null,
            quantityReal: item.quantity_real !== undefined && item.quantity_real !== null ? parseInt(item.quantity_real) : null,
            returnGoodNum: item.return_good_num !== undefined && item.return_good_num !== null ? parseInt(item.return_good_num) : null,
            returnBadNum: item.return_bad_num !== undefined && item.return_bad_num !== null ? parseInt(item.return_bad_num) : null,
            replenishNum: item.replenish_num !== undefined && item.replenish_num !== null ? parseInt(item.replenish_num) : null,
            deductionAmount: item.deduction_amount !== undefined && item.deduction_amount !== null && item.deduction_amount !== '' ? parseFloat(item.deduction_amount) : null,
            expectArriveTime: item.expect_arrive_time || null,
            remark: item.remark || null,
            sellerId: item.seller_id || null,
            itemData: item, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            itemId: returnItemIdBigInt,
            returnOrderId: returnOrderId,
            spuName: item.spu_name || null,
            spu: item.spu || null,
            productName: item.product_name || null,
            sku: item.sku || null,
            fnsku: item.fnsku || null,
            msku: item.msku || null,
            attribute: item.attribute || null,
            price: item.price !== undefined && item.price !== null && item.price !== '' ? parseFloat(item.price) : null,
            quantityReal: item.quantity_real !== undefined && item.quantity_real !== null ? parseInt(item.quantity_real) : null,
            returnGoodNum: item.return_good_num !== undefined && item.return_good_num !== null ? parseInt(item.return_good_num) : null,
            returnBadNum: item.return_bad_num !== undefined && item.return_bad_num !== null ? parseInt(item.return_bad_num) : null,
            replenishNum: item.replenish_num !== undefined && item.replenish_num !== null ? parseInt(item.replenish_num) : null,
            deductionAmount: item.deduction_amount !== undefined && item.deduction_amount !== null && item.deduction_amount !== '' ? parseFloat(item.deduction_amount) : null,
            expectArriveTime: item.expect_arrive_time || null,
            remark: item.remark || null,
            sellerId: item.seller_id || null,
            itemData: item // 保存完整数据
          }
        });
      }
    } catch (error) {
      console.error('保存采购退货单项到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有供应商列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认1000）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { suppliers: [], total: 0, stats: {} }
   */
  async fetchAllSuppliers(accountId, options = {}) {
    const {
      pageSize = 1000,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有供应商列表...');

      const allSuppliers = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 1000);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页供应商（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getSupplierList(accountId, {
            offset: offset,
            length: actualPageSize
          });

          const pageSuppliers = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个供应商，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allSuppliers.push(...pageSuppliers);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageSuppliers.length} 个供应商，累计 ${allSuppliers.length} 个供应商`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allSuppliers.length, totalCount);
          }

          if (pageSuppliers.length < actualPageSize || allSuppliers.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页供应商失败:`, error.message);
          if (allSuppliers.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有供应商列表获取完成，共 ${allSuppliers.length} 个供应商`);

      return {
        suppliers: allSuppliers,
        total: allSuppliers.length,
        stats: {
          totalSuppliers: allSuppliers.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有供应商列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有采购方列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { purchasers: [], total: 0, stats: {} }
   */
  async fetchAllPurchasers(accountId, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有采购方列表...');

      const allPurchasers = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 500);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页采购方（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchaserList(accountId, {
            offset: offset,
            length: actualPageSize
          });

          const pagePurchasers = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个采购方，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allPurchasers.push(...pagePurchasers);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pagePurchasers.length} 个采购方，累计 ${allPurchasers.length} 个采购方`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allPurchasers.length, totalCount);
          }

          if (pagePurchasers.length < actualPageSize || allPurchasers.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页采购方失败:`, error.message);
          if (allPurchasers.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有采购方列表获取完成，共 ${allPurchasers.length} 个采购方`);

      return {
        purchasers: allPurchasers,
        total: allPurchasers.length,
        stats: {
          totalPurchasers: allPurchasers.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有采购方列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有采购计划列表（自动处理分页和时间范围）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 采购计划列表查询参数（必填）
   *   - search_field_time: 时间搜索维度（必填）
   *   - start_date: 开始日期（必填）
   *   - end_date: 结束日期（必填）
   *   - 其他查询参数...
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，上限500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { plans: [], total: 0, stats: {} }
   */
  async fetchAllPurchasePlans(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      if (!listParams.search_field_time || !listParams.start_date || !listParams.end_date) {
        throw new Error('search_field_time、start_date 和 end_date 为必填参数');
      }

      console.log('开始自动拉取所有采购计划列表...');

      const allPlans = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 500);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页采购计划（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchasePlanList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pagePlans = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个采购计划，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allPlans.push(...pagePlans);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pagePlans.length} 个采购计划，累计 ${allPlans.length} 个采购计划`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allPlans.length, totalCount);
          }

          if (pagePlans.length < actualPageSize || allPlans.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页采购计划失败:`, error.message);
          if (allPlans.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有采购计划列表获取完成，共 ${allPlans.length} 个采购计划`);

      return {
        plans: allPlans,
        total: allPlans.length,
        stats: {
          totalPlans: allPlans.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有采购计划列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有采购单列表（自动处理分页和时间范围）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 采购单列表查询参数（必填）
   *   - start_date: 开始时间（必填）
   *   - end_date: 结束时间（必填）
   *   - 其他查询参数...
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，上限500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { orders: [], total: 0, stats: {} }
   */
  async fetchAllPurchaseOrders(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      if (!listParams.start_date || !listParams.end_date) {
        throw new Error('start_date 和 end_date 为必填参数');
      }

      console.log('开始自动拉取所有采购单列表...');

      const allOrders = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 500);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页采购单（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchaseOrderList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个采购单，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allOrders.push(...pageOrders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageOrders.length} 个采购单，累计 ${allOrders.length} 个采购单`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allOrders.length, totalCount);
          }

          if (pageOrders.length < actualPageSize || allOrders.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页采购单失败:`, error.message);
          if (allOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有采购单列表获取完成，共 ${allOrders.length} 个采购单`);

      return {
        orders: allOrders,
        total: allOrders.length,
        stats: {
          totalOrders: allOrders.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有采购单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有采购退货单列表（自动处理分页和时间范围）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 采购退货单列表查询参数（可选）
   *   - search_field_time: 时间搜索维度（可选）
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - status: 状态数组（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，上限500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { returnOrders: [], total: 0, stats: {} }
   */
  async fetchAllPurchaseReturnOrders(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有采购退货单列表...');

      const allReturnOrders = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 500);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页采购退货单（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchaseReturnOrderList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageReturnOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个采购退货单，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allReturnOrders.push(...pageReturnOrders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageReturnOrders.length} 个采购退货单，累计 ${allReturnOrders.length} 个采购退货单`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allReturnOrders.length, totalCount);
          }

          if (pageReturnOrders.length < actualPageSize || allReturnOrders.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页采购退货单失败:`, error.message);
          if (allReturnOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有采购退货单列表获取完成，共 ${allReturnOrders.length} 个采购退货单`);

      return {
        returnOrders: allReturnOrders,
        total: allReturnOrders.length,
        stats: {
          totalReturnOrders: allReturnOrders.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有采购退货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询采购变更单列表
   * API: POST /erp/sc/routing/purchase/purchaseChangeOrder/changeOrderList
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数
   *   - search_field_time: 筛选时间类型（可选）：create_time/update_time，默认create_time
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - offset: 分页偏移量（必填）
   *   - length: 分页长度（必填）
   *   - multi_search_field: 搜索单号字段（可选）：order_sn/purchase_order_sn
   *   - multi_search_value: 批量搜索的单号值数组（可选）
   * @returns {Promise<Object>} 采购变更单列表数据 { data: [], total: 0 }
   */
  async getPurchaseChangeOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 验证必填参数
      if (params.offset === undefined || params.length === undefined) {
        throw new Error('offset 和 length 为必填参数');
      }

      // 构建请求参数
      const requestParams = {
        offset: params.offset,
        length: params.length,
        ...(params.search_field_time && { search_field_time: params.search_field_time }),
        ...(params.start_date && { start_date: params.start_date }),
        ...(params.end_date && { end_date: params.end_date }),
        ...(params.multi_search_field && { multi_search_field: params.multi_search_field }),
        ...(params.multi_search_value && params.multi_search_value.length > 0 && { multi_search_value: params.multi_search_value })
      };

      // 调用API获取采购变更单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/purchase/purchaseChangeOrder/changeOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取采购变更单列表失败');
      }

      // 注意：这个接口返回的数据结构是 { data: { total: 338, list: [...] } }
      const responseData = response.data || {};
      const changeOrders = responseData.list || [];
      const total = responseData.total || response.total || 0;

      // 保存采购变更单列表到数据库
      if (changeOrders.length > 0) {
        await this.savePurchaseChangeOrders(accountId, changeOrders);
      }

      return {
        data: changeOrders,
        total: total
      };
    } catch (error) {
      console.error('获取采购变更单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 保存采购变更单列表到数据库
   * @param {string} accountId - 领星账户ID
   * @param {Array} changeOrders - 采购变更单列表数据
   */
  async savePurchaseChangeOrders(accountId, changeOrders) {
    try {
      // 检查 Prisma Client 是否包含新模型
      if (!prisma.lingXingPurchaseChangeOrder) {
        console.error('Prisma Client 中未找到 lingXingPurchaseChangeOrder 模型，请重新生成 Prisma Client 并重启服务器');
        console.error('可用模型:', Object.keys(prisma).filter(k => k.includes('lingXing')).join(', '));
        return;
      }

      for (const changeOrder of changeOrders) {
        if (!changeOrder.order_sn) {
          continue;
        }

        // 处理 wid 和 old_wid（可能是字符串或数字）
        const widValue = changeOrder.wid !== undefined && changeOrder.wid !== null 
          ? (typeof changeOrder.wid === 'string' ? BigInt(changeOrder.wid) : BigInt(changeOrder.wid))
          : null;
        const oldWidValue = changeOrder.old_wid !== undefined && changeOrder.old_wid !== null
          ? (typeof changeOrder.old_wid === 'string' ? BigInt(changeOrder.old_wid) : BigInt(changeOrder.old_wid))
          : null;

        // 保存采购变更单主表
        const purchaseChangeOrder = await prisma.lingXingPurchaseChangeOrder.upsert({
          where: {
            orderSn: changeOrder.order_sn
          },
          update: {
            accountId: accountId,
            createTime: changeOrder.create_time || null,
            supplierName: changeOrder.supplier_name || null,
            oldSupplierName: changeOrder.old_supplier_name || null,
            wid: widValue,
            oldWid: oldWidValue,
            wareHouseName: changeOrder.ware_house_name || null,
            oldWareHouseName: changeOrder.old_ware_house_name || null,
            createRealname: changeOrder.create_realname || null,
            optRealname: changeOrder.opt_realname || null,
            oldOptRealname: changeOrder.old_opt_realname || null,
            remark: changeOrder.remark || null,
            status: changeOrder.status !== undefined && changeOrder.status !== null ? parseInt(changeOrder.status) : null,
            statusText: changeOrder.status_text || null,
            icon: changeOrder.icon || null,
            amount: changeOrder.amount !== undefined && changeOrder.amount !== null && changeOrder.amount !== '' ? parseFloat(changeOrder.amount) : null,
            oldAmount: changeOrder.old_amount !== undefined && changeOrder.old_amount !== null && changeOrder.old_amount !== '' ? parseFloat(changeOrder.old_amount) : null,
            changeOrderData: changeOrder, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            orderSn: changeOrder.order_sn,
            accountId: accountId,
            createTime: changeOrder.create_time || null,
            supplierName: changeOrder.supplier_name || null,
            oldSupplierName: changeOrder.old_supplier_name || null,
            wid: widValue,
            oldWid: oldWidValue,
            wareHouseName: changeOrder.ware_house_name || null,
            oldWareHouseName: changeOrder.old_ware_house_name || null,
            createRealname: changeOrder.create_realname || null,
            optRealname: changeOrder.opt_realname || null,
            oldOptRealname: changeOrder.old_opt_realname || null,
            remark: changeOrder.remark || null,
            status: changeOrder.status !== undefined && changeOrder.status !== null ? parseInt(changeOrder.status) : null,
            statusText: changeOrder.status_text || null,
            icon: changeOrder.icon || null,
            amount: changeOrder.amount !== undefined && changeOrder.amount !== null && changeOrder.amount !== '' ? parseFloat(changeOrder.amount) : null,
            oldAmount: changeOrder.old_amount !== undefined && changeOrder.old_amount !== null && changeOrder.old_amount !== '' ? parseFloat(changeOrder.old_amount) : null,
            changeOrderData: changeOrder // 保存完整数据
          }
        });

        // 保存采购变更单项
        if (changeOrder.item_list && changeOrder.item_list.length > 0) {
          await this.savePurchaseChangeOrderItems(purchaseChangeOrder.id, changeOrder.item_list);
        }
      }

      console.log(`采购变更单列表已保存到数据库: 共 ${changeOrders.length} 个采购变更单`);
    } catch (error) {
      console.error('保存采购变更单列表到数据库失败:', error.message);
      console.error('错误详情:', error);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 保存采购变更单项到数据库
   * @param {string} changeOrderId - 采购变更单ID
   * @param {Array} items - 采购变更单项数组
   */
  async savePurchaseChangeOrderItems(changeOrderId, items) {
    try {
      if (!prisma.lingXingPurchaseChangeOrderItem) {
        console.error('Prisma Client 中未找到 lingXingPurchaseChangeOrderItem 模型');
        return;
      }

      for (const item of items) {
        // 处理 wid 和 old_wid（可能是数组、字符串或数字）
        // 由于 schema 中定义为 Json?，我们需要将所有值存储为 JSON 兼容格式
        let widValue = null;
        if (item.wid !== undefined && item.wid !== null) {
          if (Array.isArray(item.wid)) {
            // 如果是数组，将数组中的每个元素转换为字符串（处理大整数）
            widValue = item.wid.map(w => {
              if (typeof w === 'string' || typeof w === 'number') {
                try {
                  const bigIntVal = BigInt(w);
                  // 如果值很大，转换为字符串；否则保持原样
                  return bigIntVal > Number.MAX_SAFE_INTEGER ? bigIntVal.toString() : Number(bigIntVal);
                } catch {
                  return w;
                }
              }
              return w;
            });
          } else {
            // 单个值：转换为字符串（因为 JSON 不支持 BigInt）
            const widNum = typeof item.wid === 'string' ? BigInt(item.wid) : BigInt(item.wid);
            widValue = widNum > Number.MAX_SAFE_INTEGER ? widNum.toString() : Number(widNum);
          }
        }

        let oldWidValue = null;
        if (item.old_wid !== undefined && item.old_wid !== null) {
          if (Array.isArray(item.old_wid)) {
            // 如果是数组，将数组中的每个元素转换为字符串（处理大整数）
            oldWidValue = item.old_wid.map(w => {
              if (typeof w === 'string' || typeof w === 'number') {
                try {
                  const bigIntVal = BigInt(w);
                  // 如果值很大，转换为字符串；否则保持原样
                  return bigIntVal > Number.MAX_SAFE_INTEGER ? bigIntVal.toString() : Number(bigIntVal);
                } catch {
                  return w;
                }
              }
              return w;
            });
          } else {
            // 单个值：转换为字符串（因为 JSON 不支持 BigInt）
            const oldWidNum = typeof item.old_wid === 'string' ? BigInt(item.old_wid) : BigInt(item.old_wid);
            oldWidValue = oldWidNum > Number.MAX_SAFE_INTEGER ? oldWidNum.toString() : Number(oldWidNum);
          }
        }

        await prisma.lingXingPurchaseChangeOrderItem.upsert({
          where: {
            changeOrderId_productId_sku: {
              changeOrderId: changeOrderId,
              productId: item.product_id || '',
              sku: item.sku || ''
            }
          },
          update: {
            wid: widValue,
            oldWid: oldWidValue,
            wareHouseName: item.ware_house_name || null,
            oldWareHouseName: item.old_ware_house_name || null,
            spu: item.spu || null,
            spuName: item.spu_name || null,
            productName: item.product_name || null,
            attribute: item.attribute || null,
            fnsku: item.fnsku || null,
            price: item.price !== undefined && item.price !== null && item.price !== '' ? parseFloat(item.price) : null,
            oldPrice: item.old_price !== undefined && item.old_price !== null && item.old_price !== '' ? parseFloat(item.old_price) : null,
            priceWithoutTax: item.price_without_tax !== undefined && item.price_without_tax !== null ? parseFloat(item.price_without_tax) : null,
            oldPriceWithoutTax: item.old_price_without_tax !== undefined && item.old_price_without_tax !== null ? parseFloat(item.old_price_without_tax) : null,
            taxRate: item.tax_rate !== undefined && item.tax_rate !== null ? parseFloat(item.tax_rate) : null,
            oldTaxRate: item.old_tax_rate !== undefined && item.old_tax_rate !== null ? parseFloat(item.old_tax_rate) : null,
            quantityReal: item.quantity_real !== undefined && item.quantity_real !== null ? parseFloat(item.quantity_real) : null,
            oldQuantityReal: item.old_quantity_real !== undefined && item.old_quantity_real !== null ? parseFloat(item.old_quantity_real) : null,
            amount: item.amount !== undefined && item.amount !== null ? parseFloat(item.amount) : null,
            oldAmount: item.old_amount !== undefined && item.old_amount !== null ? parseFloat(item.old_amount) : null,
            seller: item.seller || null,
            msku: item.msku || null,
            isTax: item.is_tax !== undefined && item.is_tax !== null ? parseInt(item.is_tax) : null,
            isAux: item.is_aux !== undefined && item.is_aux !== null ? parseInt(item.is_aux) : null,
            expectArriveTime: item.expect_arrive_time || null,
            oldExpectArriveTime: item.old_expect_arrive_time || null,
            itemData: item, // 保存完整数据
            updatedAt: new Date()
          },
          create: {
            changeOrderId: changeOrderId,
            productId: item.product_id || '',
            sku: item.sku || '',
            wid: widValue,
            oldWid: oldWidValue,
            wareHouseName: item.ware_house_name || null,
            oldWareHouseName: item.old_ware_house_name || null,
            spu: item.spu || null,
            spuName: item.spu_name || null,
            productName: item.product_name || null,
            attribute: item.attribute || null,
            fnsku: item.fnsku || null,
            price: item.price !== undefined && item.price !== null && item.price !== '' ? parseFloat(item.price) : null,
            oldPrice: item.old_price !== undefined && item.old_price !== null && item.old_price !== '' ? parseFloat(item.old_price) : null,
            priceWithoutTax: item.price_without_tax !== undefined && item.price_without_tax !== null ? parseFloat(item.price_without_tax) : null,
            oldPriceWithoutTax: item.old_price_without_tax !== undefined && item.old_price_without_tax !== null ? parseFloat(item.old_price_without_tax) : null,
            taxRate: item.tax_rate !== undefined && item.tax_rate !== null ? parseFloat(item.tax_rate) : null,
            oldTaxRate: item.old_tax_rate !== undefined && item.old_tax_rate !== null ? parseFloat(item.old_tax_rate) : null,
            quantityReal: item.quantity_real !== undefined && item.quantity_real !== null ? parseFloat(item.quantity_real) : null,
            oldQuantityReal: item.old_quantity_real !== undefined && item.old_quantity_real !== null ? parseFloat(item.old_quantity_real) : null,
            amount: item.amount !== undefined && item.amount !== null ? parseFloat(item.amount) : null,
            oldAmount: item.old_amount !== undefined && item.old_amount !== null ? parseFloat(item.old_amount) : null,
            seller: item.seller || null,
            msku: item.msku || null,
            isTax: item.is_tax !== undefined && item.is_tax !== null ? parseInt(item.is_tax) : null,
            isAux: item.is_aux !== undefined && item.is_aux !== null ? parseInt(item.is_aux) : null,
            expectArriveTime: item.expect_arrive_time || null,
            oldExpectArriveTime: item.old_expect_arrive_time || null,
            itemData: item // 保存完整数据
          }
        });
      }
    } catch (error) {
      console.error('保存采购变更单项到数据库失败:', error.message);
      // 不抛出错误，因为保存失败不应该影响API调用
    }
  }

  /**
   * 自动拉取所有采购变更单列表（自动处理分页和时间范围）
   * @param {string} accountId - 领星账户ID
   * @param {Object} listParams - 采购变更单列表查询参数（可选）
   *   - search_field_time: 筛选时间类型（可选）：create_time/update_time
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - multi_search_field: 搜索单号字段（可选）
   *   - multi_search_value: 批量搜索的单号值数组（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，上限500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { changeOrders: [], total: 0, stats: {} }
   */
  async fetchAllPurchaseChangeOrders(accountId, listParams = {}, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    try {
      console.log('开始自动拉取所有采购变更单列表...');

      const allChangeOrders = [];
      let offset = 0;
      const actualPageSize = Math.min(pageSize, 500);
      let totalCount = 0;
      let currentPage = 0;
      let hasMore = true;

      while (hasMore) {
        currentPage++;
        console.log(`正在获取第 ${currentPage} 页采购变更单（offset: ${offset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchaseChangeOrderList(accountId, {
            ...listParams,
            offset: offset,
            length: actualPageSize
          });

          const pageChangeOrders = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentPage === 1) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个采购变更单，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allChangeOrders.push(...pageChangeOrders);
          console.log(`第 ${currentPage} 页获取完成，本页 ${pageChangeOrders.length} 个采购变更单，累计 ${allChangeOrders.length} 个采购变更单`);

          if (onProgress) {
            onProgress(currentPage, Math.ceil(totalCount / actualPageSize), allChangeOrders.length, totalCount);
          }

          if (pageChangeOrders.length < actualPageSize || allChangeOrders.length >= totalCount) {
            hasMore = false;
          } else {
            offset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取第 ${currentPage} 页采购变更单失败:`, error.message);
          if (allChangeOrders.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有采购变更单列表获取完成，共 ${allChangeOrders.length} 个采购变更单`);

      return {
        changeOrders: allChangeOrders,
        total: allChangeOrders.length,
        stats: {
          totalChangeOrders: allChangeOrders.length,
          pagesFetched: currentPage
        }
      };
    } catch (error) {
      console.error('自动拉取所有采购变更单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 查询收货单列表
   * API: POST /erp/sc/routing/deliveryReceipt/PurchaseReceiptOrder/getOrderList
   * @param {string} accountId - 领星账户ID
   * @param {Object} params - 查询参数（所有参数都是可选的）
   *   - date_type: 查询时间类型（可选）：1 预计到货时间，2 收货时间，3 创建时间，4 更新时间
   *   - start_date: 开始时间，格式：Y-m-d 或 Y-m-d H:i:s（可选）
   *   - end_date: 结束时间，格式：Y-m-d 或 Y-m-d H:i:s（可选）
   *   - order_sns: 收货单号，多个使用英文逗号分隔（可选）
   *   - status: 状态（可选）：10 待收货，40 已完成
   *   - wid: 仓库id，多个使用英文逗号分隔（可选）
   *   - order_type: 收货类型（可选）：1 采购订单，2 委外订单
   *   - qc_status: 质检状态，多个使用英文逗号分隔（可选）：0 未质检，1 部分质检，2 完成质检
   *   - offset: 分页偏移量（可选，默认0）
   *   - length: 分页长度（可选，默认200，上限500）
   * @returns {Promise<Object>} 收货单列表数据 { data: { total: 0, list: [] } }
   */
  async getPurchaseReceiptOrderList(accountId, params = {}) {
    try {
      const account = await prisma.lingXingAccount.findUnique({
        where: { id: accountId }
      });

      if (!account) {
        throw new Error(`领星账户不存在: ${accountId}`);
      }

      // 构建请求参数（所有参数都是可选的）
      const requestParams = {};
      if (params.date_type !== undefined) {
        requestParams.date_type = params.date_type;
      }
      if (params.start_date !== undefined) {
        requestParams.start_date = params.start_date;
      }
      if (params.end_date !== undefined) {
        requestParams.end_date = params.end_date;
      }
      if (params.order_sns !== undefined) {
        requestParams.order_sns = params.order_sns;
      }
      if (params.status !== undefined) {
        requestParams.status = params.status;
      }
      if (params.wid !== undefined) {
        requestParams.wid = params.wid;
      }
      if (params.order_type !== undefined) {
        requestParams.order_type = params.order_type;
      }
      if (params.qc_status !== undefined) {
        requestParams.qc_status = params.qc_status;
      }
      if (params.offset !== undefined) {
        requestParams.offset = params.offset;
      }
      if (params.length !== undefined) {
        // 确保 length 不超过 500
        requestParams.length = Math.min(params.length, 500);
      }

      // 调用API获取收货单列表（使用通用客户端，成功码为0，令牌桶容量为1）
      const response = await this.post(account, '/erp/sc/routing/deliveryReceipt/PurchaseReceiptOrder/getOrderList', requestParams, {
        successCode: [0, 200, '200']
      });

      // 检查响应格式（注意：这个接口返回的code是0表示成功）
      if (response.code !== 0 && response.code !== 200 && response.code !== '200') {
        throw new Error(response.message || '获取收货单列表失败');
      }

      // 注意：这个接口返回的数据结构是 { data: { total: 2, list: [...] } }
      const responseData = response.data || {};
      const receiptOrderList = responseData.list || [];
      const total = responseData.total || response.total || 0;

      return {
        data: receiptOrderList,
        total: total
      };
    } catch (error) {
      console.error('获取收货单列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 自动拉取所有收货单列表（自动处理分页）
   * @param {string} accountId - 领星账户ID
   * @param {Object} filterParams - 筛选参数（可选）
   *   - date_type: 查询时间类型（可选）
   *   - start_date: 开始时间（可选）
   *   - end_date: 结束时间（可选）
   *   - order_sns: 收货单号（可选）
   *   - status: 状态（可选）
   *   - wid: 仓库id（可选）
   *   - order_type: 收货类型（可选）
   *   - qc_status: 质检状态（可选）
   * @param {Object} options - 选项
   *   - pageSize: 每页大小（默认500，最大500）
   *   - delayBetweenPages: 分页之间的延迟时间（毫秒，默认500）
   *   - onProgress: 进度回调函数 (currentPage, totalPages, currentCount, totalCount) => void
   * @returns {Promise<Object>} { receiptOrderList: [], total: 0, stats: {} }
   */
  async fetchAllPurchaseReceiptOrders(accountId, filterParams = {}, options = {}) {
    const {
      pageSize = 500,
      delayBetweenPages = 500,
      onProgress = null
    } = options;

    // 确保 pageSize 不超过 500
    const actualPageSize = Math.min(pageSize, 500);

    try {
      console.log('开始自动拉取所有收货单列表...');

      const allReceiptOrderList = [];
      let currentOffset = 0;
      let totalCount = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(`正在获取收货单列表（offset: ${currentOffset}, length: ${actualPageSize}）...`);

        try {
          const pageResult = await this.getPurchaseReceiptOrderList(accountId, {
            ...filterParams,
            offset: currentOffset,
            length: actualPageSize
          });

          const pageReceiptOrderList = pageResult.data || [];
          const pageTotal = pageResult.total || 0;

          if (currentOffset === 0) {
            totalCount = pageTotal;
            console.log(`总共需要获取 ${totalCount} 个收货单，预计 ${Math.ceil(totalCount / actualPageSize)} 页`);
          }

          allReceiptOrderList.push(...pageReceiptOrderList);
          console.log(`获取完成，本页 ${pageReceiptOrderList.length} 个收货单，累计 ${allReceiptOrderList.length} 个收货单`);

          if (onProgress) {
            onProgress(Math.floor(currentOffset / actualPageSize) + 1, Math.ceil(totalCount / actualPageSize), allReceiptOrderList.length, totalCount);
          }

          if (pageReceiptOrderList.length < actualPageSize || allReceiptOrderList.length >= totalCount) {
            hasMore = false;
          } else {
            currentOffset += actualPageSize;
            if (delayBetweenPages > 0) {
              await new Promise(resolve => setTimeout(resolve, delayBetweenPages));
            }
          }
        } catch (error) {
          console.error(`获取收货单列表失败:`, error.message);
          if (allReceiptOrderList.length === 0) {
            throw error;
          }
          hasMore = false;
        }
      }

      console.log(`所有收货单列表获取完成，共 ${allReceiptOrderList.length} 个收货单`);

      return {
        receiptOrderList: allReceiptOrderList,
        total: allReceiptOrderList.length,
        stats: {
          totalReceiptOrders: allReceiptOrderList.length,
          pagesFetched: Math.floor(currentOffset / actualPageSize) + 1
        }
      };
    } catch (error) {
      console.error('自动拉取所有收货单列表失败:', error.message);
      throw error;
    }
  }
}

export default new LingXingPurchaseService();

