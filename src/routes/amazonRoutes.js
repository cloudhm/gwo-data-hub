import { AMAZON_MARKETPLACES, getAuthBaseUrl, generateAmazonRequestId } from '../utils/amazon.js';
import prisma from '../config/database.js';
import amazonVcPoService from '../services/amazon/amazonVcPoService.js';
import amazonVcReportService from '../services/amazon/amazonVcReportService.js';

/**
 * 亚马逊路由插件（独立于领星：授权、VC PO 拉取等）
 */
async function amazonRoutes(fastify, options) {

    fastify.get('/auth-url', async (req, res) => {
        const { state, redirect_uri, account_type } = req.query;
        const clientId = process.env.AMAZON_APPLICATION_ID;
        const redirectUri = redirect_uri || process.env.AMAZON_REDIRECT_URI;
        const type = (account_type || 'sc').toString().toLowerCase();
        const accountType = type === 'vc' ? 'vc' : 'sc';
        const countryCode = (state || 'US').toString().trim();
        const baseUrl = getAuthBaseUrl(countryCode, accountType);
        // 将账户类型编入 state，回调时可解析出 sc/vc
        const stateParam = `${countryCode}:${accountType}`;
        const authUrl = `${baseUrl}/apps/authorize/consent?application_id=${clientId}&version=beta&state=${encodeURIComponent(stateParam)}&redirect_uri=${encodeURIComponent(redirectUri)}`;
        return { url: authUrl };
      });


    fastify.get('/callback', {}, async (req, res) => {
        const { state, selling_partner_id, spapi_oauth_code } = req.query;


        if (!spapi_oauth_code) {
            return res.status(400).send({ error: 'SP-API OAuth code is required' });
        }

        if (!selling_partner_id) {
            return res.status(400).send({ error: 'Selling Partner ID is required' });
        }

        // 解析 state：支持 "US" 或 "US:vc" 格式，后者用于区分 SC/VC
        let countryCode = state;
        let accountType = 'sc';
        if (state && state.includes(':')) {
            const [code, type] = state.split(':');
            countryCode = code?.trim() || '';
            const t = (type || '').toString().toLowerCase();
            accountType = t === 'vc' ? 'vc' : 'sc';
        }

        const SUPPORTED_COUNTRY_CODES = AMAZON_MARKETPLACES.map(m => m.countryCode);
        if (!countryCode || !SUPPORTED_COUNTRY_CODES.includes(countryCode)) {
            return res.status(400).send({
                error: 'Invalid state. Must be a valid Amazon marketplace country code (e.g. US or US:vc).',
                code: 'INVALID_STATE'
            });
        }

        try {
            console.log('Received SP-API OAuth code:', spapi_oauth_code);
            console.log('State:', state, '-> countryCode:', countryCode, 'accountType:', accountType);
            console.log('Selling Partner ID:', selling_partner_id);

            try {
                // 交换授权码获取访问令牌
                const tokenResponse = await exchangeCodeForToken(spapi_oauth_code);

                if (!tokenResponse || !tokenResponse.refresh_token) {
                    return res.status(400).send({
                        error: 'Failed to obtain refresh token from Amazon',
                        code: 'TOKEN_EXCHANGE_FAILED'
                    });
                }

                // 根据 selling_partner_id (即 sellerId) 查询 LingXingConceptSeller 表
                // 假设有 prisma 实例已初始化为 prisma，可通过 prisma.lingXingConceptSeller
                let lingxingConceptStore = null;
                try {
                    lingxingConceptStore = await prisma.lingXingConceptSeller.findFirst({
                        where: {
                            sellerId: selling_partner_id
                        }
                    });
                } catch (err) {
                    console.error('Failed to query LingXingConceptSeller:', err);
                }
                if (!lingxingConceptStore) {
                    return res.status(400).send({
                        error: 'LingXingConceptSeller not found',
                        code: 'LINGXING_CONCEPT_SELLER_NOT_FOUND'
                    });
                }
                const accountId = lingxingConceptStore.accountId;


                // 根据 AmazonStore model，插入或更新（upsert）Amazon 店铺数据，并写入 accountType
                await prisma.amazonStore.upsert({
                    where: {
                        accountId_sellerId_countryCode_accountType: {
                            accountId,
                            sellerId: selling_partner_id,
                            countryCode,
                            accountType
                        }
                    },
                    update: {
                        refreshToken: tokenResponse.refresh_token,
                        updatedAt: new Date(),
                        linkedAt: new Date(),
                        isAuthorized: true,
                        archived: false
                    },
                    create: {
                        sellerId: selling_partner_id,
                        accountId,
                        refreshToken: tokenResponse.refresh_token,
                        createdAt: new Date(),
                        updatedAt: new Date(),
                        countryCode,
                        accountType,
                        linkedAt: new Date(),
                        archived: false,
                        isAuthorized: true
                    }
                });

                console.log(`Created/Updated shop authorization for sellerId: ${selling_partner_id}, accountType: ${accountType}`);

            } catch (error) {
                console.error('Failed to exchange code for token:', error);
                return res.status(500).send({
                    error: 'Failed to exchange authorization code for token',
                    code: 'TOKEN_EXCHANGE_ERROR',
                    details: error instanceof Error ? error.message : String(error)
                });
            }

            return {
                success: true,
                message: 'Amazon authorization completed successfully',
                selling_partner_id,
                account_type: accountType
            };

        } catch (error) {
            console.error(`[Amazon] [${generateAmazonRequestId()}] callback error:`, error);
            return res.status(500).send({
                error: 'Internal server error during Amazon callback',
                code: 'INTERNAL_ERROR'
            });
        }
    });

    /**
     * 列出已授权的 VC 店铺（用于独立亚马逊 VC 功能，与领星无关）
     * GET /api/amazon/vc-stores
     */
    fastify.get('/vc-stores', async (req, res) => {
        const stores = await amazonVcPoService.getVcStores();
        return { success: true, data: stores };
    });

    /**
     * 增量拉取所有 VC 店铺的 PO 单
     * POST /api/amazon/vc-purchase-orders/sync
     * Body: { endDate?: 'YYYY-MM-DD', defaultLookbackDays?: number }
     */
    fastify.post('/vc-purchase-orders/sync', async (req, res) => {
        const options = req.body || {};
        try {
            const result = await amazonVcPoService.incrementalSyncAllVcPo(options);
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC PO] /vc-purchase-orders/sync 未捕获异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || 'VC PO 增量同步失败',
                error: e?.message
            });
        }
    });

    /**
     * 仅处理 VC PO 限流重试队列（到点的任务），可由定时任务每 10 分钟调一次
     * POST /api/amazon/vc-purchase-orders/process-retry-queue
     */
    fastify.post('/vc-purchase-orders/process-retry-queue', async (req, res) => {
        try {
            const result = await amazonVcPoService.processRetryQueue();
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC PO] /vc-purchase-orders/process-retry-queue 异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || '处理重试队列失败',
                error: e?.message
            });
        }
    });

    /**
     * 增量拉取所有 VC 店铺的 DF 单（Direct Fulfillment）
     * POST /api/amazon/vc-df-orders/sync
     * Body: { endDate?: 'YYYY-MM-DD', defaultLookbackDays?: number }
     */
    fastify.post('/vc-df-orders/sync', async (req, res) => {
        const options = req.body || {};
        try {
            const result = await amazonVcPoService.incrementalSyncAllVcDf(options);
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC DF] /vc-df-orders/sync 未捕获异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || 'VC DF 增量同步失败',
                error: e?.message
            });
        }
    });

    /**
     * 仅处理 VC DF 限流重试队列
     * POST /api/amazon/vc-df-orders/process-retry-queue
     */
    fastify.post('/vc-df-orders/process-retry-queue', async (req, res) => {
        try {
            const result = await amazonVcPoService.processDfRetryQueue();
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC DF] /vc-df-orders/process-retry-queue 异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || '处理 DF 重试队列失败',
                error: e?.message
            });
        }
    });

    /**
     * 拉取最近 60 天内 VC 货件明细（GetShipmentDetails，Vendor Shipments API v1）
     * POST /api/amazon/vc-shipments/sync
     * Body: { endDate?: 'YYYY-MM-DD', defaultLookbackDays?: number } 默认 defaultLookbackDays=60
     */
    fastify.post('/vc-shipments/sync', async (req, res) => {
        const options = req.body || {};
        try {
            const result = await amazonVcPoService.incrementalSyncAllVcShipments(options);
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC Shipments] /vc-shipments/sync 未捕获异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || 'VC Shipments 拉取失败',
                error: e?.message
            });
        }
    });

    /**
     * VC 报表增量同步（所有 VC 店铺、所有 Vendor 报表类型，按天存储）
     * POST /api/amazon/vc-reports/sync
     * Body: { endDate?: 'YYYY-MM-DD', defaultLookbackDays?: number, logErrors?: boolean }
     */
    fastify.post('/vc-reports/sync', async (req, res) => {
        const options = req.body || {};
        try {
            const result = await amazonVcReportService.incrementalSyncAllVcReports(options);
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC Reports] /vc-reports/sync 未捕获异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || 'VC Reports 增量同步失败',
                error: e?.message
            });
        }
    });

    /**
     * 单个报表类型、所有 VC 店铺的按天增量同步
     * POST /api/amazon/vc-reports/sync-by-type
     * Body: { reportType: string, endDate?: 'YYYY-MM-DD', defaultLookbackDays?: number, logErrors?: boolean }
     */
    fastify.post('/vc-reports/sync-by-type', async (req, res) => {
        const { reportType, ...options } = req.body || {};
        if (!reportType || typeof reportType !== 'string') {
            return res.status(400).send({
                success: false,
                message: 'reportType 必填，且为字符串（如 GET_VENDOR_SALES_REPORT）',
                error: 'MISSING_REPORT_TYPE'
            });
        }
        try {
            const result = await amazonVcReportService.incrementalSyncReportTypeForAllStores(reportType.trim(), options);
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC Reports] /vc-reports/sync-by-type 未捕获异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || 'VC Reports 按类型增量同步失败',
                error: e?.message
            });
        }
    });

    /**
     * 处理 VC 报表待处理队列（createReport 后未 DONE 入队的任务，到点重试 getReport）
     * POST /api/amazon/vc-reports/process-pending-queue
     * Body: { maxItems?: number }
     */
    fastify.post('/vc-reports/process-pending-queue', async (req, res) => {
        const options = req.body || {};
        try {
            const result = await amazonVcReportService.processReportPendingQueue(options);
            return { success: true, data: result };
        } catch (e) {
            console.error('[VC Reports] /vc-reports/process-pending-queue 异常:', e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || '处理 VC 报表待处理队列失败',
                error: e?.message
            });
        }
    });

    /**
     * 根据店铺 id 生成并返回 access_token（LWA 用 refresh_token 刷新）
     * GET /api/amazon/access-token?storeId=xxx 或 POST /api/amazon/access-token Body: { storeId }
     */
    fastify.get('/access-token', async (req, res) => {
        const storeId = req.query?.storeId;
        if (!storeId) {
            return res.status(400).send({
                success: false,
                message: '请提供 storeId（query: ?storeId=xxx）',
                code: 'MISSING_STORE_ID'
            });
        }
        const store = await prisma.amazonStore.findUnique({ where: { id: storeId } });
        if (!store) {
            return res.status(404).send({
                success: false,
                message: '店铺不存在',
                code: 'STORE_NOT_FOUND'
            });
        }
        if (!store.refreshToken) {
            return res.status(400).send({
                success: false,
                message: '该店铺未授权或缺少 refresh_token',
                code: 'NO_REFRESH_TOKEN'
            });
        }
        try {
            const tokenData = await refreshAccessTokenByRefreshToken(store.refreshToken);
            return {
                success: true,
                access_token: tokenData.access_token,
                expires_in: tokenData.expires_in,
                token_type: tokenData.token_type || 'Bearer'
            };
        } catch (e) {
            console.error(`[Amazon] [${generateAmazonRequestId()}] access-token 刷新失败:`, e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || '获取 access_token 失败',
                code: 'TOKEN_REFRESH_FAILED'
            });
        }
    });

    fastify.post('/access-token', async (req, res) => {
        const storeId = (req.body && req.body.storeId) || req.query?.storeId;
        if (!storeId) {
            return res.status(400).send({
                success: false,
                message: '请提供 storeId（body: { storeId } 或 query: ?storeId=xxx）',
                code: 'MISSING_STORE_ID'
            });
        }
        const store = await prisma.amazonStore.findUnique({ where: { id: storeId } });
        if (!store) {
            return res.status(404).send({
                success: false,
                message: '店铺不存在',
                code: 'STORE_NOT_FOUND'
            });
        }
        if (!store.refreshToken) {
            return res.status(400).send({
                success: false,
                message: '该店铺未授权或缺少 refresh_token',
                code: 'NO_REFRESH_TOKEN'
            });
        }
        try {
            const tokenData = await refreshAccessTokenByRefreshToken(store.refreshToken);
            return {
                success: true,
                access_token: tokenData.access_token,
                expires_in: tokenData.expires_in,
                token_type: tokenData.token_type || 'Bearer'
            };
        } catch (e) {
            console.error(`[Amazon] [${generateAmazonRequestId()}] access-token 刷新失败:`, e?.message, e?.stack);
            return res.status(500).send({
                success: false,
                message: e?.message || '获取 access_token 失败',
                code: 'TOKEN_REFRESH_FAILED'
            });
        }
    });
}
async function exchangeCodeForToken(spapi_oauth_code) {
    const tokenUrl = 'https://api.amazon.com/auth/o2/token';
    const params = new URLSearchParams({
        grant_type: 'authorization_code',
        code: spapi_oauth_code,
        redirect_uri: process.env.AMAZON_REDIRECT_URI,
        client_id: process.env.AMAZON_CLIENT_ID,
        client_secret: process.env.AMAZON_CLIENT_SECRET
    });

    const response = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: params.toString()
    });

    if (!response.ok) {
        const errorText = await response.text();
        console.error('Token exchange failed:', response.status, errorText);
        throw new Error(`Token exchange failed: ${response.status} ${errorText}`);
    }

    const tokenData = await response.json();

    if (!tokenData.refresh_token) {
        throw new Error('No refresh token received from Amazon');
    }

    return {
        access_token: tokenData.access_token,
        refresh_token: tokenData.refresh_token,
        expires_in: tokenData.expires_in,
        token_type: tokenData.token_type
    };
}

/** 使用 refresh_token 刷新并返回 access_token（LWA） */
async function refreshAccessTokenByRefreshToken(refresh_token) {
    const clientId = process.env.AMAZON_CLIENT_ID || process.env.SELLING_PARTNER_APP_CLIENT_ID;
    const clientSecret = process.env.AMAZON_CLIENT_SECRET || process.env.SELLING_PARTNER_APP_CLIENT_SECRET;
    if (!clientId || !clientSecret) {
        throw new Error('未配置 AMAZON_CLIENT_ID / AMAZON_CLIENT_SECRET');
    }
    const tokenUrl = 'https://api.amazon.com/auth/o2/token';
    const params = new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token,
        client_id: clientId,
        client_secret: clientSecret
    });
    const response = await fetch(tokenUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: params.toString()
    });
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Token refresh failed: ${response.status} ${errorText}`);
    }
    const tokenData = await response.json();
    if (!tokenData.access_token) {
        throw new Error('No access_token in response');
    }
    return {
        access_token: tokenData.access_token,
        expires_in: tokenData.expires_in ?? 3600,
        token_type: tokenData.token_type || 'Bearer'
    };
}
export default amazonRoutes;

