import { AMAZON_MARKETPLACES } from '../../utils/amazon.js';
import prisma from '../config/database.js';
/**
 * 亚马逊路由插件
 */
async function amazonRoutes(fastify, options) {

    fastify.get('/auth-url', async (req, res) => {
        const { state, redirect_uri } = req.query;
        const clientId = process.env.AMAZON_APPLICATION_ID;
        const redirectUri = redirect_uri || process.env.AMAZON_REDIRECT_URI;
        const authUrl = `https://sellercentral.amazon.com/apps/authorize/consent?application_id=${clientId}&version=beta&state=${encodeURIComponent(state)}&redirect_uri=${encodeURIComponent(redirectUri)}`;
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

        // 判断 state 必须为 AMAZON_MARKETPLACES 中的 countryCode
        const SUPPORTED_COUNTRY_CODES = AMAZON_MARKETPLACES.map(m => m.countryCode);
        if (!state || !SUPPORTED_COUNTRY_CODES.includes(state)) {
            return res.status(400).send({
                error: 'Invalid state. Must be a valid Amazon marketplace country code.',
                code: 'INVALID_STATE'
            });
        }

        try {
            console.log('Received SP-API OAuth code:', spapi_oauth_code);
            console.log('State:', state);
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


                // 根据 AmazonStore model，插入或更新（upsert）Amazon 店铺数据
                await prisma.amazonStore.upsert({
                    where: {
                        accountId,
                        sellerId: selling_partner_id,
                        countryCode: state
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
                        countryCode: state,
                        linkedAt: new Date(),
                        archived: false,
                        isAuthorized: true
                    }
                });

                console.log(`Created/Updated shop authorization for sellerId: ${selling_partner_id}`);

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
                selling_partner_id
            };

        } catch (error) {
            console.error('Amazon callback error:', error);
            return res.status(500).send({
                error: 'Internal server error during Amazon callback',
                code: 'INTERNAL_ERROR'
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
export default amazonRoutes;

