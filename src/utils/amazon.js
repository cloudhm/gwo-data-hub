// AmazonMarketplace 对象结构:
// {
//   countryCode: string,      // 国家二字码 (如 "US")
//   marketplaceId: string,    // 亚马逊市场ID (如 "ATVPDKIKX0DER")
//   region: string,           // 亚马逊区域 (如 "na")
//   endpoint: string,         // API端点
//   currency: string,         // 货币代码
//   name: string,            // 国家/地区名称
//   marketplaceName: string   // marketplace-name, 如 Amazon.com.mx
// }

const AMAZON_MARKETPLACES = [
  // 北美地区
  {
    countryCode: "US",
    marketplaceId: "ATVPDKIKX0DER",
    region: "na",
    endpoint: "https://sellingpartnerapi-na.amazon.com",
    currency: "USD",
    name: "United States",
    marketplaceName: "Amazon.com"
  },
  {
    countryCode: "CA",
    marketplaceId: "A2EUQ1WTGCTBG2",
    region: "na",
    endpoint: "https://sellingpartnerapi-na.amazon.com",
    currency: "CAD",
    name: "Canada",
    marketplaceName: "Amazon.ca"
  },
  {
    countryCode: "MX",
    marketplaceId: "A1AM78C64UM0Y8",
    region: "na",
    endpoint: "https://sellingpartnerapi-na.amazon.com",
    currency: "MXN",
    name: "Mexico",
    marketplaceName: "Amazon.com.mx"
  },
  {
    countryCode: "BR",
    marketplaceId: "A2Q3Y263D00KWC",
    region: "na",
    endpoint: "https://sellingpartnerapi-na.amazon.com",
    currency: "BRL",
    name: "Brazil",
    marketplaceName: "Amazon.com.br"
  },

  // 欧洲地区
  {
    countryCode: "UK",
    marketplaceId: "A1F83G8C2ARO7P",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "GBP",
    name: "United Kingdom",
    marketplaceName: "Amazon.co.uk"
  },
  {
    countryCode: "DE",
    marketplaceId: "A1PA6795UKMFR9",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EUR",
    name: "Germany",
    marketplaceName: "Amazon.de"
  },
  {
    countryCode: "FR",
    marketplaceId: "A13V1IB3VIYZZH",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EUR",
    name: "France",
    marketplaceName: "Amazon.fr"
  },
  {
    countryCode: "IT",
    marketplaceId: "APJ6JRA9NG5V4",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EUR",
    name: "Italy",
    marketplaceName: "Amazon.it"
  },
  {
    countryCode: "ES",
    marketplaceId: "A1RKKUPIHCS9HS",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EUR",
    name: "Spain",
    marketplaceName: "Amazon.es"
  },
  {
    countryCode: "NL",
    marketplaceId: "A1805IZSGTT6HS",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EUR",
    name: "Netherlands",
    marketplaceName: "Amazon.nl"
  },
  {
    countryCode: "SE",
    marketplaceId: "A2NODRKZP88ZB9",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "SEK",
    name: "Sweden",
    marketplaceName: "Amazon.se"
  },
  {
    countryCode: "PL",
    marketplaceId: "A1C3SOZRARQ6R3",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "PLN",
    name: "Poland",
    marketplaceName: "Amazon.pl"
  },
  {
    countryCode: "BE",
    marketplaceId: "AMEN7PMS3EDWL",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EUR",
    name: "Belgium",
    marketplaceName: "Amazon.com.be"
  },
  {
    countryCode: "TR",
    marketplaceId: "A33AVAJ2PDY3EV",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "TRY",
    name: "Turkey",
    marketplaceName: "Amazon.com.tr"
  },
  {
    countryCode: "EG",
    marketplaceId: "ARBP9OOSHTCHU",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "EGP",
    name: "Egypt",
    marketplaceName: "Amazon.eg"
  },
  {
    countryCode: "SA",
    marketplaceId: "A17E79C6D8DWNP",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "SAR",
    name: "Saudi Arabia",
    marketplaceName: "Amazon.sa"
  },
  {
    countryCode: "AE",
    marketplaceId: "A2VIGQ35RCS4UG",
    region: "eu",
    endpoint: "https://sellingpartnerapi-eu.amazon.com",
    currency: "AED",
    name: "United Arab Emirates",
    marketplaceName: "Amazon.ae"
  },

  // 远东地区
  {
    countryCode: "JP",
    marketplaceId: "A1VC38T7YXB528",
    region: "fe",
    endpoint: "https://sellingpartnerapi-fe.amazon.com",
    currency: "JPY",
    name: "Japan",
    marketplaceName: "Amazon.co.jp"
  },
  {
    countryCode: "SG",
    marketplaceId: "A19VAU5U5O7RUS",
    region: "fe",
    endpoint: "https://sellingpartnerapi-fe.amazon.com",
    currency: "SGD",
    name: "Singapore",
    marketplaceName: "Amazon.sg"
  },
  {
    countryCode: "AU",
    marketplaceId: "A39IBJ37TRP1C6",
    region: "fe",
    endpoint: "https://sellingpartnerapi-fe.amazon.com",
    currency: "AUD",
    name: "Australia",
    marketplaceName: "Amazon.com.au"
  },
  {
    countryCode: "IN",
    marketplaceId: "A21TJRUUN4KGV",
    region: "fe",
    endpoint: "https://sellingpartnerapi-fe.amazon.com",
    currency: "INR",
    name: "India",
    marketplaceName: "Amazon.in"
  }
];

/**
 * 根据国家代码查询亚马逊市场信息
 * @param {string} countryCode 国家代码字符串
 * @returns {Object|null} 匹配的第一个有效市场信息，未找到时返回 null
 */
function getAmazonMarketplace(countryCode) {
  if (!countryCode) return null;

  const marketplace = AMAZON_MARKETPLACES.find(m => m.countryCode === countryCode);
  if (marketplace) return marketplace;

  return null;
}

/**
 * 根据国家代码数组查询多个亚马逊市场信息
 * @param {string[]} countryCodes 国家代码数组
 * @returns {Array} 匹配的市场信息数组
 */
function getAmazonMarketplaces(countryCodes) {
  if (!countryCodes?.length) return [];  // 更安全的空值检查

  const validCountryCodes = new Set(
    countryCodes
      .map(code => code.trim().toUpperCase())
      .filter(code => code.length === 2)
  );

  return AMAZON_MARKETPLACES.filter(marketplace =>
    validCountryCodes.has(marketplace.countryCode)
  );
}

/**
 * 根据 marketplaceId 查询亚马逊市场信息
 * @param {string} marketplaceId marketplaceId 字符串
 * @returns {Object|null} 匹配的第一个有效市场信息，未找到时返回 null
 */
function getAmazonMarketplaceByMarketplaceId(marketplaceId) {
  if (!marketplaceId) return null;
  const marketplace = AMAZON_MARKETPLACES.find(m => m.marketplaceId === marketplaceId || m.countryCode === marketplaceId);
  return marketplace || null;
}

export {
  AMAZON_MARKETPLACES,
  getAmazonMarketplace,
  getAmazonMarketplaces,
  getAmazonMarketplaceByMarketplaceId
};
