/**
 * 领星ERP全局错误码说明和处理
 */
const ErrorCodesMap = {
  // 认证相关错误
  '2001001': {
    code: '2001001',
    message: 'appId不存在',
    description: 'appId不存在，检查值有效性',
    action: '检查app_key是否正确配置'
  },
  '2001002': {
    code: '2001002',
    message: 'appSecret不正确',
    description: 'appSecret不正确，检查值有效性',
    action: '检查app_secret是否正确配置'
  },
  '2001003': {
    code: '2001003',
    message: 'access_token不存在或已过期',
    description: 'token不存在或者已经过期，可刷新token重试',
    action: '自动刷新token后重试',
    shouldRetry: true,
    shouldRefreshToken: true
  },
  '2001004': {
    code: '2001004',
    message: 'API未授权',
    description: '请求的api未授权，联系领星相关工作人员确认',
    action: '联系领星相关工作人员确认API授权'
  },
  '2001005': {
    code: '2001005',
    message: 'access_token不正确',
    description: 'access_token不正确，检查值有效性',
    action: '重新获取access_token'
  },
  '2001006': {
    code: '2001006',
    message: '接口签名不正确',
    description: '接口签名不正确，校验生成签名正确性',
    action: '检查签名生成逻辑和URL编码处理'
  },
  '2001007': {
    code: '2001007',
    message: '签名已过期',
    description: '签名已经过期，可重新发起请求',
    action: '重新生成签名后重试',
    shouldRetry: true
  },
  '2001008': {
    code: '2001008',
    message: 'refresh_token已过期',
    description: 'refresh_token过期，请重新获取',
    action: '重新获取access_token',
    shouldRefreshToken: true
  },
  '2001009': {
    code: '2001009',
    message: 'refresh_token无效',
    description: 'refresh_token值无效，检查值有效性或重新获取',
    action: '重新获取refresh_token'
  },
  
  // 请求参数错误
  '400': {
    code: '400',
    message: '参数有误',
    description: '请求参数有误，请检查参数格式和值',
    action: '检查请求参数是否正确，查看error_details获取详细信息'
  },
  '500': {
    code: '500',
    message: '内部错误',
    description: '服务器内部错误，可能是系统异常或配置问题',
    action: '查看error_details获取详细错误信息，如问题持续存在请联系技术支持'
  },
  '3001001': {
    code: '3001001',
    message: '缺少必传参数',
    description: 'access_token、sign、timestamp、app_key为必传参数',
    action: '检查请求参数是否缺失或拼接在url后格式是否正确'
  },
  '3001002': {
    code: '3001002',
    message: 'IP未加入白名单',
    description: 'ip未加入白名单，确认发起ip地址后在ERP内自行增加即可',
    action: '在ERP内添加IP白名单'
  },
  '3001008': {
    code: '3001008',
    message: '请求太频繁',
    description: '接口请求太频繁触发限流，适当下调接口请求频率',
    action: '降低请求频率，稍后重试',
    shouldRetry: true,
    retryAfter: 2000 // 2秒后重试
  }
};

/**
 * 根据错误码获取错误信息
 */
export function getErrorInfo(errorCode) {
  const code = String(errorCode);
  return ErrorCodesMap[code] || {
    code: code,
    message: '未知错误',
    description: '未知错误码',
    action: '联系技术支持'
  };
}

/**
 * 导出错误码映射（用于检查）
 */
export const ErrorCodes = ErrorCodesMap;

/**
 * 创建友好的错误对象
 */
export function createError(errorCode, originalError = null) {
  const errorInfo = getErrorInfo(errorCode);
  const error = new Error(errorInfo.message);
  error.code = errorInfo.code;
  error.description = errorInfo.description;
  error.action = errorInfo.action;
  error.shouldRetry = errorInfo.shouldRetry || false;
  error.shouldRefreshToken = errorInfo.shouldRefreshToken || false;
  error.retryAfter = errorInfo.retryAfter;
  
  if (originalError) {
    error.originalError = originalError;
    error.stack = originalError.stack;
  }
  
  return error;
}

