import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';

interface Response<T = any> {
  success: boolean;
  data: T;
  code: number;
  msg?: string;
  [key: string]: any;
}

export class ResponseError<T = any> extends Error {
  msg: string;
  data: T | undefined;
  code: number | undefined;

  constructor(msg?: string, code?: number, data?: T) {
    const _message = msg ?? 'error.network';
    super(_message);
    this.name = 'ResponseError';

    this.msg = _message;
    this.data = data;
    this.code = code;
  }
}

interface Instance extends AxiosInstance {
  <T, R = Response<T>>(config: AxiosRequestConfig): Promise<R>;
  <T, R = Response<T>>(url: string, config?: AxiosRequestConfig): Promise<R>;
  request: <T, R = Response<T>>(config: AxiosRequestConfig) => Promise<R>;
  get: <T, R = Response<T>>(url: string, config?: AxiosRequestConfig) => Promise<R>;
  delete: <T, R = Response<T>>(url: string, config?: AxiosRequestConfig) => Promise<R>;
  head: <T, R = Response<T>>(url: string, config?: AxiosRequestConfig) => Promise<R>;
  post: <T, R = Response<T>>(url: string, data?: any, config?: AxiosRequestConfig) => Promise<R>;
  put: <T, R = Response<T>>(url: string, data?: any, config?: AxiosRequestConfig) => Promise<R>;
  patch: <T, R = Response<T>>(url: string, data?: any, config?: AxiosRequestConfig) => Promise<R>;
}

const instance: Instance = axios.create({
  timeout: 20000,
  baseURL: import.meta.env.VITE_REQUEST_DOMAIN,
});

instance.interceptors.request.use((config: AxiosRequestConfig) => {
  if (config.method === 'get' || config.method === 'post') {
    config.params = {
      ...config.params,
    };
  }
  // config.headers.post['Content-Type'] = 'application/json';
  return config;
});

instance.interceptors.response.use(
  async (res: AxiosResponse<Response>) => {
    // TOOD: 异常处理
    const { data: { data } } = res
    if (data === undefined) {
      console.log('instance.interceptors.response.', data, res.data);
      return await Promise.resolve(res.data);
    }

    return await Promise.resolve({ data })
  },
  async () => {
    return await Promise.reject(new ResponseError('网络繁忙，请稍候再试'));
  },
);

export default instance;
