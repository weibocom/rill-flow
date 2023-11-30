export interface BasicPageParams {
  page: number;
  pageSize: number;
}

export interface InstanceDetailParams {
  id: string;
}

export interface InstanceDetailResult {
  id: string;
}

export interface BasicFetchResult<T> {
  items: T[];
  total: number;
}

export interface FlowInstanceResult<T> {
  items: T[];
  total: number;
}
