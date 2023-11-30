import {
  BasicPageParams,
  BasicFetchResult,
  FlowInstanceResult,
  InstanceDetailParams
} from '/@/api/model/baseModel';
/**
 * @description: Request list interface parameters
 */
export type DemoParams = BasicPageParams;
export type InstanceDetailParam = InstanceDetailParams;

export interface DemoListItem {
  id: string;
  beginTime: string;
  endTime: string;
  address: string;
  name: string;
  no: number;
  business_id: string;
  feature_id: string;
  status: string;
  submit_time: number;
  execution_id: string;
}

export interface FlowInstanceITEM {
  id: string;
  business_id: string;
  // execution_id: string;
  feature_id: string;
  status: string;
  submit_time: number;
}

/**
 * @description: Request list return value
 */
export type DemoListGetResultModel = BasicFetchResult<DemoListItem>;
export type FlowInstanceResultModel = FlowInstanceResult<FlowInstanceITEM>;
