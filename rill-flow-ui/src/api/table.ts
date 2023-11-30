import {defHttp} from '@/utils/http/axios';
import {
  DemoParams,
  InstanceDetailParam
} from './demo/model/tableModel';

enum Api {
  INSTANCE_LIST = '/flow/bg/get_execution_ids.json?current=1',
  BUSINESS_LIST = '/flow/bg/get_business_options.json',
  FEATURE_LIST = '/flow/bg/manage/descriptor/get_feature.json',
  INSTANCE_DETAIL = '/flow/bg/get_execution.json',
  FLOW_GROUP = '/flow/bg/edit/dag_op_groups.json',
}

/**
 * @description: Get sample list value
 */

export const instanceListApi = (params: DemoParams) =>
  defHttp.get({
    url: Api.INSTANCE_LIST,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });


export const flowInstanceDetailApi = (params: InstanceDetailParam) =>
  defHttp.get({
    url: Api.INSTANCE_DETAIL,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });


export const flowGroupDetailApi = () =>
  defHttp.get({
    url: Api.FLOW_GROUP,
  });

export const getBusinessIdsApi = () =>
  defHttp.get({
    url: Api.BUSINESS_LIST,
  });

export const getFeatureIdsApi = (params) =>
  defHttp.get({
    url: Api.FEATURE_LIST,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

