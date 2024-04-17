import { defHttp } from '@/utils/http/axios';
import { DemoParams, InstanceDetailParam } from './demo/model/tableModel';
import {templateEnable} from "@/views/flow-definition/node-templetes/tableData";

enum Api {
  INSTANCE_LIST = '/flow/bg/get_execution_ids.json?current=1',
  DEFINITION_LIST = '/flow/bg/get_descriptor_ids.json?current=1',
  TEMPLATE_LIST = '/flow/template/get_task_templates.json',
  UPDATE_TEMPLATE = '/flow/template/update_task_template.json',
  CREATE_TEMPLATE = '/flow/template/create_task_template.json',
  DISABLE_TEMPLATE = '/flow/template/disable_task_template.json',
  ENABLE_TEMPLATE = '/flow/template/enable_task_template.json',
  BUSINESS_LIST = '/flow/bg/get_business_options.json',
  FEATURE_LIST = '/flow/bg/manage/descriptor/get_feature.json',
  INSTANCE_DETAIL = '/flow/bg/get_execution.json',
  DEFINITION_DETAIL = '/flow/bg/get_descriptor.json',
  FLOW_GROUP = '/flow/template/get_task_templates.json',
  FLOW_VERSIONS = '/flow/bg/manage/descriptor/get_version.json',
  FLOW_DETAIL = '/flow/bg/manage/descriptor/get_descriptor.json',
  FLOW_SUBMIT = '/flow/bg/manage/descriptor/add_descriptor.json',
  FLOW_EXECUTE = '/flow/submit.json',
}

/**
 * @description: Get sample list value
 */

export const instanceListApi = (params: any) =>
  defHttp.get({
    url: Api.INSTANCE_LIST,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const definitionListApi = (params: any) =>
  defHttp.get({
    url: Api.DEFINITION_LIST,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const templateListApi = (params: any) => {
  params.enable = templateEnable.value
  return defHttp.get({
    url: Api.TEMPLATE_LIST,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });
}

export const updateTemplateApi = (params: any) =>
  defHttp.post({
    url: Api.UPDATE_TEMPLATE,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const createTemplateApi = (params: any) =>
  defHttp.post({
    url: Api.CREATE_TEMPLATE,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const enableTemplateApi = (id: number) =>
  defHttp.post({
    url: Api.ENABLE_TEMPLATE + "?id=" + id,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const disableTemplateApi = (id: number) =>
  defHttp.post({
    url: Api.DISABLE_TEMPLATE + "?id=" + id,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const flowDefinitionDetailApi = (params: InstanceDetailParam) =>
  defHttp.get({
    url: Api.DEFINITION_DETAIL,
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

export const flowGroupDetailApi = (params) =>
  defHttp.get({
    url: Api.FLOW_GROUP,
    params,
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

export const getFlowVersionsApi = (params) =>
  defHttp.get({
    url: Api.FLOW_VERSIONS,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const getFlowDetailApi = (params) =>
  defHttp.get({
    url: Api.FLOW_DETAIL,
    params,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
    },
  });

export const getFlowSubmitApi = (params, yamlData) =>
  defHttp.post({
    url: Api.FLOW_SUBMIT,
    params: params,
    data: yamlData,
    joinParamsToUrl: true,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
      'Content-Type': 'text/plain',
    },
  });

export const getFlowExecuteApi = (params, data) =>
  defHttp.post({
    url: Api.FLOW_EXECUTE,
    params: params,
    data: data,
    joinParamsToUrl: true,
    headers: {
      // @ts-ignore
      ignoreCancelToken: true,
      'Content-Type': 'application/json',
    },
  });
