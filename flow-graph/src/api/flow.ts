import { DagSubmitParams, DagSubmitTaskParams, GetFlowParams } from "./types";
import instance from "./axios";
import { NodePrototype } from "../models/nodeTemplate";

export const queryDagInfo = async <T>(url: string, params: GetFlowParams) => {
  const response = await instance.get<T>(url, {
    params: params,
  });
  return response?.data;
};

export const queryTemplateNodes = async (url: string) => {
  return (await instance.get<NodePrototype[]>(url)).data;
};

export const submitDagInfo = async (url: string, params: DagSubmitParams, yamlData: string) => {
  return await instance.post(url, yamlData, {
    headers: {
      'Content-Type': 'text/plain',
    },
    params: params,
  });
};

export const submitDagTask = async (url: string, params: DagSubmitTaskParams, data) => {
  const response = await instance.post(url, data, {
    headers: {
      'Content-Type': 'application/json',
    },
    params: params,
  });
  return response;
};
