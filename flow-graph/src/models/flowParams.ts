import { FlowApiEnum } from './enums/flowApiEnum';
import { getOptEnumByOpt, OptEnum } from './enums/optEnum';

export class FlowParams {
  id?: string;
  opt: OptEnum;
  // rillFlow 的基础 host，优先使用 url，url 为空时，使用 host
  flowApiHost: string;
  queryDagUrl: string;
  submitDagUrl: string;
  queryExecutionUrl: string;
  queryMetaNodesUrl: string;
  submitExecuteUrl: string;
  queryTemplateNodesUrls: string[];

  constructor(
    id: string,
    opt: string,
    flowApiHost: string,
    queryDagUrl: string,
    submitDagUrl: string,
    queryExecutionUrl: string,
    queryMetaNodesUrl: string,
    submitExecuteUrl: string,
    queryTemplateNodesUrls: string[],
  ) {
    this.id = id;
    this.opt = getOptEnumByOpt(opt);
    if (flowApiHost === undefined || flowApiHost === '') {
      return;
    }
    if (flowApiHost.startsWith('http')) {
      this.flowApiHost = flowApiHost;
    } else {
      this.flowApiHost = 'http://' + flowApiHost;
    }
    this.queryDagUrl =
      queryDagUrl == null || queryDagUrl == ''
        ? this.flowApiHost + FlowApiEnum.QUERY_DAG
        : queryDagUrl;
    this.submitDagUrl =
      submitDagUrl == null || submitDagUrl == ''
        ? this.flowApiHost + FlowApiEnum.SUBMIT_DAG
        : submitDagUrl;
    this.queryExecutionUrl =
      queryExecutionUrl == null || queryExecutionUrl == ''
        ? this.flowApiHost + FlowApiEnum.QUERY_EXECUTION
        : queryExecutionUrl;
    this.submitExecuteUrl =
      submitExecuteUrl == null || submitExecuteUrl == ''
        ? this.flowApiHost + FlowApiEnum.SUBMIT_EXECUTION
        : submitExecuteUrl;
    this.queryMetaNodesUrl =
      queryMetaNodesUrl == null || queryMetaNodesUrl == ''
        ? this.flowApiHost + FlowApiEnum.QUERY_META_NODES
        : queryMetaNodesUrl;
    if (queryTemplateNodesUrls == null || queryTemplateNodesUrls.length == 0) {
      this.queryTemplateNodesUrls = [this.flowApiHost + FlowApiEnum.QUERY_NODE_PROTOTYPES];
    } else {
      this.queryTemplateNodesUrls = queryTemplateNodesUrls;
    }
  }
}
