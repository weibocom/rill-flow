export class FlowParams {
  id?: string;
  opt: string;
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
    this.opt = opt;
    this.flowApiHost = flowApiHost;
    this.queryDagUrl = queryDagUrl;
    this.submitDagUrl = submitDagUrl;
    this.submitExecuteUrl = submitExecuteUrl;
    this.queryExecutionUrl = queryExecutionUrl;
    this.queryMetaNodesUrl = queryMetaNodesUrl;
    this.queryTemplateNodesUrls = queryTemplateNodesUrls;
  }
}
