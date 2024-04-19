export class GetFlowParams {
  id: string;

  constructor(id: string) {
    this.id = id;
  }
}

export class FlowInfo {
  dagName: string;
  alias: string;
  type: string;
  version: string;
  workspace: string;
  tasks: Object;
  inputSchema: Object;
}

export interface BaseResponse<T = any> {
  data: T;
  message: string;
  success: boolean;
}

export class DagSubmitParams {
  business_id: string;
  feature_name: string;
  alias: string;

  constructor(business_id: string, feature_name: string, alias: string) {
    this.business_id = business_id;
    this.feature_name = feature_name;
    this.alias = alias;
  }
}
export class DagSubmitTaskParams {
  descriptor_id: string;

  constructor(descriptor_id: string) {
    this.descriptor_id = descriptor_id;
  }
}
