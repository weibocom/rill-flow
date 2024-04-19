import { BaseTask } from './task/baseTask';
import { DagStatusEnum } from './enums/dagStatusEnum';
import { DagInvokeMessage } from './dagInvokeMessage';

export class DagInfo {
  dagName: string;
  workspace: string;
  type: string;
  alias: string;
  version: string;
  inputSchema: string;
  execution_id: string;
  dag_status: DagStatusEnum;
  process: 100;
  trace_url: string;
  dag_invoke_msg: DagInvokeMessage;
  context: Map<string, object>;
  tasks: Map<string, DagTask>;
}

export class DagTask {
  next: string[];
  task: BaseTask;
}
