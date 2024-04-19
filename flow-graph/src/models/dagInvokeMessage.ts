import {ExecutionInfo} from "./executionInfo";
import {InvokeTimeInfo} from "./invokeTimeInfo";

export class DagInvokeMessage {
  dag_execution_routes: ExecutionInfo[];
  invoke_time_infos: InvokeTimeInfo[];
  msg: string;
  input: object;
  output: object;
}
