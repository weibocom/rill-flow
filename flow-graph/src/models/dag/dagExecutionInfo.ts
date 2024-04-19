import { DagStatusEnum } from '../enums/dagStatusEnum';
import { DagInvokeMessage } from '../dagInvokeMessage';

export class DagExecutionInfo {
  executionId: string;
  dagStatus: DagStatusEnum;
  process: number;
  traceUrl: string;
  dagInvokeMsg: DagInvokeMessage;
  context: Map<string, object>;
}

export class DagExecutionShowInfo {
  executionId: string;
  dagStatus: DagStatusEnum;
  process: number;
  traceUrl: string;
  startTime: string;
  endTime: string;
  msg: string;
  context: Map<string, object>;

  constructor(
    executionId: string,
    dagStatus: DagStatusEnum,
    process: number,
    traceUrl: string,
    startTime: string,
    endTime: string,
    msg: string,
    context: Map<string, object>,
  ) {
    this.executionId = executionId;
    this.dagStatus = dagStatus;
    this.process = process;
    this.traceUrl = traceUrl;
    this.startTime = startTime;
    this.endTime = endTime;
    this.msg = msg;
    this.context = context;
  }
}
