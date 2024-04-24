import {Mapping} from "./mapping";
import { DagInvokeMessage } from "../dagInvokeMessage";

export class BaseTask {
  name: string;
  title: string;
  category: string;
  taskTemplateId: number;
  next: string;
  inputMappings: Mapping[];
  outputMappings: Mapping[];
  status: string;
  containsSub: boolean;
  invokeMsg: DagInvokeMessage;
  outputSchema: object;
}
