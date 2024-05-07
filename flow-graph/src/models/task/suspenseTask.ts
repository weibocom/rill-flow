import {Mapping} from "./mapping";
import {BaseTask} from "./baseTask";

export class SuspenseTask extends BaseTask {
  inputMappings: Mapping[];
  outputMappings: Mapping[];
  conditions: string[];
}
