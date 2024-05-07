import { BaseTask } from './baseTask';
import {Retry} from "./retry";

export class FunctionTask extends BaseTask {
  resourceName: string;
  resourceProtocol: string;
  requestType: string;
  pattern: string;
  tolerance: boolean;
  successConditions: string[];
  failConditions: string[];
  retry: Retry;
}
