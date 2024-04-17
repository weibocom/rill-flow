import {BaseTask} from './baseTask'
import {Synchronization} from "./synchronization";
import {IterationMapping} from "./iterationMapping";

export class ForeachTask extends  BaseTask {
  synchronization: Synchronization;
  iterationMapping: IterationMapping;
}
