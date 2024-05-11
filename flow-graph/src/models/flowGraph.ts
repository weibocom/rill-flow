import { Edge, RillNode } from "./node";
import { DagInfo } from './dagInfo';
import {OptEnum} from "./enums/optEnum";
import { DagBaseInfo } from './dag/dagBaseInfo';
import { BaseTask } from "./task/baseTask";
import { DagExecutionInfo } from "./dag/dagExecutionInfo";

export interface FlowGraph {
  init(operation: OptEnum, container: HTMLElement, dagInfo: DagInfo): void;

  addNode(node: RillNode);

  addNodeByPrototype(prototypeId: string, event: MouseEvent);

  removeNode(nodeId: string): void;

  removeEdge(rillEdge: Edge): void;

  getNode(id: string): RillNode;

  updateNodeData(nodeId: string, task: BaseTask);

  getNodes(): RillNode[];

  getNodeByTaskName(taskName: string): RillNode;

  getDagBaseInfo(): DagBaseInfo;

  getDagExecutionInfo(): DagExecutionInfo;

  updateDagBaseInfo(dagBaseInfo: DagBaseInfo);

  updateNodeTaskData(nodeId: string, nodeData: object);

  updateNodeTaskMappingInfos(nodeId: string, taskMappingInfos: object);

  updateNodeTaskOutput(nodeId: string, outputSchema: object);

  updateNodeTaskTitle(nodeId: string, title: string);

  containNode(nodeName: string);

  toJSON();

  toYaml(): string;

  reset();

  addEdge(rillEdge: Edge);
}
