import { BaseTask } from './task/baseTask';
import { GraphNode } from './graphNode';
import {useFlowStoreWithOut} from "../store/modules/flowGraphStore";
import {FlowGraph} from "./flowGraph";

export class RillNode {
  id: string;
  task: BaseTask;
  nodePrototypeId: string;
  incomingEdges: Edge[];
  outgoingEdges: Edge[];
  graphNode: GraphNode;
}

export class Edge {
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
}

export function addEdge(edge: Edge): void {
  const {sourceNode, targetNode} = getNodesByEdge(edge);

  sourceNode.outgoingEdges.push(edge);
  targetNode.incomingEdges.push(edge);

}

export function addEdgeByNodeId(sourceNodeId: string, targetNodeId: string) {
  const edge = new Edge();
  edge.sourceNodeId = sourceNodeId;
  edge.sourcePortId = sourceNodeId + '-down';
  edge.targetNodeId = targetNodeId;
  edge.targetPortId = targetNodeId + '-up';
  addEdge(edge);
}

export function removeEdge(edge: Edge): void {
  const {sourceNode, targetNode} = getNodesByEdge(edge);

  // 删除 sourceNode.outgoingEdges 中，sourceNodeId 为 edge.sourceNodeId 的元素
  sourceNode.outgoingEdges = sourceNode?.outgoingEdges.filter(
    (item) => item.sourceNodeId !== edge.sourceNodeId
  );
  // 删除 targetNode.incomingEdges 中，targetNodeId 为 edge.targetNodeId 的元素
  targetNode.incomingEdges = targetNode?.incomingEdges.filter(
    (item) => item.targetNodeId !== edge.targetNodeId
  );

}

export function removeEdgeByNodeId(sourceNodeId: string, targetNodeId: string) {
  const edge = new Edge();
  edge.sourceNodeId = sourceNodeId;
  edge.sourcePortId = sourceNodeId + '-down';
  edge.targetNodeId = targetNodeId;
  edge.targetPortId = targetNodeId + '-up';
  removeEdge(edge);
}

function getNodesByEdge(edge: Edge) {
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();

  const sourceNode = flowGraph.getNode(edge.sourceNodeId);
  const targetNode = flowGraph.getNode(edge.targetNodeId);

  if (sourceNode === null || sourceNode === undefined) {
    throw new Error(`sourceNodeId: ${edge.sourceNodeId} is not exist`);
  }
  if (targetNode === null || targetNode === undefined) {
    throw new Error(`targetNodeId: ${edge.targetNodeId} is not exist`);
  }
  return {sourceNode, targetNode};
}
