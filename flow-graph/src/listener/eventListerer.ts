import { Channel } from '../common/transmit';
import { CustomEventTypeEnum } from '../common/enums';
import { FlowParams } from '../models/flowParams';
import { OptEnum } from '../models/enums/optEnum';
import { initFlowGraph } from '../common/flowService';
import { Edge, RillNode } from '../models/node';
import { BaseTask } from '../models/task/baseTask';
import { useFlowStoreWithOut } from '../store/modules/flowGraphStore';

export function registerEventListener(container) {
  const flowGraphStore = useFlowStoreWithOut();

  Channel.eventListener(CustomEventTypeEnum.REFRESH_DAG_GRAPH, (id) => {
    const flowParams: FlowParams = flowGraphStore.getFlowParams();
    flowParams.id = id;
    flowParams.opt = OptEnum.EDIT;
    flowGraphStore.setFlowParams(flowParams);
    flowGraphStore.getFlowGraph().reset();
    initFlowGraph(flowParams, container);
  });

  Channel.eventListener(CustomEventTypeEnum.SHOW_EXECUTION_RESULT, (id) => {
    const flowParams: FlowParams = flowGraphStore.getFlowParams();
    flowParams.id = id;
    flowParams.opt = OptEnum.DISPLAY;
    flowGraphStore.setFlowParams(flowParams);
    flowGraphStore.getFlowGraph().reset();
    initFlowGraph(flowParams, container);
  });

  Channel.eventListener(CustomEventTypeEnum.EDGE_ADD, (rillEdge) => {
    flowGraphStore.getFlowGraph().addEdge(rillEdge);
  });

  Channel.eventListener(CustomEventTypeEnum.EDGE_REMOVE, (rillEdge) => {
    flowGraphStore.getFlowGraph().removeEdge(rillEdge);
  });

  Channel.eventListener(CustomEventTypeEnum.NODE_REMOVE, (node) => {
    flowGraphStore.getFlowGraph().removeNode(node.id);
  });

  Channel.eventListener(CustomEventTypeEnum.NODE_ADD, (node) => {
    const rillNode = new RillNode();
    rillNode.id = node.id;
    rillNode.task = new BaseTask();
    rillNode.task.name = node.getData().name;
    rillNode.nodePrototypeId = node.getData().nodePrototype;
    node.incomingEdges = new Array<Edge>();
    node.outgoingEdges = new Array<Edge>();
    flowGraphStore.getFlowGraph().addNode(rillNode);
  });
}
