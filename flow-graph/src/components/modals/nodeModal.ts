import {useFlowStoreWithOut} from "../../store/modules/flowGraphStore";
import {FlowGraph} from "../../models/flowGraph";
import {getNodeCategoryByNumber, NodeCategory} from "../../models/enums/nodeCategory";

export function renderNodeEditModal(cellId: string) {
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();
  const nodePrototypeRegistry = flowGraphStore.getNodePrototypeRegistry();

  const node = flowGraph.getNode(cellId);
  if (node === null || node === undefined) {
    throw new Error("node is null or undefined: " + cellId);
  }
  const task = node.task;
  if (task === null || task === undefined) {
    throw new Error("task is null or undefined: " + cellId);
  }
  let nodePrototypeId = task.category;
  if (task.taskTemplateId !== null && task.taskTemplateId !== undefined) {
    nodePrototypeId = task.taskTemplateId.toString();
  }

  const nodePrototype = nodePrototypeRegistry.getNodePrototype(nodePrototypeId);
  if (nodePrototype === null || nodePrototype === undefined) {
    throw new Error("nodePrototype is null or undefined: " + nodePrototypeId);
  }

  let inputSchema:string = null;
  const nodeCategory = getNodeCategoryByNumber(nodePrototype.node_category);
  if (nodeCategory === NodeCategory.TEMPLATE_NODE) {
    inputSchema = nodePrototype.template.schema;
  }

  const fields = nodePrototype.meta_data.fields;
}

