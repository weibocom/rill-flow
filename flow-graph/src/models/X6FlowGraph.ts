import {Addon, Graph, Model} from '@antv/x6';
import {FlowGraph} from './flowGraph';
import {Edge, RillNode} from './node';
import {DagInfo, DagTask} from './dagInfo';
import {buildUUID} from '../uitils/uuid';
import {registerVueNode} from '../components/shape/registerNode';
import {registerGraphListener} from '../listener/registerGraphListener';
import {DagBaseInfo} from './dag/dagBaseInfo';
import {DagExecutionInfo} from './dag/dagExecutionInfo';
import {useFlowStoreWithOut} from '../store/modules/flowGraphStore';
import {GraphCellRenderService} from '../common/graphCellRenderService';
import {BaseTask} from './task/baseTask';
import {DagreLayout} from '@antv/layout';
import {NodePrototype} from "./nodeTemplate";
import {getNodeCategoryByNumber, NodeCategory} from "./enums/nodeCategory"; // umd模式下， const { GridLayout } = window.layout
import yaml from 'js-yaml';
import {graphConfig} from "../config/graphConfig";
import {OptEnum} from "./enums/optEnum";
import {
  convertObjectToMappingParametersMap,
  getFieldsSchemaData, isArray, isObject,
  updateInputMappings, updateOutputMappings
} from "../common/flowService";

export class X6FlowGraph implements FlowGraph {
  private nodes: RillNode[] = new Array<RillNode>();
  private nodeMap: Map<string, RillNode> = new Map<string, RillNode>();
  private graph: Graph;
  private dagBaseInfo: DagBaseInfo = new DagBaseInfo();
  private dagExecutionInfo: DagExecutionInfo = new DagExecutionInfo();
  private taskNames: Set<string> = new Set<string>();

  public init(opt: OptEnum, container, dagInfo: DagInfo) {
    this.reset();
    if (opt !== OptEnum.CREATE) {
      this.fillDagBaseInfo(dagInfo);
      this.fillDagExecutionInfo(dagInfo);
      this.initFlowGraph(dagInfo);
    }
    this.initX6Graph(container, opt);
  }

  private initFlowGraph(dagInfo: DagInfo) {
    const tasks = dagInfo.tasks;
    const taskNodeMap = new Map<string, RillNode>();
    const nodeTaskMap = new Map<String, DagTask>();
    for (const taskName in tasks) {
      const task = tasks[taskName].task;
      task.status = tasks[taskName]?.status;
      task.containsSub = tasks[taskName]?.contains_sub;
      task.invokeMsg = tasks[taskName]?.invoke_msg;
      this.taskNames.add(taskName);
      const node = new RillNode();

      node.id = buildUUID();
      node.task = task;
      if (node.nodePrototypeId === undefined) {
        node.nodePrototypeId = task.taskTemplateId;
      }
      if (node.nodePrototypeId === null || node.nodePrototypeId === undefined) {
        node.nodePrototypeId = task.category;
      }
      node.incomingEdges = new Array<Edge>();
      node.outgoingEdges = new Array<Edge>();
      taskNodeMap.set(taskName, node);
      nodeTaskMap.set(node.id, tasks[taskName]);
      this.nodes.push(node);
      this.nodeMap.set(node.id, node);
    }

    // fill edges
    this.generateEdges(nodeTaskMap, taskNodeMap);
  }

  private generateTaskName(nodePrototype:NodePrototype): string {

    let taskNumber = 1;
    let taskName;
    if (getNodeCategoryByNumber(nodePrototype.node_category) === NodeCategory.TEMPLATE_NODE) {
      const taskYaml = nodePrototype.template.task_yaml;
      const fields = yaml.load(taskYaml);
      taskName = fields?.name === undefined ? fields?.category : fields?.name;
    } else {
      taskName = nodePrototype.meta_data.category;
    }
    taskName = this.normalizeTaskName(taskName);
    if (!this.taskNames.has(taskName)) {
      return taskName;
    }
    while (this.taskNames.has(taskName + taskNumber)) {
      taskNumber += 1;
    }
    return taskName + taskNumber;
  }

  private normalizeTaskName(input: string): string {
    return input.split('').reduce((acc, char, index, array) => {
      if (char === '-' || char === '_') {
        // 如果下一个字符存在，将其转换为大写
        if (array[index + 1]) {
          acc += array[index + 1].toUpperCase();
        }
        // 跳过下一个字符，因为我们已经将其转换为大写
        return acc;
      } else if (index !== 0 && (array[index - 1] === '-' || array[index - 1] === '_')) {
        // 如果当前字符的前一个字符是 '-' 或 '_'，跳过当前字符
        return acc;
      } else {
        // 否则，直接添加当前字符
        acc += char;
        return acc;
      }
    }, '');
  }

  public containNode(nodeName: string) {
    return this.taskNames.has(nodeName);
  }

  public toJSON() {
    const dagJson = {
      workspace: this.dagBaseInfo.workspace,
      dagName: this.dagBaseInfo.dagName,
      version: this.dagBaseInfo.version,
      alias: this.dagBaseInfo.alias,
      type: 'flow',
      inputSchema: JSON.stringify(this.dagBaseInfo.inputSchema),
      tasks: undefined,
    };

    this.nodes.forEach(node => {
      if (dagJson.tasks === undefined) {
        dagJson.tasks = [];
      }
      dagJson.tasks.push(node.task);
    });
    return dagJson;
  }

  public toYaml() {
    if (this.toJSON().dagName ===  undefined) {
      return null;
    }
    return yaml.dump(this.toJSON());
  }

  public reset() {
    this.nodes = new Array<RillNode>();
    this.nodeMap = new Map<string, RillNode>();
    if (this.graph) {
      this.graph.clearCells();
    }
    this.dagBaseInfo = new DagBaseInfo();
    this.dagExecutionInfo = new DagExecutionInfo();
    this.taskNames = new Set<string>();
  }

  public removeEdge(rillEdge: Edge) {
    const fromNode: RillNode = this.getNode(rillEdge.sourceNodeId);
    const toNode: RillNode = this.getNode(rillEdge.targetNodeId);
    if (fromNode === undefined || toNode === undefined) {
      return;
    }
    const outgoingEdges = fromNode.outgoingEdges;
    const incomingEdges = toNode.incomingEdges;

    fromNode.outgoingEdges = outgoingEdges.filter((edge) => {
      return edge.sourcePortId !== rillEdge.sourcePortId && edge.targetPortId !== rillEdge.targetPortId;
    });
    toNode.incomingEdges = incomingEdges.filter((edge) => {
      return edge.sourcePortId !== rillEdge.sourcePortId && edge.targetPortId !== rillEdge.targetPortId;
    });
    const fromNodeNextSets = new Set(fromNode.task.next.split(','));
    fromNodeNextSets.delete(toNode.task.name);
    fromNode.task.next = Array.from(fromNodeNextSets).join(',');
  }

  private initX6Graph(container, opt: OptEnum) {
    graphConfig['container'] = container;
    this.graph = new Graph(graphConfig);
    registerVueNode();
    registerGraphListener(this.graph, opt);
    if (opt !== OptEnum.CREATE) {
      this.initGraphCells();
    }
    this.graph.resize(document.body.offsetWidth, document.body.offsetHeight);
  }

  public addEdge(rillEdge: Edge) {
    const fromNode: RillNode = this.getNode(rillEdge.sourceNodeId);
    const toNode: RillNode = this.getNode(rillEdge.targetNodeId);
    if (fromNode.outgoingEdges === undefined) {
      fromNode.outgoingEdges = [];
    }
    if (toNode.incomingEdges === undefined) {
      toNode.incomingEdges = [];
    }
    fromNode.outgoingEdges.push(rillEdge);
    toNode.incomingEdges.push(rillEdge);
    const next = fromNode.task.next;
    if (next === undefined || next === '') {
      fromNode.task.next = toNode.task.name;
    } else {
      const newNext = next.split(',').filter((it) => {
        return it !== toNode.task.name;
      });
      newNext.push(toNode.task.name);
      fromNode.task.next = newNext.join(',');
    }
  }

  private initGraphCells() {
    const flowGraphStore = useFlowStoreWithOut();

    // 遍历nodes 生成 node和edges
    const data: Model.FromJSONData = {
      nodes: [],
      edges: [],
    };
    this.nodes.forEach((node) => {
      const nodePrototype =
        node.task.taskTemplateId === undefined ? node.task.category : node.task.taskTemplateId;
      const icon = flowGraphStore.getNodePrototypeRegistry().getNodePrototype(nodePrototype.toString())?.icon;
      GraphCellRenderService.render(node, icon, data);
    });

    const dagreLayout = new DagreLayout({
      type: 'dagre',
      rankdir: 'TB',
      ranksep: 15,
      nodesep: 50,
      controlPoints: true,
      nodeSize: 100,
    });
    const model = dagreLayout.layout(data);
    this.graph.fromJSON(model);
  }

  public addNode(node: RillNode) {
    if (this.nodeMap.has(node.id)) {
      return;
    }
    this.buildNodeTask(node);
    this.nodes.push(node);
    this.nodeMap.set(node.id, node);
    this.taskNames.add(node.task.name);
  }

  private buildNodeTask(node: RillNode) {
    const flowGraphStore = useFlowStoreWithOut();
    const nodePrototype = flowGraphStore.getNodePrototypeRegistry().getNodePrototype(node.nodePrototypeId);
    const fields = getFieldsSchemaData(node, nodePrototype);
    for (const field in fields) {
      node.task[field] = fields[field];
    }
    const nodeCategory = getNodeCategoryByNumber(nodePrototype.node_category);
    if (nodeCategory === NodeCategory.TEMPLATE_NODE) {
      node.task.taskTemplateId = node.nodePrototypeId;
    }
  }

  public addNodeByPrototype(prototypeId: string, event: MouseEvent) {
    const node = new RillNode();
    node.id = buildUUID();
    node.task = new BaseTask();
    node.nodePrototypeId = prototypeId;
    node.incomingEdges = new Array<Edge>();
    node.outgoingEdges = new Array<Edge>();

    const flowGraphStore = useFlowStoreWithOut();
    const nodePrototype = flowGraphStore.getNodePrototypeRegistry().getNodePrototype(prototypeId);
    const icon = nodePrototype.icon;
    node.task.name = this.generateTaskName(nodePrototype);
    this.buildNodeTask(node);
    const nodeConfig = GraphCellRenderService.render(node, icon)[0];
    const graphNode = this.createNode(nodeConfig);

    const data = {
      dnd: {},
      freeze: false,
    };
    const dnd = new Addon.Dnd({
      target: this.graph,
      validateNode() {
        return true;
      },
    });
    dnd.start(graphNode, event);
    return;
  }


  private createNode(cellConfig: {}) {
    if (cellConfig.shape === 'edge') {
      return this.graph.createEdge({
        ...cellConfig,
      });
    } else if (cellConfig.shape === 'groupNode') {
      return this.graph.createNode({
        ...cellConfig,
      });
    } else if (cellConfig.shape === 'vue-shape') {
      return this.graph.createNode({
        ...cellConfig,
      });
    }

    return null;
  }

  public removeNode(nodeId: string) {
    this.nodes = this.nodes.filter((n) => n.id !== nodeId);
    this.nodeMap.delete(nodeId);
  }

  public getNode(id: string): RillNode {
    return this.nodeMap.get(id);
  }

  public getNodes() {
    return this.nodes;
  }

  public getNodeByTaskName(taskName: string): RillNode {
    for (const node of this.nodes) {
      if (node.task.name === taskName) {
        return node;
      }
    }
  }

  private generateEdges(nodeTaskMap: Map<String, DagTask>, taskNodeMap: Map<string, RillNode>) {
    this.nodes.forEach((node) => {
      const task = nodeTaskMap.get(node.id);
      const links = task?.next;
      if (links == null) {
        return;
      }
      links.forEach((taskName) => {
        const linkNode = taskNodeMap.get(taskName);
        if (linkNode == null) {
          console.log('the next task(' + taskName + ') of task(' + node.task.name + ') not exist ');
          return;
        }
        const outgoingEdge = new Edge();
        outgoingEdge.sourceNodeId = node.id;
        outgoingEdge.sourcePortId = node.id + '-down';
        outgoingEdge.targetNodeId = linkNode.id;
        outgoingEdge.targetPortId = linkNode.id + '-up';
        node.outgoingEdges.push(outgoingEdge);
        const incomingEdge = new Edge();
        incomingEdge.sourceNodeId = node.id;
        incomingEdge.sourcePortId = node.id + '-down';
        incomingEdge.targetNodeId = linkNode.id;
        incomingEdge.targetPortId = linkNode.id + '-up';
        linkNode.incomingEdges.push(incomingEdge);
      });
    });
  }

  private fillDagExecutionInfo(dagInfo: DagInfo) {
    this.dagExecutionInfo.executionId = dagInfo.execution_id;
    this.dagExecutionInfo.dagStatus = dagInfo.dag_status;
    this.dagExecutionInfo.process = dagInfo.process;
    this.dagExecutionInfo.traceUrl = dagInfo.trace_url;
    this.dagExecutionInfo.dagInvokeMsg = dagInfo.dag_invoke_msg;
    this.dagExecutionInfo.context = dagInfo.context;
  }

  private fillDagBaseInfo(dagInfo: DagInfo) {
    this.dagBaseInfo.version = dagInfo.version;
    this.dagBaseInfo.workspace = dagInfo.workspace;
    this.dagBaseInfo.dagName = dagInfo.dagName;
    this.dagBaseInfo.inputSchema = this.parseInputSchema(dagInfo.inputSchema);
    this.dagBaseInfo.alias = dagInfo.alias;
  }

  public getDagBaseInfo(): DagBaseInfo {
    return this.dagBaseInfo;
  }

  public getDagExecutionInfo(): DagExecutionInfo {
    return this.dagExecutionInfo;
  }


  public updateDagBaseInfo(dagBaseInfo: DagBaseInfo) {
    this.dagBaseInfo = dagBaseInfo;
  }

  public updateNodeTaskData(nodeId: string, nodeData: object) {
    const node = this.getNode(nodeId);

    const flowGraphStore = useFlowStoreWithOut();
    const nodePrototypeRegistry = flowGraphStore.getNodePrototypeRegistry();
    const nodePrototype = nodePrototypeRegistry.getNodePrototype(node.nodePrototypeId.toString());

    if (node.task === undefined) {
      node.task = new BaseTask();
    }
    node.task.category = nodePrototype.meta_data.category;
    for (const key in nodeData) {
      if (isArray(nodeData[key]) && nodeData[key].length === 0
        || isObject(nodeData[key]) && Object.keys(nodeData[key]).length === 0
        || key === 'name') {
        continue;
      }
      node.task[key] = nodeData[key];
    }
    if (nodeData['name'] !== undefined && nodeData['name'] !== node.task.name) {
      this.handleTaskNameChange(node, nodeData['name']);
      node.task.name = nodeData['name'];
    }
    const label = (node.task.title === undefined || node.task.title === '') ? node.task.name : node.task.title;
    this.graph.getCellById(nodeId).prop('label', label);
  }

  private handleTaskNameChange(node: RillNode, newName: string) {
    const oldTaskName = node.task.name;
    if (oldTaskName === newName || oldTaskName === undefined) {
      return;
    }
    this.taskNames.delete(oldTaskName);
    this.taskNames.add(newName);
    this.modifyInputMappingsWhenTaskNameChange(oldTaskName, newName);
    this.modifyOutputMappingWhenTaskNameChange(node, oldTaskName, newName);
    this.modifyNextOfInputNodes(node, oldTaskName, newName);
  }

  private modifyNextOfInputNodes(node: RillNode, oldTaskName: string, newName: string) {
    if (node.incomingEdges === undefined || node.incomingEdges.length === 0) {
      return;
    }
    node.incomingEdges.forEach(edge => {
      const source = this.getNode(edge.sourceNodeId);
      const nextArray = source.task.next.split(',');
      nextArray.push(newName);
      source.task.next = nextArray.filter(next => next !== oldTaskName).join(',');
    });
  }

  private modifyOutputMappingWhenTaskNameChange(node: RillNode, oldTaskName: string, newName: string) {
    if (node.task.outputMappings === undefined) {
      return;
    }
    for (const outputMapping of node.task.outputMappings) {
      const target = outputMapping.target;
      if (target === undefined || !target.startsWith('$.context.' + oldTaskName + '.')) {
        continue;
      }
      outputMapping.target = '$.context.' + newName + target.substring('$.context.'.length, oldTaskName.length);
    }
  }

  private modifyInputMappingsWhenTaskNameChange(oldTaskName: string, newTaskName: string) {
    for (const node of this.nodes) {
      if (node === undefined || node.task === undefined) {
        continue;
      }
      const inputMappings = node.task.inputMappings;
      if (inputMappings === undefined) {
        continue;
      }
      for (const inputMapping of inputMappings) {
        const source = String(inputMapping.source);
        if (source === undefined || !source.startsWith('$.context.' + oldTaskName + '.')) {
          continue;
        }
        inputMapping.source = '$.context.' + newTaskName + source.substring('$.context.'.length, oldTaskName.length);
      }
    }
  }

  public updateNodeTaskTitle(nodeId: string, title: string) {
    const currentNode = this.getNode(nodeId);
    currentNode.task.title = title;
    this.graph.getCellById(nodeId).prop('label', title);
  }
  public updateNodeTaskOutput(nodeId: string, outputSchema: object){
    const currentNode = this.getNode(nodeId);
    currentNode.task.outputSchema = outputSchema;
  }
  public updateNodeTaskMappingInfos(nodeId: string, mappingParametersObject: object, oldTaskMappingInfos: object) {
    // 1. 通过 nodeId 获取当前 node
    const currentNode = this.getNode(nodeId);
    const flowGraphStore = useFlowStoreWithOut();
    const nodePrototype = flowGraphStore.getNodePrototypeRegistry().getNodePrototype(currentNode.nodePrototypeId.toString());


    // 2. 将 mappingParametersObject 转换为 Map<string, MappingParameters> 的 parametersMap
    const parametersMap = convertObjectToMappingParametersMap(nodePrototype?.template?.schema, mappingParametersObject);
    const oldParametersMap = convertObjectToMappingParametersMap(nodePrototype?.template?.schema, oldTaskMappingInfos);
    console.log('parametersMap', parametersMap, currentNode.nodePrototypeId, nodePrototype, mappingParametersObject)

    // 3. 更新节点的inputMappings。
    const oldSources: Map<string, Set<string>> = new Map<string, Set<string>>();
    const outputTasks: Map<string, Set<string>> = new Map<string, Set<string>>();
    updateInputMappings(currentNode, parametersMap, oldSources, oldParametersMap, outputTasks);

    // 4. 更新 outputTask 的 outputMappings
    updateOutputMappings(oldSources, outputTasks, parametersMap);
  }

  private parseInputSchema(schema: string) {
    if (schema === undefined) {
      return undefined;
    }
    return JSON.parse(schema);
  }
}
