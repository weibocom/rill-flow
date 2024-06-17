import {queryDagInfo, queryTemplateNodes} from '../api/flow';
import {GetFlowParams} from '../api/types';
import {useFlowStoreWithOut} from '../store/modules/flowGraphStore';
import {FlowGraph} from "../models/flowGraph";
import {DagInfo} from "../models/dagInfo";
import {getNodeCategoryByNumber, NodeCategory} from "../models/enums/nodeCategory";
import {TreeData} from "../models/graph/treeData";
import {MappingParameters} from "../models/mappingParameters";
import {
  MappingEditTypeEnum
} from "../models/enums/mappingEditTypeEnum";
import {Mapping} from "../models/task/mapping";
import {RillNode} from "../models/node";
import {FlowParams} from "../models/flowParams";
import {OptEnum} from "../models/enums/optEnum";
import {Channel} from "./transmit";
import {CustomEventTypeEnum} from "./enums";
import {NodePrototype} from "../models/nodeTemplate";
import yaml from 'js-yaml';
import { useI18nStoreWithOut } from "../store/modules/i18nStore";
import { InputSchemaHandlerFactory } from "./inputSchemaStyleHandler";
import { InputSchemaTypeEnum } from "../models/enums/InputSchemaTypeEnum";
// 保存节点分组信息
export function saveNodeGroups(queryTemplateNodesUrls: string[]) {
  const flowGraphStore = useFlowStoreWithOut();
  const nodePrototypeRegistry = flowGraphStore.getNodePrototypeRegistry();
  if (queryTemplateNodesUrls === undefined) {
    return;
  }
  queryTemplateNodesUrls.forEach((url) => {
    queryTemplateNodes(url).then((res) => {
      const urlSearchParams = new URLSearchParams(url)
      res.forEach((node) => {
        node.id = urlSearchParams.get('source') ? urlSearchParams.get('source') + node.id : node.id;
        nodePrototypeRegistry.add(node);
      });
    });
  });
}

// 保存DAG信息
export function initFlowGraph(flowParams: FlowParams, container) {
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();

  if (flowParams.opt === OptEnum.CREATE) {
    flowGraph.init(OptEnum.CREATE, container, undefined);
    // 发 CREATE 事件
    Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_EDIT_INPUT_SCHEMA, 'init');
  } else {
    queryDagInfo<DagInfo>(getQueryUrlByOpt(flowParams), new GetFlowParams(flowParams.id)).then((res) => {
      flowGraph.init(flowParams.opt, container, res);
    });
  }
}

export function isArray(obj: any) {
  return Object.prototype.toString.call(obj) === '[object Array]';
}

export function isObject(obj: any) {
  return Object.prototype.toString.call(obj) === '[object Object]';
}

/**
 * 通过 nodeId 获取所有的前驱节点的 nodeId 的列表，包括自身 nodeId
 * @param nodeId
 */
export function getPredecessorNodeIds(nodeId: string) {
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();
  const currentNode = flowGraph.getNode(nodeId);
  if (!currentNode) {
    throw new Error(`nodeId: ${nodeId} 不存在`);
  }

  const nodeIds = new Set<string>();
  nodeIds.add(nodeId);
  // 1. 根据节点id获取该节点的所有 incomingEdges
  const incomingEdges = currentNode.incomingEdges;
  if (incomingEdges === undefined || incomingEdges === null || incomingEdges.length === 0) {
    return nodeIds;
  }
  // 2. 遍历所有的 incomingEdges，获取所有的前驱节点的 nodeId
  for (const edge of incomingEdges) {
    const nodeId = edge.sourceNodeId;
    if (nodeIds.has(nodeId)) {
      continue;
    }
    const incomingNodeIds = getPredecessorNodeIds(nodeId);
    incomingNodeIds.forEach((id) => {
      nodeIds.add(id);
    });
  }
  return nodeIds;
}

// 通过 nodeId 获取所有前驱节点的 output 信息
export function getReferences(nodeId: string): TreeData[] {
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();

  // 1. 获取所有前驱 nodeId 的列表
  const incomingNodeIds = getPredecessorNodeIds(nodeId);
  incomingNodeIds.delete(nodeId);

  // 2. 获取 inputSchema 的 treeData 数据
  const inputSchema = flowGraph.getDagBaseInfo().inputSchema;
  const treeDataList = convertInputSchemaToTreeData(inputSchema);

  // 3. 获取所有前驱节点的 output 信息对应的 treeData 数据
  incomingNodeIds.forEach((nodeId) => {
    const node = flowGraph.getNode(nodeId);
    if (!node) {
      console.error(`nodeId: ${nodeId} 对应的节点不存在，请检查节点是否存在`);
      return;
    }
    const nodePrototype = flowGraphStore.getNodePrototypeRegistry().getNodePrototype(node.nodePrototypeId.toString());
    const nodeCategory = getNodeCategoryByNumber(nodePrototype.node_category);
    if (!node || nodeCategory != NodeCategory.TEMPLATE_NODE) {
      return;
    }
    const taskName = node.task.name;
    const nodePath = '$.' + taskName;
    const output = node.task.outputSchema === undefined ? JSON.parse(nodePrototype.template.output): node.task.outputSchema;

    const treeData = convertSchemaToTreeData(output, nodePath);
    let title = node.task.title === undefined ? node.task.name : node.task.title;
    treeDataList.push({ title: title, value: nodePath, children: treeData });
  });
  return treeDataList;
}

/**
 * 将 inputSchema 转换为 treeData
 * @param inputSchema
 */
export function convertInputSchemaToTreeData(inputSchema): TreeData[] {
  if (inputSchema === null || inputSchema === undefined) {
    return [];
  }
  const treeData = [];
  const inputSchemaData = new TreeData();
  const { t } = useI18nStoreWithOut().getI18n().global;
  inputSchemaData.title = t('context');
  inputSchemaData.value = '$.context';
  inputSchemaData.children = [];
  const inputSchemaDataChildren = [];
  for (const dataKey in inputSchema) {
    const treeData = new TreeData();
    const title = (inputSchema[dataKey].desc === '' || inputSchema[dataKey].desc === undefined) ? inputSchema[dataKey].name: inputSchema[dataKey].desc;
    treeData.title = title + '【' + inputSchema[dataKey].type+ '】';
    treeData.value = '$.context.' + inputSchema[dataKey].name;
    treeData.children = [];
    inputSchemaDataChildren.push(treeData);
  }

  inputSchemaData.children = inputSchemaDataChildren;
  treeData.push(inputSchemaData);
  return treeData;
}

/**
 * 将 json schema 转换成 treeData
 * @param schema
 * @param currentPath
 */
export function convertSchemaToTreeData(schema, currentPath = '$'): TreeData[] {
  const treeData = [];
  if (schema.type === 'object') {
    if (schema?.properties === undefined) {
      return treeData;
    }
    for (const [key, value] of Object.entries(schema.properties)) {
      currentPath += '.' + key;
      const node = new TreeData();
      node.title = key + '【' + value?.type + '】';
      node.value = currentPath;
      node.children = convertSchemaToTreeData(value, currentPath);
      treeData.push(node);
    }
  } else if (schema.type === 'array') {
    const arrayItems = schema.items;

    if (arrayItems && arrayItems.type === 'object') {
      currentPath += '.*';
      return convertSchemaToTreeData(arrayItems, currentPath);
    }
  }

  return treeData;
}

function getQueryUrlByOpt(flowParams: FlowParams) {
  switch (flowParams.opt) {
    case OptEnum.EDIT:
      return flowParams.queryDagUrl;
    case OptEnum.DISPLAY:
      return flowParams.queryExecutionUrl;
    default:
      return undefined;
  }
}

export function updateInputMappings(currentNode: RillNode, parameters: Map<string, MappingParameters>, oldSources: Map<string, Set<string>>, oldParameters: Map<string, MappingParameters>, outputTasks: Map<string, Set<string>>) {
  if (currentNode.task.inputMappings === undefined) {
    currentNode.task.inputMappings = [];
  }
  // 1. 构建模版默认inputMappings
  const newMappings = [];
  for (const mapping of currentNode.task.inputMappings) {
    if (parameters.has(mapping.target)) {
      const sourceInfos = mapping.source.toString().split('.');
      if (sourceInfos.length < 3) {
        continue;
      }
      let sources = oldSources.get(sourceInfos[2]);
      if (sources === undefined) {
        sources = new Set<string>();
      }
      sources.add(mapping.source);
      oldSources.set(sourceInfos[2], sources);
    } else if (!oldParameters.has(mapping.target)) {
      // 模版节点中默认的参数需要再次添加到mapping中
      newMappings.push(mapping);
    }
  }
  // 2. 更新节点参数对应的inputMappings
  currentNode.task.inputMappings = newMappings;
  for (const [key, params] of parameters) {
    const mapping: Mapping = getMappingByMappingParameters(key, params, outputTasks);
    if (mapping === undefined) {
      continue;
    }
    currentNode.task.inputMappings.push(mapping);
  }
}

export function getFieldsSchemaData(node: RillNode, nodePrototype: NodePrototype): object {
  let fields = {};
  if (getNodeCategoryByNumber(nodePrototype.node_category) == NodeCategory.TEMPLATE_NODE && nodePrototype.template !== undefined) {
    const taskYaml = nodePrototype.template.task_yaml;
    fields = yaml.load(taskYaml);
  }
  if (node.task === undefined) {
    return fields;
  }
  for (const field of Object.keys(node.task)) {
    fields[field] = node.task[field];
  }
  return fields;
}

function removeOldOutputMappingsFromSourceNode(oldSources: Map<string, Set<string>>) {
  if (oldSources.size === 0) {
    return;
  }
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();
  const cannotBeDeleted = new Set<string>();
  for (const node of flowGraph.getNodes()) {
    if (node.task.inputMappings === undefined) {
      continue;
    }
    for (const mapping of node.task.inputMappings) {
      cannotBeDeleted.add(mapping.source);
    }
  }
  for (const [taskName, sources] of oldSources) {
    const outputNode: RillNode = flowGraph.getNodeByTaskName(taskName);
    if (outputNode === undefined || outputNode.task.outputMappings === undefined) {
      continue;
    }
    outputNode.task.outputMappings = outputNode.task.outputMappings.filter(outputMapping => {
      return !sources.has(outputMapping.target) || cannotBeDeleted.has(outputMapping.target);
    })
    if (outputNode.task.outputMappings.length == 0) {
      outputNode.task.outputMappings = undefined;
    }
  }
}

function generateOutputMappingsForOneTask(taskName: string, mappingParametersKeys: Set<string>, mappingParameters: Map<string, MappingParameters>) {
  // 1. 通过 outputNodes 的 key 获取对应的 outputNode 和 outputTask
  const flowGraphStore = useFlowStoreWithOut();
  const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();
  const outputNode: RillNode = flowGraph.getNodeByTaskName(taskName);
  if (outputNode === undefined) {
    return;
  }
  // 2. 通过 MappingParameters 拼装 outputMapping 的 target
  mappingParametersKeys.forEach(mappingParametersKey => {
    // 2.1 生成 outputMapping 的 target
    const mappingParameter: MappingParameters = mappingParameters.get(mappingParametersKey);
    const references = mappingParameter.reference.split('.');
    let target = '$.context.' + references[1];
    if (references.length >= 3) {
      for (let i = 2; i < references.length; i++) {
        target += '.' + references[i];
      }
    }

    // 2.2 根据 target，遍历 outputTask 的 outputMappings，删除已存在的 outputMapping
    if (outputNode.task.outputMappings === undefined) {
      outputNode.task.outputMappings = [];
    }

    outputNode.task.outputMappings = outputNode.task.outputMappings.filter(outputMapping => {
      return outputMapping.target !== target;
    });

    // 2.3 生成 outputMapping 的 source，生成 outputMapping
    let source = '$.output';
    if (references.length >= 3) {
      for (let i = 2; i < references.length; i++) {
        source += '.' + references[i];
      }
    }
    const outputMapping: Mapping = new Mapping();
    outputMapping.source = source;
    outputMapping.target = target;
    outputNode.task.outputMappings.push(outputMapping);
  });
}

/**
 * 更新依赖节点的 outputMappings
 * @param outputNodes 依赖节点, key 是 taskName，value 是 mappingParameters 的 key
 * @param mappingParameters 映射参数
 * @param oldSources 旧的 source 集合
 */
export function updateOutputMappings(oldSources: Map<string, Set<string>>,
                              outputNodes: Map<string, Set<string>>,
                              mappingParameters: Map<string, MappingParameters>) {
  // 1. 根据 oldSources 删除已废弃的 mapping
  removeOldOutputMappingsFromSourceNode(oldSources);

  // 2. 遍历 outputNodes 生成新的 mapping
  for (const [taskName, mappingParametersKeys] of outputNodes) {
    generateOutputMappingsForOneTask(taskName, mappingParametersKeys, mappingParameters);
  }
}

/**
 * 通过 MappingParameters 获取 Mapping 对象
 */
export function getMappingByMappingParameters(mappingParametersKey: string, mappingParameters: MappingParameters, outputTasks: Map<string, Set<string>>): Mapping {
  const mapping: Mapping = new Mapping();
  mapping.target = mappingParametersKey;
  // 1. 如果是 input 类型，直接返回
  if (mappingParameters.type === MappingEditTypeEnum.INPUT) {
    mapping.source = mappingParameters.input;
    return mapping;
  }
  // 2. 如果是 reference 类型，获取 source
  if (mappingParameters.reference === undefined || mappingParameters.reference === null || mappingParameters.reference === '') {
    return undefined;
  }
  const referenceInfos = mappingParameters.reference.split(".");
  let source = mappingParameters.reference;
  if (referenceInfos.length < 2) {
    return undefined;
  }
  const taskName: string = referenceInfos[1];
  if (taskName !== 'context') {
    let targets:Set<string> = outputTasks.get(taskName);
    if (targets === undefined) {
      targets = new Set<string>();
    }
    targets.add(mappingParametersKey);
    outputTasks.set(taskName, targets);
    // 不是从 inputSchema 中取数据，则需要在 $.{taskName} 之间加上 context => $.context.{taskName}
    source = '$.context.' + taskName;
    if (referenceInfos.length >= 3) {
      // 如果是 $.{taskName}.{fields} 这种格式，则需要在最后加上 fields => $.context.{taskName}.{fields}
      for (let i = 2; i < referenceInfos.length; i++) {
        source += '.' + referenceInfos[i];
      }
    }
  }
  mapping.source = source;
  return mapping;
}

/**
 * 通过 mappingParametersObject 获取 key 和 object
 */
export function convertObjectToMappingParametersMap(nodeSchema: string, obj: any, currentKey = ''): Map<string, MappingParameters> {
  let result = new Map<string, MappingParameters>();
  let nodeTemplateSchema = nodeSchema !== undefined ? JSON.parse(nodeSchema) : {};

  for (const key in obj) {
    let bizType = nodeTemplateSchema?.properties?.[key]?.bizType !== undefined ? nodeTemplateSchema?.properties?.[key]?.bizType : InputSchemaTypeEnum.NORMAL;
    if (Array.isArray(obj[key]) || typeof obj[key] === 'string') {
      const newKey = currentKey ? `${currentKey}.${key}` : key;
      let arrayToMapResult = InputSchemaHandlerFactory.getHandler(bizType).saveSchemaValueHandle(newKey, obj[key])
      result = new Map([...result, ...arrayToMapResult])
      continue;
    }

    if (typeof obj[key] === 'object' && obj[key] !== null
      && (obj[key]['attr'] === null || typeof obj[key]['attr'] !== 'string')) {
      const newKey = currentKey ? `${currentKey}.${key}` : key;
      const subResult = convertObjectToMappingParametersMap(nodeSchema, obj[key], newKey);
      subResult.forEach((value, subKey) => {
        result.set(subKey, value);
      });
    } else {
      const newKey = currentKey ? `${currentKey}.${key}` : key;
      let arrayToMapResult = InputSchemaHandlerFactory.getHandler(InputSchemaTypeEnum.REFERENCE).saveSchemaValueHandle(newKey, obj[key])
      result = new Map([...result, ...arrayToMapResult])
    }
  }

  return result;
}
