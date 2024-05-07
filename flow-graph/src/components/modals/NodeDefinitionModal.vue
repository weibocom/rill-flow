<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    title="节点编辑"
    @ok="handleOk"
    width="70%"
  >
    <a-card>
      <a-tabs v-model:activeKey="activeKey">
        <a-tab-pane key="1" tab="基础设置" v-if="nodeCategory === NodeCategory.TEMPLATE_NODE">
          <a-card title="Input">
            <VueForm v-model="jsonSchemaFormData" :schema="inputJsonSchema" :formProps="formProps">
              <div slot-scope="{ jsonSchemaFormData }"></div>
            </VueForm>
          </a-card>
          <a-card title="Output">
            <a-tree
              v-model:expandedKeys="expandedKeys"
              v-model:selectedKeys="selectedKeys"
              :tree-data="treeData"
            />
          </a-card>
        </a-tab-pane>
        <a-tab-pane key="2" tab="高级设置">
          <a-card title="">
            <VueForm v-model="fieldsSchemaData" :schema="fieldsSchema" :formProps="formProps">
              <div slot-scope="{ fieldsSchemaData }"></div>
            </VueForm>
          </a-card>
        </a-tab-pane>
      </a-tabs>
    </a-card>
  </a-modal>
</template>

<script lang="ts" setup>
  import { reactive, ref, shallowRef, toRaw } from 'vue';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import VueForm from '@lljj/vue3-form-ant';
  import type { TreeProps } from 'ant-design-vue';
  import { useFlowStoreWithOut } from '@/src/store/modules/flowGraphStore';
  import { convertSchemaToTreeData } from '@/src/common/outputToTree';
  import { cloneDeep } from 'lodash-es';
  import { replaceUIWidget } from '../../common/replaceJsonSchemaConfig';
  import { getJsonByJsonPaths, getJsonPathByJsonSchema } from '../../common/transform';
  import { getReferences } from '../../common/flowService';
  import { RillNode } from '@/src/models/node';
  import yaml from 'js-yaml';
  import { NodePrototype } from '@/src/models/nodeTemplate';
  import { getNodeCategoryByNumber, NodeCategory } from '@/src/models/enums/nodeCategory';
  import { FlowGraph } from '@/src/models/flowGraph';
  import { Mapping } from '@/src/models/task/mapping';
  import { getOptEnumByOpt, OptEnum } from '../../models/enums/optEnum';

  const open = shallowRef<boolean>(false);
  const activeKey = ref('1');
  const nodeCategory = ref(NodeCategory.TEMPLATE_NODE);

  const expandedKeys = ref<string[]>([]);
  const selectedKeys = ref<string[]>([]);
  const treeData = ref<TreeProps['treeData']>([]);

  let jsonSchemaFormData = reactive({});
  const inputJsonSchema = ref({});
  const fieldsSchema = ref({});
  const fieldsSchemaData = ref({});
  const formProps = ref({});

  const nodeRef = ref(null);

  function getSchemaFormDataByReference(
    inputMapping: Mapping,
    flowGraph: FlowGraph,
    inputTargetParam: string,
  ): object {
    let isNodeTemplate = false;
    if (inputMapping.source.startsWith('$.context.')) {
      const maybeTaskName = inputMapping.source.split('.')[2];
      isNodeTemplate = flowGraph.containNode(maybeTaskName);
    }
    return {
      key: inputTargetParam,
      value: {
        attr: 'reference',
        reference: isNodeTemplate
          ? inputMapping.source.replace('.context', '')
          : inputMapping.source,
        input: '',
      },
    };
  }

  function renderInputSchemaForm(
    node: RillNode,
    nodePrototype: NodePrototype,
    flowGraph: FlowGraph,
  ) {
    if (nodeCategory.value !== NodeCategory.TEMPLATE_NODE) {
      activeKey.value = '2';
      return;
    } else {
      activeKey.value = '1';
    }

    // 1. 初始化 output 和 templateSchema
    const output = JSON.parse(nodePrototype.template.output);
    const outputTreeData = convertSchemaToTreeData(output);
    treeData.value = outputTreeData;
    const templateSchemaOri = JSON.parse(nodePrototype.template.schema);
    const templateSchema = cloneDeep(templateSchemaOri);

    // 通过 nodeId 获取references
    replaceUIWidget(templateSchema, getReferences(node.id));
    inputJsonSchema.value = templateSchema;
    formProps.value = {
      labelPosition: 'left',
      labelSuffix: '：',
    };

    // 2. 初始化templateSchema中的值jsonSchemaFormData
    const inputMappingMap: Map<string, Mapping> = new Map<string, Mapping>();
    for (const inputKey in node.task.inputMappings) {
      const inputMapping = node.task.inputMappings[inputKey];
      const mapKey = inputMapping.target.split('$.input.')[1];
      inputMappingMap.set(mapKey, inputMapping);
    }

    const schemaParamsList = getJsonPathByJsonSchema(templateSchemaOri.properties);
    let jsonSchemaFormDataList = [];
    for (const inputTargetParam of schemaParamsList) {
      const inputMapping = inputMappingMap.get(inputTargetParam);
      if (inputMapping === undefined) {
        continue;
      }
      const inputFormData = {
        key: inputTargetParam,
        value: {
          attr: 'input',
          input: inputMapping.source,
          reference: '',
        },
      };
      if (inputMapping?.source === undefined) {
        continue;
      }
      const formData = inputMapping.source.startsWith('$.')
        ? getSchemaFormDataByReference(inputMapping, flowGraph, inputTargetParam)
        : inputFormData;
      jsonSchemaFormDataList.push(formData);
    }
    jsonSchemaFormData = getJsonByJsonPaths(jsonSchemaFormDataList);
  }

  // 监听点击事件后弹modal
  function isOpenModel(opt: OptEnum) {
    return opt === OptEnum.CREATE || opt === OptEnum.EDIT;
  }

  Channel.eventListener(CustomEventTypeEnum.NODE_CLICK, (nodeCell) => {
    const flowGraphStore = useFlowStoreWithOut();
    nodeRef.value = nodeCell;
    if (!isOpenModel(getOptEnumByOpt(flowGraphStore.getFlowParams().opt))) {
      return;
    }

    const node = flowGraphStore.getFlowGraph().getNode(nodeCell.id);
    const nodePrototype = flowGraphStore
      .getNodePrototypeRegistry()
      .getNodePrototype(nodeCell.getData().nodePrototype + '');
    nodeCategory.value = getNodeCategoryByNumber(nodePrototype.node_category);
    // 1. 渲染基础配置表单
    renderInputSchemaForm(node, nodePrototype, flowGraphStore.getFlowGraph());

    // 2. 渲染并填充高级配置表单
    // 2.1 获取 fields 数据，渲染表单
    fieldsSchema.value = getFieldsSchema(nodePrototype);

    // 2.2 获取 fieldsSchemaData 数据，填充表单
    fieldsSchemaData.value = getFieldsSchemaData(node, nodePrototype);

    open.value = true;
  });

  function getFieldsSchema(nodePrototype: NodePrototype): object {
    const result = { type: 'object', required: [], properties: {} };
    const fields = nodePrototype.meta_data.fields;
    for (const field in fields) {
      const properties = fields[field];
      if (field === 'next') {
        continue;
      }
      if (properties.required !== undefined && properties.required) {
        result.required.push(field);
      }

      result.properties[field] = properties;
    }
    return result;
  }

  function getFieldsSchemaData(node: RillNode, nodePrototype: NodePrototype): object {
    let fields = {};
    if (
      getNodeCategoryByNumber(nodePrototype.node_category) == NodeCategory.TEMPLATE_NODE &&
      nodePrototype.template !== undefined
    ) {
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

  const handleOk = () => {
    const flowGraphStore = useFlowStoreWithOut();
    const flowGraph: FlowGraph = flowGraphStore.getFlowGraph();
    if (activeKey.value === '1') {
      // 基础模式
      flowGraph.updateNodeTaskMappingInfos(
        nodeRef.value.id,
        toRaw(jsonSchemaFormData),
      );
    } else {
      // 高级模式
      flowGraph.updateNodeTaskData(nodeRef.value.id, fieldsSchemaData.value);
    }
    open.value = false;
  };
</script>

<style scoped lang="less"></style>
