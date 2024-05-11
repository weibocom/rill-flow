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
          <a-card>
              <a-space>
                节点名称:
                <a-input v-model:value="nodeTitle"/>
              </a-space>
          </a-card>
          <a-card title="Input">
            <VueForm v-model="jsonSchemaFormData" :schema="inputJsonSchema" :formProps="formProps">
              <div slot-scope="{ jsonSchemaFormData }"></div>
            </VueForm>
          </a-card>
          <a-card title="Output">
            <a-card>
              编辑: <a-switch v-model:checked="editOutputSwitch" checked-children="开" un-checked-children="关"/>
              <a-tabs v-if="editOutputSwitch" v-model:activeKey="outputActiveKey">
              <a-tab-pane key="1" tab="基础模式">
                <FormProvider :form="outputForm" class="form"  >
                  <SchemaField :schema="outputSchema" />
                </FormProvider>

              </a-tab-pane>
              <a-tab-pane key="2" tab="高级模式">
                <Codemirror v-model:value="nodeOutputStr"
                            :options="codeOptions"
                            border
                            style="width: 100%; height: 300px"
                />
              </a-tab-pane>
            </a-tabs>
            </a-card>
            <a-card title="Output详情">
              <a-tree
                v-model:expandedKeys="expandedKeys"
                v-model:selectedKeys="selectedKeys"
                :tree-data="treeData"
              />
            </a-card>
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
import { reactive, ref, shallowRef, toRaw, watch } from "vue";
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
  import Codemirror from "codemirror-editor-vue3";
  import { getOutputSchema } from "@/src/components/ToolBar/data";
  import {
    ArrayItems,
    DatePicker,
    FormButtonGroup, FormCollapse,
    FormItem,
    FormStep,
    Input, InputNumber, PreviewText,
    Select,
    Space,
    Switch
  } from "@formily/antdv-x3";
  import { createSchemaField, FormProvider } from "@formily/vue";
  import { createForm } from "@formily/core";
  import { InputSchemaValueItem } from "@/src/models/inputSchema";

  const outputForm = ref();
  outputForm.value = createForm();
  const { SchemaField } = createSchemaField({
    components: {
      FormItem,
      FormStep,
      Input,
      Space,
      Select,
      DatePicker,
      ArrayItems,
      Switch,
      InputNumber,
      FormCollapse,
      PreviewText,
    },
  });

  const outputSchema = ref({});
  outputSchema.value = getOutputSchema()

  const open = shallowRef<boolean>(false);
  const activeKey = ref('1');
  const outputActiveKey = ref('1');
  const nodeCategory = ref(NodeCategory.TEMPLATE_NODE);
  const expandedKeys = ref<string[]>([]);
  const selectedKeys = ref<string[]>([]);
  const treeData = ref<TreeProps['treeData']>([]);
  const editOutputSwitch = ref<boolean>(false);
  const nodeTitle = ref('');
  let jsonSchemaFormData = reactive({});
  const codeOptions = ref({
    mode: 'application/json',
  });
  const inputJsonSchema = ref({});
  const fieldsSchema = ref({});
  const fieldsSchemaData = ref({});
  const formProps = ref({});
  const nodeOutputStr = ref("");
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
    let nodeOutput= node.task.outputSchema === undefined ? JSON.parse(nodePrototype.template.output): node.task.outputSchema;
    nodeOutputStr.value = JSON.stringify(nodeOutput,null,2);
    treeData.value = convertSchemaToTreeData(JSON.parse(nodeOutputStr.value));
    if (nodeOutput?.mode === 'simple') {
      let properties = nodeOutput.properties
      let outputSchemaList = []
      for (const key in properties) {
        console.log('nodeOutput item', key, properties[key])
        outputSchemaList.push({
          name: key,
          type: properties[key].type,
          desc: properties[key]?.title,
          required: properties[key]?.required
        })
      };
      outputForm.value.setFormState((state) => {
        state.values['outputSchema'] = outputSchemaList
      });
      console.log('outputSchemaList item', outputSchemaList, outputForm.value.getFormState().values)
    } else {
      outputForm.value.setFormState((state) => {
        state.values['outputSchema'] = []
      });
    }

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
    nodeTitle.value = node.task.title;
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
      const nodeSchema = JSON.parse(nodePrototype.template.schema)?.properties
      if (nodeSchema !== undefined && (nodeSchema[inputTargetParam]?.bizType === 'array-to-map')) {
        const properties = nodeSchema[inputTargetParam]?.items?.properties
        const arrayValue = []
        const arrayItemMap = {}
        for (const key of inputMappingMap.keys()){
          if (key.split('.')[0] === inputTargetParam) {
            const itemKey = key.split('.')[1]
            arrayItemMap[itemKey] = inputMappingMap.get(key)
            if (properties['value']?.bizType === 'none') {
              arrayValue.push({
                key: itemKey,
                value: inputMappingMap.get(key).source
              })
            } else {
              const formData = getJsonSchemaFormDataItem(itemKey, inputMappingMap.get(key), flowGraph)
              if (formData === null) {
                continue;
              }
              arrayValue.push(formData)
            }
          }
        }

        jsonSchemaFormDataList.push({
          key: inputTargetParam,
          value: arrayValue
        })
        continue;
      }
      if (inputMapping === undefined) {
        continue;
      }
      if (nodeSchema !== undefined && (nodeSchema[inputTargetParam]?.bizType === 'code' || nodeSchema[inputTargetParam]?.bizType === 'none')) {

        if (nodeSchema[inputTargetParam]?.oneOf) {
          const select = nodeSchema[inputTargetParam]?.oneOf
        }
        jsonSchemaFormDataList.push({
          key: inputTargetParam,
          value: inputMapping.source
        })
        continue;
      }
      const formData = getJsonSchemaFormDataItem(inputTargetParam, inputMapping, flowGraph)
      if (formData === null) {
        continue;
      }
      jsonSchemaFormDataList.push(formData);
    }
    jsonSchemaFormData = getJsonByJsonPaths(jsonSchemaFormDataList);
  }

  function getJsonSchemaFormDataItem(key: string,inputMapping:Mapping, flowGraph: FlowGraph) {
    const inputFormData = {
      key: key,
      value: {
        attr: 'input',
        input: inputMapping.source,
        reference: '',
      },
    };
    if (inputMapping?.source === undefined) {
      return null;
    }
    const formData = inputMapping.source.toString().startsWith('$.')
      ? getSchemaFormDataByReference(inputMapping, flowGraph, key)
      : inputFormData;
    return formData;
  }

  // 监听点击事件后弹modal
  function isOpenModel(opt: OptEnum) {
    return opt === OptEnum.CREATE || opt === OptEnum.EDIT;
  }

  Channel.eventListener(CustomEventTypeEnum.NODE_CLICK, (nodeCell) => {
    const flowGraphStore = useFlowStoreWithOut();
    reset();

    nodeRef.value = nodeCell;
    if (!isOpenModel(getOptEnumByOpt(flowGraphStore.getFlowParams().opt))) {
      open.value = false;
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
      flowGraph.updateNodeTaskTitle(nodeRef.value.id,  nodeTitle.value)

      // 基础模式
      flowGraph.updateNodeTaskMappingInfos(
        nodeRef.value.id,
        toRaw(jsonSchemaFormData),
      );

      if (editOutputSwitch.value) {
        let outputSchema = {}
        if (outputActiveKey.value === '1') {
          outputSchema = formValuesToJsonSchema(outputForm.value.getFormState().values['outputSchema'])
        } else {
          outputSchema = JSON.parse(nodeOutputStr.value)
        }
        flowGraph.updateNodeTaskOutput(nodeRef.value.id, outputSchema)
      }
    } else {
      // 高级模式
      flowGraph.updateNodeTaskData(nodeRef.value.id, fieldsSchemaData.value);
    }
    open.value = false;
    reset();
  };

  function formValuesToJsonSchema(formValues: Array<InputSchemaValueItem>) {
    const schema = {
      type: 'object',
      properties: {},
      mode: 'simple'
    }

    if (formValues === undefined) {
      return schema;
    }
    console.log('formValues', formValues)
    for (const i in formValues) {

      schema.properties[formValues[i].name] = {
        'required': formValues[i].required,
        'type': formValues[i].type,
        'title': formValues[i].desc,
      }
      console.log('formValues', i,formValues[i])
    }
    return schema;
  }

  function reset() {
    editOutputSwitch.value = false;
    outputActiveKey.value = '1';
    outputForm.value.setFormState((state) => {
      state.values['outputSchema'] = []
    });
  }

</script>

<style scoped lang="less"></style>
