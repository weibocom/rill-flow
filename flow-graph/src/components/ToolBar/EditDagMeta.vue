<template>
    <a-modal v-model:visible="open" class="modal" title="详情" width="70%" :footer="null">
      <a-card>
        一键导入:
        <a-switch v-model:checked="quickCreate" checked-children="开" un-checked-children="关">
        </a-switch>
      </a-card>
      <a-card>
          <FormProvider :form="form" class="form" v-if="!quickCreate" >
          <SchemaField :schema="schema" :scope="{ formStep }" />
          <FormConsumer>
            <template #default>
              <FormButtonGroup>
                <a-button
                  :disabled="!formStep.allowBack"
                  @click="
                    () => {
                      formStep.back();
                    }
                  "
                >
                  上一步
                </a-button>
                <a-button
                  :disabled="!formStep.allowNext"
                  @click="
                    () => {
                      formStep.next();
                      handleOk(false);
                    }
                  "
                >
                  下一步
                </a-button>
                <Submit :disabled="formStep.allowNext" @submit="handleOk">保存</Submit>
              </FormButtonGroup>
            </template>
          </FormConsumer>
        </FormProvider>
          <Codemirror v-if="quickCreate" v-model:value="yamlCode" :options="yamlOptions" border />
          <a-button v-if="quickCreate" @click="save">提交</a-button>
      </a-card>
    </a-modal>
</template>

<script lang="ts" setup>
  import { createSchemaField, FormConsumer, FormProvider } from '@formily/vue';
  import { ref } from 'vue';
  import { createForm } from '@formily/core';
  import {
    ArrayItems,
    DatePicker,
    FormButtonGroup,
    FormCollapse,
    FormItem,
    FormStep,
    Input,
    InputNumber,
    PreviewText,
    Select,
    Space,
    Submit,
    Switch,
  } from '@formily/antdv-x3';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { getSaveFormSchema } from '@/src/components/ToolBar/data';
  import Codemirror from "codemirror-editor-vue3";
  import {DagSubmitParams} from "@/src/api/types";
  import {submitDagInfo} from "@/src/api/flow";
  import {message} from "ant-design-vue";
  import yaml from 'js-yaml';

  const open = ref<boolean>(false);
  const quickCreate = ref<boolean>(false);
  const yamlCode = ref('');
  const yamlOptions = ref({
    mode: 'yaml',
  });

  const form = ref();
  form.value = createForm();

  const formStep = ref();
  formStep.value = FormStep.createFormStep();

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

  const schema = ref({});

  Channel.eventListener(CustomEventTypeEnum.TOOL_BAR_EDIT_INPUT_SCHEMA, () => {
    quickCreate.value = false;
    schema.value = getSaveFormSchema();
    const flowGraphStore = useFlowStoreWithOut();
    form.value.setFormState((state) => {
      state.values['workspace'] = flowGraphStore.getFlowGraph().getDagBaseInfo().workspace;
      state.values['dagName'] = flowGraphStore.getFlowGraph().getDagBaseInfo().dagName;
      state.values['alias'] = flowGraphStore.getFlowGraph().getDagBaseInfo().alias;
      state.values['inputSchema'] = flowGraphStore.getFlowGraph().getDagBaseInfo().inputSchema;
      state.values['type'] = flowGraphStore.getFlowGraph().getDagBaseInfo().type;
    });
    yamlCode.value = flowGraphStore.getFlowGraph().toYaml()
    open.value = true;
  });

  const handleOk = (close = true) => {
    const flowGraphStore = useFlowStoreWithOut();
    const dagBaseInfo = flowGraphStore.getFlowGraph().getDagBaseInfo();
    dagBaseInfo.workspace = form.value.getFormState().values['workspace'];
    dagBaseInfo.dagName = form.value.getFormState().values['dagName'];
    dagBaseInfo.alias = form.value.getFormState().values['alias'];
    dagBaseInfo.inputSchema = form.value.getFormState().values['inputSchema'];
    dagBaseInfo.type = form.value.getFormState().values['type'];
    flowGraphStore.getFlowGraph().updateDagBaseInfo(dagBaseInfo);
    open.value = !close;
  };

  function save() {
    const flowGraphStore = useFlowStoreWithOut();
    const dagBaseInfo = flowGraphStore.getFlowGraph().getDagBaseInfo();
    flowGraphStore.getFlowGraph().updateDagBaseInfo(dagBaseInfo);
    const dagJson = yaml.load(yamlCode.value)
    if (dagJson?.workspace === undefined || dagJson?.dagName === undefined || dagJson?.alias === undefined) {
      message.error('参数异常');
      return;
    }
    const dagSubmitParams = new DagSubmitParams(
        dagJson.workspace,
        dagJson.dagName,
        dagJson.alias,
    );
    submitDagInfo(
        flowGraphStore.flowParams.submitDagUrl,
        dagSubmitParams,
        yamlCode.value,
    ).then((res) => {
      if (res?.error) {
        message.error('提交异常:' + res?.error, 10);
      } else {
        message.success('DagId:' + res.descriptor_id, 2);
        Channel.dispatchEvent(CustomEventTypeEnum.REFRESH_DAG_GRAPH, res.descriptor_id);
      }
      open.value = false;
    });

  }
</script>

<style scoped xml:lang="scss">

  .box {
    padding: 100px;
  }
  .ant-modal-content {
    margin: 100px;
  }
</style>
