<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    title="详情"
    width="70%"
    :footer="null"
  >
    <a-card>
      <FormProvider :form="form">
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
                  }
                "
              >
                下一步
              </a-button>
              <Submit :disabled="formStep.allowNext" @submit="handleOk">提交</Submit>
            </FormButtonGroup>
          </template>
        </FormConsumer>
      </FormProvider>
    </a-card>
  </a-modal>
</template>

<script lang="ts" setup>
  import { createSchemaField, FormConsumer, FormProvider } from '@formily/vue';
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
    Select,
    Space,
    Submit,
    Switch,
    PreviewText,
  } from '@formily/antdv-x3';
  import { ref } from 'vue';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { getSaveFormSchema } from './data';
  import { submitDagInfo } from '@/src/api/flow';
  import { DagSubmitParams } from '@/src/api/types';
  import { message } from 'ant-design-vue';

  const open = ref<boolean>(false);

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

  Channel.eventListener(CustomEventTypeEnum.TOOL_BAR_SAVE_DAG, () => {
    schema.value = getSaveFormSchema();
    const flowGraphStore = useFlowStoreWithOut();
    form.value.setFormState((state) => {
      state.values['workspace'] = flowGraphStore.getFlowGraph().getDagBaseInfo().workspace;
      state.values['dagName'] = flowGraphStore.getFlowGraph().getDagBaseInfo().dagName;
      state.values['alias'] = flowGraphStore.getFlowGraph().getDagBaseInfo().alias;
      state.values['inputSchema'] = flowGraphStore.getFlowGraph().getDagBaseInfo().inputSchema;
      state.values['type'] = flowGraphStore.getFlowGraph().getDagBaseInfo().type;
    });
    open.value = true;
  });

  const handleOk = () => {
    const flowGraphStore = useFlowStoreWithOut();
    const dagBaseInfo = flowGraphStore.getFlowGraph().getDagBaseInfo();
    dagBaseInfo.workspace = form.value.getFormState().values['workspace'];
    dagBaseInfo.dagName = form.value.getFormState().values['dagName'];
    dagBaseInfo.alias = form.value.getFormState().values['alias'];
    dagBaseInfo.inputSchema = form.value.getFormState().values['inputSchema'];
    flowGraphStore.getFlowGraph().updateDagBaseInfo(dagBaseInfo);

    const dagSubmitParams = new DagSubmitParams(
      flowGraphStore.getFlowGraph().getDagBaseInfo().workspace,
      flowGraphStore.getFlowGraph().getDagBaseInfo().dagName,
      flowGraphStore.getFlowGraph().getDagBaseInfo().alias,
    );
    submitDagInfo(
      flowGraphStore.flowParams.submitDagUrl,
      dagSubmitParams,
      flowGraphStore.getFlowGraph().toYaml(),
    ).then((res) => {
      if (res?.error) {
        message.error('提交异常:' + res?.error, 10);
      } else {
        message.success('DagId:' + res.descriptor_id, 2);
        Channel.dispatchEvent(CustomEventTypeEnum.REFRESH_DAG_GRAPH, res.descriptor_id);
      }
      open.value = false;
    });
  };
</script>

<style scoped xml:lang="scss"></style>
