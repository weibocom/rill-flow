<template>
  <a-modal v-model:visible="open" title="测试任务" width="70%" @ok="handleOk()" okText="提交" cancelText="取消">
    <a-card>
      <FormProvider :form="form">
        <SchemaField :schema="schema" />
      </FormProvider>
    </a-card>
  </a-modal>
</template>

<script lang="ts" setup>
  import { createSchemaField, FormProvider } from '@formily/vue';
  import { ref } from 'vue';
  import { createForm } from '@formily/core';
  import {
    ArrayItems,
    DatePicker,
    FormCollapse,
    FormItem,
    Input,
    InputNumber,
    Select,
    Space,
    Switch,
  } from '@formily/antdv-x3';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { InputSchemaValueItem } from '../../models/inputSchema';
  import { submitDagTask } from '../../api/flow';
  import { DagSubmitTaskParams } from '../../api/types';
  import { message } from 'ant-design-vue';

  const open = ref<boolean>(false);
  const form = ref();
  form.value = createForm();

  const schema = ref({});
  schema.value = {
    type: 'object',
    properties: {},
  };
  const { SchemaField } = createSchemaField({
    components: {
      FormItem,
      Space,
      Input,
      Select,
      DatePicker,
      ArrayItems,
      Switch,
      InputNumber,
      FormCollapse,
    },
  });

  Channel.eventListener(CustomEventTypeEnum.TOOL_BAR_SUBMIT_DAG_TEST_RUN, () => {
    const flowGraphStore = useFlowStoreWithOut();
    const inputSchema = flowGraphStore.getFlowGraph().getDagBaseInfo().inputSchema === undefined ? [] : flowGraphStore.getFlowGraph().getDagBaseInfo().inputSchema;
    console.log('inputSchema', inputSchema, inputSchema === '{}')
    if (inputSchema !== {}) {
      inputSchema.forEach((item: InputSchemaValueItem) => {
        schema.value.properties[item.name] = buildSchema(item);
      });
    }
    open.value = true;
  });

  const handleOk = () => {
    const flowGraphStore = useFlowStoreWithOut();

    submitDagTask(
      flowGraphStore.getFlowParams().submitExecuteUrl,
      new DagSubmitTaskParams(flowGraphStore.getFlowParams().id),
      form.value.getFormState().values,
    ).then((res) => {
      if (res?.error) {
        message.error('提交异常:' + res?.error, 10);
      } else {
        message.success('ExecutionId:' + res.execution_id, 2);
        Channel.dispatchEvent(CustomEventTypeEnum.SHOW_EXECUTION_RESULT, res.execution_id);
      }
    });
    open.value = false;
  };

  function buildSchema(item: InputSchemaValueItem) {
    if (item.type === 'Number') {
      return {
        type: 'number',
        title: item.name,
        'x-decorator': 'FormItem',
        'x-component': 'InputNumber',
        required: item.required,
        'x-component-props': {
          style: {
            // width: '240px',
          },
        },
        'x-decorator-props': {
          tooltip: item.desc,
          wrapperAlign: 'right',
          labelAlign: 'left',
        },
      };
    } else if (item.type === 'Boolean') {
      return {
        type: 'boolean',
        title: item.name,
        'x-decorator': 'FormItem',
        'x-component': 'Switch',
        required: item.required,
        'x-component-props': {
          style: {
            // width: '240px',
          },
        },
        'x-decorator-props': {
          tooltip: item.desc,
          wrapperAlign: 'right',
          labelAlign: 'left',
        },
      };
    } else if (item.type === 'JSON') {
      return {
        type: 'string',
        title: '文本框',
        'x-decorator': 'FormItem',
        'x-component': 'Input.TextArea',
        'x-component-props': {
          style: {
            // width: 400,
          },
        },
        'x-decorator-props': {
          tooltip: item.desc,
          wrapperAlign: 'right',
          labelAlign: 'left',
        },
        required: true,
      };
    }
    return {
      type: 'string',
      title: item.name,
      required: item.required,
      'x-decorator': 'FormItem',
      'x-component': 'Input',
      'x-decorator-props': {
        tooltip: item.desc,
        wrapperAlign: 'right',
        labelAlign: 'left',
      },
    };
  }
</script>

<style scoped xml.lang="less"></style>
