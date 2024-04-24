<template>
  <BasicDrawer
    v-bind="$attrs"
    @register="registerDrawer"
    showFooter
    :title="t('routes.flow.definitions.node_templates_detail.option.editTask_template')"
    width="400"
    @ok="handleSubmit"
  >
    <BasicForm @register="registerForm" autoFocusFirstItem:true :actionColOptions="{ span: 24 }" />

    <template #insertFooter>
      <a-button @click="handlePreviewSchema">{{
        t('routes.flow.definitions.node_templates_detail.option.preview_input')
      }}</a-button>
      <a-button @click="handlePreviewOutput">{{
        t('routes.flow.definitions.node_templates_detail.option.preview_output')
      }}</a-button>
    </template>
  </BasicDrawer>
  <SchemaPreviewModal @register="schemaPreviewModalRegister" :minHeight="100" />
</template>

<script lang="ts">
  import { defineComponent } from 'vue';
  import { BasicDrawer, useDrawerInner } from '/@/components/Drawer';
  import { useForm, BasicForm } from '@/components/Form';
  import { createTemplateApi, updateTemplateApi } from '@/api/table';
  import { useModal } from '@/components/Modal';
  import SchemaPreviewModal from '@/views/flow-definition/node-templetes/schemaPreviewModal.vue';
  import { templateSchema } from '@/views/flow-definition/node-templetes/tableData';
  import { useI18n } from '/@/hooks/web/useI18n';

  export default defineComponent({
    name: 'TaskTemplateEditDrawer',
    components: {
      SchemaPreviewModal,
      BasicDrawer,
      BasicForm,
    },

    setup(_, { emit }) {
      let action;
      let id;
      const { t } = useI18n();

      const [schemaPreviewModalRegister, { openModal }] = useModal();

      const formSchemas = [
        {
          field: 'name',
          component: 'Input',
          label: t(
            'routes.flow.definitions.node_templates_detail.task_template_detail.columns.name',
          ),
          rules: [{ required: true }],
        },
        {
          field: 'type',
          component: 'Select',
          label: t(
            'routes.flow.definitions.node_templates_detail.task_template_detail.columns.type',
          ),
          componentProps: {
            options: [
              {
                label: t(
                  'routes.flow.definitions.node_templates_detail.task_template_detail.columns.function_template',
                ),
                value: 0,
              },
              {
                label: t(
                  'routes.flow.definitions.node_templates_detail.task_template_detail.columns.plugin_template',
                ),
                value: 1,
              },
              {
                label: t(
                  'routes.flow.definitions.node_templates_detail.task_template_detail.columns.logic_template',
                ),
                value: 2,
              },
              {
                label: t(
                  'routes.flow.definitions.node_templates_detail.task_template_detail.columns.code_template',
                ),
                value: 3,
              },
            ],
            multiple: false,
            filterable: true,
          },
        },
        {
          field: 'category',
          component: 'Select',
          label: 'category',
          rules: [{ required: true }],
          componentProps: {
            options: [
              { label: 'function', value: 'function' },
              { label: 'suspense', value: 'suspense' },
              { label: 'pass', value: 'pass' },
              { label: 'return', value: 'return' },
              { label: 'foreach', value: 'foreach' },
            ],
          },
        },
        {
          field: 'icon',
          component: 'InputTextArea',
          label: 'icon base64',
        },
        {
          field: 'task_yaml',
          component: 'InputTextArea',
          label: t(
            'routes.flow.definitions.node_templates_detail.task_template_detail.columns.task_yaml',
          ),
        },
        {
          field: 'schema',
          component: 'InputTextArea',
          label: t(
            'routes.flow.definitions.node_templates_detail.task_template_detail.columns.schema',
          ),
        },
        {
          field: 'output',
          component: 'InputTextArea',
          label: t(
            'routes.flow.definitions.node_templates_detail.task_template_detail.columns.output',
          ),
        },
      ];
      const [registerForm, { getFieldsValue, setFieldsValue, validateFields, resetFields }] =
        useForm({
          layout: 'vertical',
          schemas: formSchemas,
          showResetButton: false,
          showSubmitButton: false,
          labelWidth: 140,
          baseColProps: { span: 24 },
        });

      const [registerDrawer, { closeDrawer }] = useDrawerInner(async (data) => {
        action = data.action;
        id = data.id;
        resetFields();
        setFieldsValue(data);
      });

      async function handleSubmit() {
        const data = await validateFields();
        data.id = id;
        let res;
        if (action == 'update') {
          res = await updateTemplateApi(data);
        } else {
          res = await createTemplateApi(data);
        }
        emit('response', res);
        closeDrawer();
      }

      function handlePreviewSchema() {
        const data = getFieldsValue();
        templateSchema.value = JSON.parse(data.schema);
        console.log("handlePreviewSchema", templateSchema.value)
        openModal(true);
      }

      function handlePreviewOutput() {
        const data = getFieldsValue();
        templateSchema.value = JSON.parse(data.output);
        openModal(true);
      }

      return {
        registerDrawer,
        handleSubmit,
        registerForm,
        handlePreviewSchema,
        schemaPreviewModalRegister,
        handlePreviewOutput,
        t,
      };
    },
  });
</script>
