import { FormProps } from '/@/components/Table';
import { BasicColumn } from '/@/components/Table/src/types/table';
import { ref } from 'vue';
import { useI18n } from '/@/hooks/web/useI18n';

const { t } = useI18n();

export function getBasicColumns(): BasicColumn[] {
  return [
    {
      title: 'ID',
      dataIndex: 'id',
      fixed: 'left',
      width: 50,
    },
    {
      title: t('routes.flow.definitions.node_templates_detail.columns.name'),
      dataIndex: 'name',
      width: 150,
    },
    {
      title: t('routes.flow.definitions.node_templates_detail.columns.type'),
      dataIndex: 'type_str',
      width: 100,
    },
    {
      title: t('routes.flow.definitions.node_templates_detail.columns.node_type'),
      dataIndex: 'node_type',
      width: 100,
      slots: { customRender: 'node_type' },
    },
    {
      title: t('routes.flow.definitions.node_templates_detail.columns.category'),
      dataIndex: 'category',
      width: 100,
    },
    {
      title: t('routes.flow.definitions.node_templates_detail.columns.status'),
      dataIndex: 'enable',
      width: 100,
      slots: { customRender: 'enable' },
    },
  ];
}

export const templateEnable = ref(1);
export const templateSchema = ref({});

export function getFormConfig(): Partial<FormProps> {
  return {
    labelWidth: 100,
    schemas: [
      {
        field: 'id',
        label: 'id',
        component: 'Input',
        colProps: {
          xl: 3,
          xxl: 3,
        },
      },
      {
        field: 'name',
        label: t('routes.flow.definitions.node_templates_detail.columns.name'),
        component: 'Input',
        colProps: {
          xl: 3,
          xxl: 5,
        },
      },
      {
        field: 'type',
        label: t('routes.flow.definitions.node_templates_detail.columns.type'),
        component: 'Select',
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
        },
        colProps: {
          xl: 3,
          xxl: 3,
        },
      },
      {
        field: 'category',
        label: 'category',
        component: 'Select',
        componentProps: {
          options: [
            { label: 'function', value: 'function' },
            { label: 'suspense', value: 'suspense' },
            { label: 'pass', value: 'pass' },
            { label: 'return', value: 'return' },
            { label: 'foreach', value: 'foreach' },
          ],
        },
        colProps: {
          xl: 3,
          xxl: 3,
        },
      },
      {
        field: 'node_type',
        label: t('routes.flow.definitions.node_templates_detail.columns.node_type'),
        component: 'Select',
        componentProps: {
          options: [
            {
              label: t('routes.flow.definitions.node_templates_detail.node_type.meta'),
              value: 'meta',
            },
            {
              label: t('routes.flow.definitions.node_templates_detail.node_type.template'),
              value: 'template',
            },
          ],
        },
        colProps: {
          xl: 3,
          xxl: 3,
        },
      },
      {
        field: 'enable',
        label: t('routes.flow.definitions.node_templates_detail.columns.status'),
        component: 'Select',
        componentProps: {
          options: [
            { label: t('routes.flow.definitions.node_templates_detail.option.enable'), value: 1 },
            { label: t('routes.flow.definitions.node_templates_detail.option.disable'), value: 0 },
          ],
          defaultValue: 1,
          onChange: (e: number) => {
            templateEnable.value = e;
          },
        },
        colProps: {
          xl: 3,
          xxl: 3,
        },
      },
    ],
  };
}
