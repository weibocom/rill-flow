import { useI18nStoreWithOut } from "../../store/modules/i18nStore";

export function getSaveFormSchema() {
  const { t } = useI18nStoreWithOut().getI18n().global;
  const labelWidth = 100;
  return {
    type: 'object',
    properties: {
      collapse: {
        type: 'void',
        'x-component': 'FormStep',
        'x-component-props': {
          formStep: '{{formStep}}',
        },
        properties: {
          step1: {
            type: 'void',
            'x-component': 'FormStep.StepPane',
            'x-component-props': {
              title: t('toolBar.saveDag.baseInfo'),
            },
            properties: {
              workspace: {
                type: 'string',
                title: t('toolBar.saveDag.businessName'),
                required: true,
                disabled: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
                'x-decorator-props': {
                  labelAlign: 'left',
                  labelWidth: labelWidth,
                },
              },
              dagName: {
                type: 'string',
                title: t('toolBar.saveDag.serviceName'),
                required: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
                'x-decorator-props': {
                  labelAlign: 'left',
                  labelWidth: labelWidth,
                },
              },
              alias: {
                type: 'string',
                title: t('toolBar.saveDag.alias'),
                required: false,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
                'x-decorator-props': {
                  labelAlign: 'left',
                  labelWidth: labelWidth,
                },
              }
            },
          },
          step2: {
            type: 'void',
            'x-component': 'FormStep.StepPane',
            'x-component-props': {
              title: t('toolBar.saveDag.inputSchemaLists'),
            },
            properties: getInputSchema(),
          },
        },
      },
    },
  }
}

export function getInputSchema() {
  const { t } = useI18nStoreWithOut().getI18n().global;
  return {
    inputSchema: {
      type: 'array',
      'x-component': 'ArrayItems',
      'x-decorator': 'FormItem',
      title: t('toolBar.saveDag.inputParams'),
      required: false,
      items: {
        type: 'object',
        properties: {
          space: {
            type: 'void',
            'x-component': 'Space',
            properties: {
              sort: {
                type: 'void',
                'x-decorator': 'FormItem',
                'x-component': 'ArrayItems.SortHandle',
              },
              name: {
                type: 'string',
                title: t('toolBar.saveDag.paramsName'),
                required: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
              },
              type: {
                type: 'string',
                title: t('toolBar.saveDag.paramsType'),
                required: true,
                'x-decorator': 'FormItem',
                'x-component': 'Select',
                enum: [
                  {
                    label: 'String',
                    value: 'String',
                  },
                  {
                    label: 'Number',
                    value: 'Number',
                  },
                  {
                    label: 'Boolean',
                    value: 'Boolean',
                  },
                  {
                    label: 'JSON',
                    value: 'JSON',
                  },
                ],
                'x-component-props': {
                  style: {
                    width: '100px',
                  },
                },
              },
              required: {
                type: 'boolean',
                title: t('toolBar.saveDag.paramsRequired'),
                'x-decorator': 'FormItem',
                'x-component': 'Switch',
                required: true,
                default: true,
                'x-component-props': {
                  style: {},
                },
              },
              desc: {
                type: 'string',
                title: t('toolBar.saveDag.paramsDesc'),
                required: false,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
              },
              remove: {
                type: 'void',
                'x-decorator': 'FormItem',
                'x-component': 'ArrayItems.Remove',
              },
            },
          },
        },
      },
      properties: {
        add: {
          type: 'void',
          title: t('toolBar.saveDag.addInputParams'),
          'x-component': 'ArrayItems.Addition',
        },
      },
    },
  };
}

export function getOutputSchema() {
  const { t } = useI18nStoreWithOut().getI18n().global;
  return {
    type: 'object',
    title: t('toolBar.saveDag.outputSchemaLists'),
    properties: {
      outputSchema: {
        type: 'array',
        'x-component': 'ArrayItems',
        'x-decorator': 'FormItem',
        title: t('toolBar.saveDag.outputParams'),
        required: false,
        items: {
          type: 'object',
          properties: {
            space: {
              type: 'void',
              'x-component': 'Space',
              properties: {
                sort: {
                  type: 'void',
                  'x-decorator': 'FormItem',
                  'x-component': 'ArrayItems.SortHandle',
                },
                name: {
                  type: 'string',
                  title: t('toolBar.saveDag.paramsName'),
                  required: true,
                  'x-decorator': 'FormItem',
                  'x-component': 'Input',
                },
                type: {
                  type: 'string',
                  title: t('toolBar.saveDag.paramsType'),
                  required: true,
                  'x-decorator': 'FormItem',
                  'x-component': 'Select',
                  enum: [
                    {
                      label: 'String',
                      value: 'string',
                    },
                    {
                      label: 'Number',
                      value: 'number',
                    },
                    {
                      label: 'Boolean',
                      value: 'boolean',
                    },
                    {
                      label: 'Object',
                      value: 'object',
                    },
                  ],
                  'x-component-props': {
                    style: {
                      width: '100px',
                    },
                  },
                },
                required: {
                  type: 'boolean',
                  title: t('toolBar.saveDag.paramsRequired'),
                  'x-decorator': 'FormItem',
                  'x-component': 'Switch',
                  required: true,
                  default: true,
                  'x-component-props': {
                    style: {},
                  },
                },
                desc: {
                  type: 'string',
                  title: t('toolBar.saveDag.paramsDesc'),
                  required: false,
                  'x-decorator': 'FormItem',
                  'x-component': 'Input',
                },
                remove: {
                  type: 'void',
                  'x-decorator': 'FormItem',
                  'x-component': 'ArrayItems.Remove',
                },
              },
            },
          },
        },
        properties: {
          add: {
            type: 'void',
            title: t('toolBar.saveDag.addOutputParams'),
            'x-component': 'ArrayItems.Addition',
          },
        },
      },
    }
  }
}