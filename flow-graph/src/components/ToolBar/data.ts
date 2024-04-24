export function getSaveFormSchema() {
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
              title: '流程基本信息',
            },
            properties: {
              workspace: {
                type: 'string',
                title: '业务名称',
                required: true,
                disabled: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
              },
              dagName: {
                type: 'string',
                title: '服务名称',
                required: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
              },
              alias: {
                type: 'string',
                title: '别名',
                required: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
              }
            },
          },
          step2: {
            type: 'void',
            'x-component': 'FormStep.StepPane',
            'x-component-props': {
              title: '输入参数列表',
            },
            properties: getInputSchema(),
          },
        },
      },
    },
  }
}

export function getInputSchema() {
  return {
    inputSchema: {
      type: 'array',
      'x-component': 'ArrayItems',
      'x-decorator': 'FormItem',
      title: '输入参数',
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
                title: '参数名',
                required: true,
                'x-decorator': 'FormItem',
                'x-component': 'Input',
              },
              type: {
                type: 'string',
                title: '参数类型',
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
                title: '参数是否必填',
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
                title: '参数描述',
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
          title: '添加输入参数',
          'x-component': 'ArrayItems.Addition',
        },
      },
    },
  };
}
