<template>
  <a-card>
    <FormProvider :form="form">
      <SchemaField :schema="schema" />
    </FormProvider>
  </a-card>
</template>

<script setup lang="ts">
  import { ref } from "vue";
  import { createForm } from '@formily/core'
  import { FormProvider, createSchemaField } from '@formily/vue'
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
  import { Button } from 'ant-design-vue'

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

  const form = ref();
  form.value = createForm();

  const schema = ref({});
  schema.value = {
    type: 'object',
    properties: {
      input: {
        type: 'array',
        'x-component': 'ArrayItems',
        'x-decorator': 'FormItem',
        title: '输入变量',
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
                input: {
                  type: 'string',
                  title: '参数key',
                  'x-decorator': 'FormItem',
                  'x-component': 'Input',
                },
                select: {
                  type: 'string',
                  title: '下拉框',
                  enum: [
                    { label: '选项1', value: 1 },
                    { label: '选项2', value: 2 },
                  ],
                  'x-decorator': 'FormItem',
                  'x-component': 'Select',
                  'x-component-props': {
                    style: {
                      width: '250px',
                    },
                  },
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
            title: '添加条目',
            'x-component': 'ArrayItems.Addition',
          },
        },
      },
      output: {
        type: 'array',
        'x-component': 'ArrayItems',
        'x-decorator': 'FormItem',
        title: '输出变量',
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
                input: {
                  type: 'string',
                  title: '变量key',
                  'x-decorator': 'FormItem',
                  'x-component': 'Input',
                },
                select: {
                  type: 'string',
                  title: '下拉框',
                  enum: [
                    { label: '选项1', value: 1 },
                    { label: '选项2', value: 2 },
                  ],
                  'x-decorator': 'FormItem',
                  'x-component': 'Select',
                  'x-component-props': {
                    style: {
                      width: '250px',
                    },
                  },
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
            title: '添加条目',
            'x-component': 'ArrayItems.Addition',
          },
        },
      },
    },
  };
  function log(values) {
    console.log(values)
  }
</script>

<style scoped lang="less">

</style>