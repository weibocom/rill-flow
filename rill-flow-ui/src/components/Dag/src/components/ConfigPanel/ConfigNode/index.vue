<template>
  <div class="title">{{ t('routes.flow.instances.graph.node.title') }}</div>
  <div class="context px-2">
    <Description
      class="description"
      :column="1"
      :schema="schema"
      :data="nodeDetail"
      layout="vertical"
      :bordered="false"
    />
  </div>

</template>


<script lang="ts" setup>
import {createVNode, h, inject, onMounted, ref, watch} from 'vue';
import {Description, DescItem} from '@/components/Description';
import {BasicColumn, BasicTable} from "@/components/Table";
import BlockText from "@/components/Dag/src/components/Templates/BlockTypography.vue";
import FlowStatus from "@/components/Dag/src/components/Templates/FlowStatus.vue";
import {Tag, Switch, Typography} from "ant-design-vue";
import {useI18n} from '@/hooks/web/useI18n';
import {RILL_CATEGORY} from "@/components/Dag";

const commonTagRender = (color: string) => (curVal) => h(Tag, {color}, () => curVal);
const {t} = useI18n();
const schema = ref()

const nodeDetail: any = inject('nodeDetail');
onMounted(() => {
  schema.value = getSchema(nodeDetail.value.category)
});


watch(() => nodeDetail.value, (n) => {
  schema.value = getSchema(nodeDetail.value.category)
}, {deep: true})

function getSchema(category) {
  switch (category) {
    case RILL_CATEGORY.FOREACH:
      return [
        {
          field: 'name',
          label: t('routes.flow.instances.graph.node.schema.name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'category',
          label: t('routes.flow.instances.graph.node.schema.category'),
          render: commonTagRender("blue")
        },
        {
          field: 'tolerance',
          label: t('routes.flow.instances.graph.node.schema.tolerance'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'true', disabled: true});
          }
        },
        {
          field: 'status',
          label: t('routes.flow.instances.graph.node.schema.status'),
          render: (text) => {
            return createVNode(FlowStatus, {status: text});
          }
        },
        {
          field: 'iterationMapping',
          label: t('routes.flow.instances.graph.node.schema.iteration_msg'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.iteration_msg_key'),
                  dataIndex: 'key',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.iteration_msg_value'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "key": key,
                "value": typeof result[key] === 'string' ? result[key] : JSON.stringify(result[key])
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
      ];
      break;
    case RILL_CATEGORY.RETURN:
      return [
        {
          field: 'name',
          label: t('routes.flow.instances.graph.node.schema.name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'category',
          label: t('routes.flow.instances.graph.node.schema.category'),
          render: commonTagRender("blue")
        },
        {
          field: 'tolerance',
          label: t('routes.flow.instances.graph.node.schema.tolerance'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'true', disabled: true});
          }
        },
        {
          field: 'status',
          label: t('routes.flow.instances.graph.node.schema.status'),
          render: (text) => {
            return createVNode(FlowStatus, {status: text});
          }
        },
        {
          field: 'inputMappings',
          label: t('routes.flow.instances.graph.node.schema.input_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'conditions',
          label: t('routes.flow.instances.graph.node.schema.conditions'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "value": result[key],
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        }
      ];
      break;
    case RILL_CATEGORY.PASS:
      return [
        {
          field: 'name',
          label: t('routes.flow.instances.graph.node.schema.name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'category',
          label: t('routes.flow.instances.graph.node.schema.category'),
          render: commonTagRender("blue")
        },
        {
          field: 'tolerance',
          label: t('routes.flow.instances.graph.node.schema.tolerance'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'true', disabled: true});
          }
        },
        {
          field: 'status',
          label: t('routes.flow.instances.graph.node.schema.status'),
          render: (text) => {
            return createVNode(FlowStatus, {status: text});
          }
        },
        {
          field: 'inputMappings',
          label: t('routes.flow.instances.graph.node.schema.input_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'outputMappings',
          label: t('routes.flow.instances.graph.node.schema.output_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'conditions',
          label: t('routes.flow.instances.graph.node.schema.conditions'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "value": result[key],
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        }
      ];
      break;
    case RILL_CATEGORY.SUSPENSE:
      return [
        {
          field: 'name',
          label: t('routes.flow.instances.graph.node.schema.name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'category',
          label: t('routes.flow.instances.graph.node.schema.category'),
          render: commonTagRender("blue")
        },
        {
          field: 'tolerance',
          label: t('routes.flow.instances.graph.node.schema.tolerance'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'true', disabled: true});
          }
        },
        {
          field: 'status',
          label: t('routes.flow.instances.graph.node.schema.status'),
          render: (text) => {
            return createVNode(FlowStatus, {status: text});
          }
        },
        {
          field: 'inputMappings',
          label: t('routes.flow.instances.graph.node.schema.input_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'outputMappings',
          label: t('routes.flow.instances.graph.node.schema.output_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'interruptions',
          label: t('routes.flow.instances.graph.node.schema.interruptions'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.interruptions_key'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "value": result[key],
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'timeline',
          label: t('routes.flow.instances.graph.node.schema.timeline'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.timeline_key'),
                  dataIndex: 'key',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.timeline_value'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "key": key,
                "value": result[key],
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'conditions',
          label: t('routes.flow.instances.graph.node.schema.conditions'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.conditions_key'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "value": result[key],
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        }
      ];
      break;
    case RILL_CATEGORY.CHOICE:
      return [
        {
          field: 'name',
          label: t('routes.flow.instances.graph.node.schema.name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'category',
          label: t('routes.flow.instances.graph.node.schema.category'),
          render: commonTagRender("blue")
        },
        {
          field: 'tolerance',
          label: t('routes.flow.instances.graph.node.schema.tolerance'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'true', disabled: true});
          }
        },
        {
          field: 'status',
          label: t('routes.flow.instances.graph.node.schema.status'),
          render: (text) => {
            return createVNode(FlowStatus, {status: text});
          }
        },
        {
          field: 'inputMappings',
          label: t('routes.flow.instances.graph.node.schema.input_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'outputMappings',
          label: t('routes.flow.instances.graph.node.schema.output_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
      ];
      break;
    default:
      return [
        {
          field: 'name',
          label: t('routes.flow.instances.graph.node.schema.name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'resourceProtocol',
          label: t('routes.flow.instances.graph.node.schema.resource_protocol'),
          render: commonTagRender("blue")
        },
        {
          field: 'resourceName',
          label: t('routes.flow.instances.graph.node.schema.resource_name'),
          render: (text) => {
            return createVNode(Typography.Paragraph, {underline: true}, {default: () => text});
          }
        },
        {
          field: 'pattern',
          label: t('routes.flow.instances.graph.node.schema.pattern'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'task_sync', disabled: true});
          }
        },
        {
          field: 'tolerance',
          label: t('routes.flow.instances.graph.node.schema.tolerance'),
          render: (text) => {
            return createVNode(Switch, {checked: text === 'true', disabled: true});
          }
        },
        {
          field: 'start_time',
          label: t('routes.flow.instances.graph.node.schema.start_time'),
        },
        {
          field: 'end_time',
          label: t('routes.flow.instances.graph.node.schema.end_time'),
        },
        {
          field: 'status',
          label: t('routes.flow.instances.graph.node.schema.status'),
          render: (text) => {
            return createVNode(FlowStatus, {status: text});
          }
        },
        {
          field: 'inputMsg',
          label: t('routes.flow.instances.graph.node.schema.input_msg'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_msg_key'),
                  dataIndex: 'key',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.input_msg_value'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "key": key,
                "value": typeof result[key] === 'string' ? result[key] : JSON.stringify(result[key])
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'outputMsg',
          label: t('routes.flow.instances.graph.node.schema.output_msg'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.output_msg_key'),
                  dataIndex: 'key',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.output_msg_value'),
                  dataIndex: 'value',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []
            for (const key in result) {
              data.push({
                "key": key,
                "value": typeof result[key] === 'string' ? result[key] : JSON.stringify(result[key])
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'result',
          label: t('routes.flow.instances.graph.node.schema.error_result_msg'),
          render: (text) => {
            return createVNode(BlockText, {context: text}, {default: () => text});
          }
        },
        {
          field: 'inputMappings',
          label: t('routes.flow.instances.graph.node.schema.input_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.input_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
        {
          field: 'outputMappings',
          label: t('routes.flow.instances.graph.node.schema.output_mappings'),
          render: (text) => {
            function getBasicShortColumns(): BasicColumn[] {
              return [
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_source'),
                  dataIndex: 'source',
                },
                {
                  title: t('routes.flow.instances.graph.node.schema.output_mappings_target'),
                  dataIndex: 'target',
                }
              ];
            }

            let result = text !== undefined ? JSON.parse(text) : {}
            const data: any = []

            for (const key in result) {
              data.push({
                "source": result[key].source,
                "target": result[key].target,
              })
            }
            return createVNode(BasicTable, {
              showIndexColumn: false,
              canResize: false,
              columns: getBasicShortColumns(),
              dataSource: data,
              pagination: false
            }, {default: () => data});
          }
        },
      ];
  }
}


</script>

<style lang="less" scoped></style>
