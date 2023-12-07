<template>
  <div class="title">{{ t('routes.flow.instances.graph.grid.title') }}</div>
  <div class="context px-2">
    <Description
      class="description"
      :column="1"
      :schema="schema"
      layout="vertical"
      @register="registerDes"
    />
  </div>
</template>


<script lang="ts" setup>
import {h, createVNode, inject, onMounted, ref} from 'vue';
import {Description, DescItem, useDescription} from '@/components/Description';
import moment from "moment";
import {Typography, Progress} from "ant-design-vue";
import {BasicColumn, BasicTable} from "@/components/Table";
import BlockText from "@/components/Dag/src/components/Templates/BlockTypography.vue";
import FlowStatus from "@/components/Dag/src/components/Templates/FlowStatus.vue";
import {useI18n} from '@/hooks/web/useI18n';
import {useMessage} from "@/hooks/web/useMessage";
import {useGlobSetting} from "@/hooks/setting";
import {getAppEnvConfig} from "@/utils/env";
import {useAppStore} from "@/store/modules/app";
import {flowGroupDetailApi} from "@/api/table";

const {createMessage} = useMessage();
const dagDetail: any = inject('dagInfo');

const {t} = useI18n();
const {tarce_server} = useGlobSetting()
const dagInfo = ref([])

const appStore = useAppStore();
const schema = ref([])

const commonLinkRender = (text: string) => (href) => {
  if (href) {
    return h('a', {href, target: '_blank'}, text);
  } else {
    return h('a', {href, target: '_blank', style: {cursor: 'not-allowed'}}, text);
  }
};


onMounted(() => {
  dagInfo.value = {
    execution_id: dagDetail.value?.execution_id,
    status: dagDetail.value?.dag_status,
    progress: dagDetail.value?.process,
    start_time: moment(dagDetail.value?.dag_invoke_msg?.invoke_time_infos[0].start_time).format('YYYY-MM-DD HH:mm:ss'),
    end_time: moment(dagDetail.value?.dag_invoke_msg?.invoke_time_infos[0].end_time).format('YYYY-MM-DD HH:mm:ss'),
    context: JSON.stringify(dagDetail.value?.context),
    result: dagDetail.value?.dag_invoke_msg?.msg,
    trace: dagDetail.value?.trace_url
  }
  const trace_schema = {
    field: 'trace',
    label: t('routes.flow.instances.graph.grid.schema.trace'),
    render: commonLinkRender(t('routes.flow.instances.graph.grid.schema.trace_detail')),
  }
  schema.value = [
    {
      field: 'execution_id',
      label: t('routes.flow.instances.graph.grid.schema.execution_id'),
      render: (text) => {
        return createVNode(Typography.Paragraph, {copyable: true, underline: true}, {default: () => text});
      }
    },
    {
      field: 'status',
      label: t('routes.flow.instances.graph.grid.schema.status'),
      render: (text) => {
        return createVNode(FlowStatus, {status: text});
      }
    },
    {
      field: 'progress',
      label: t('routes.flow.instances.graph.grid.schema.progress'),
      render: (text) => {
        return createVNode(Progress, {percent: text});
      }
    },
    {
      field: 'start_time',
      label: t('routes.flow.instances.graph.grid.schema.start_time'),
    },
    {
      field: 'end_time',
      label: t('routes.flow.instances.graph.grid.schema.end_time'),
    },
    {
      field: 'context',
      label: t('routes.flow.instances.graph.grid.schema.context'),
      render: (text) => {
        function getBasicShortColumns(): BasicColumn[] {
          return [
            {
              title: t('routes.flow.instances.graph.grid.schema.context_key'),
              dataIndex: 'key',
            },
            {
              title: t('routes.flow.instances.graph.grid.schema.context_value'),
              dataIndex: 'value',
            }
          ];
        }

        const result = text === undefined ? {} : JSON.parse(text)
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
      label: t('routes.flow.instances.graph.grid.schema.error_result_msg'),
      render: (text) => {
        return createVNode(BlockText, {context: text}, {default: () => text});
      }
    },
  ];
  if(dagInfo.value.trace !== undefined) {
    schema.value.push(trace_schema)
  }

});

const [registerDes] = useDescription({
  title: '',
  bordered: false,
  data: dagInfo,
  schema: schema,
});


</script>

<style lang="less" scoped></style>
