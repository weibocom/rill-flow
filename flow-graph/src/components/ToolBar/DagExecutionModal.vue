<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    title="执行详情"
    width="60%"
    :footer="null"
  >
    <a-card>
      <a-tabs v-model:activeKey="activeKey">
        <a-tab-pane key="1" tab="执行详情">
          <a-descriptions :column="1" bordered :label-style="{ width: '20%' }">
            <a-descriptions-item label="执行ID">
              <a-typography-paragraph copyable underline strong>
                {{ form.executionId }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item label="状态">
              <a-tag :color="dagStatusColor(form.dagStatus)">
                {{ form.dagStatus }}
              </a-tag>
            </a-descriptions-item>
            <a-descriptions-item label="进度">
              <a-progress :percent="form.process" />
            </a-descriptions-item>
            <a-descriptions-item label="开始时间">{{ form.startTime }}</a-descriptions-item>
            <a-descriptions-item label="结束时间">{{ form.endTime }}</a-descriptions-item>
            <a-descriptions-item label="异常信息">
              <a-typography-paragraph code>
                {{ errorMsg }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item label="上下文信息" layout="vertical">
              <a-table
                :data-source="formContextDataSource"
                :columns="formContextColumns"
                :pagination="false"
              >
                <template #value="{ text }">
                  <a @click="handleClick(text)">{{ truncateText(text) }}</a>
                </template>
              </a-table>
            </a-descriptions-item>
            <a-descriptions-item label="Trace日志信息">
              <a-typography-link :href="form.traceUrl" target="_bank">
                {{ form.traceUrl }}
              </a-typography-link>
            </a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>
        <a-tab-pane key="2" tab="其他" disabled>
          <a-card title="" />
        </a-tab-pane>
      </a-tabs>
    </a-card>
  </a-modal>
  <a-modal
    v-model:visible="showCodemirror"
    wrap-class-name="full-modal-to-xl"
    title="详情"
    width="50%"
    :footer="null"
  >
    <Codemirror v-if="showCodemirror" v-model:value="codeValue" :options="codeOptions" border />
  </a-modal>
</template>

<script setup lang="ts">
  import { ref, toRaw } from 'vue';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { DagExecutionShowInfo } from '../../models/dag/dagExecutionInfo';
  import moment from 'moment/moment';
  import { dagStatusColor } from '../../common/dagStatusStyle';
  import Codemirror from 'codemirror-editor-vue3';

  const form = ref<DagExecutionShowInfo>();
  const open = ref<boolean>(false);
  const activeKey = ref('1');
  const formContextDataSource = ref([]);
  const errorMsg = ref('暂无');
  const maxTextLength = ref(100);
  const codeValue = ref({});
  const showCodemirror = ref(false);
  const codeOptions = ref({
    mode: 'application/json',
  });
  const formContextColumns = ref([
    {
      title: '参数名',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '参数值',
      dataIndex: 'value',
      key: 'value',
      ellipsis: true,
      slots: { title: 'value', customRender: 'value' },
    },
  ]);
  function reset() {
    activeKey.value = '1';
    formContextDataSource.value = [];
    errorMsg.value = '暂无';
  }
  Channel.eventListener(CustomEventTypeEnum.TOOL_BAR_EXECUTION_DETAIL, () => {
    reset();
    const flowGraphStore = useFlowStoreWithOut();
    const startTime = moment(
      flowGraphStore.getFlowGraph().getDagExecutionInfo().dagInvokeMsg.invoke_time_infos[0]
        .start_time,
    ).format('YYYY-MM-DD HH:mm:ss');
    const endTime = moment(
      flowGraphStore.getFlowGraph().getDagExecutionInfo().dagInvokeMsg.invoke_time_infos[0]
        .end_time,
    ).format('YYYY-MM-DD HH:mm:ss');
    form.value = new DagExecutionShowInfo(
      flowGraphStore.getFlowGraph().getDagExecutionInfo().executionId,
      flowGraphStore.getFlowGraph().getDagExecutionInfo().dagStatus,
      flowGraphStore.getFlowGraph().getDagExecutionInfo().process,
      flowGraphStore.getFlowGraph().getDagExecutionInfo().traceUrl,
      startTime,
      endTime,
      flowGraphStore.getFlowGraph().getDagExecutionInfo().dagInvokeMsg.msg,
      flowGraphStore.getFlowGraph().getDagExecutionInfo().context,
    );
    const context = toRaw(flowGraphStore.getFlowGraph().getDagExecutionInfo().context);

    for (const key in context) {
      formContextDataSource.value.push({
        name: key,
        value: isValidJSON(context[key]) ? JSON.stringify(context[key], null, 2) : context[key],
      });
    }
    if(form.value.traceUrl.includes("jaeger")) {
      form.value.traceUrl = form.value.traceUrl.replace("jaeger", window.location.hostname)
    }
    if (form.value.msg) {
      errorMsg.value = form.value.msg;
    }
    activeKey.value = '1';

    open.value = true;
  });

  function isValidJSON(text) {
    try {
      if (typeof text === 'string') {
        JSON.parse(text);
      } else {
        JSON.parse(JSON.stringify(text));
      }
      return true;
    } catch (error) {
      return false;
    }
  }

  function truncateText(text) {
    if (text && text.length > maxTextLength.value) {
      return text.slice(0, maxTextLength.value) + '...';
    }
    return text;
  }

  function handleClick(text) {
    codeValue.value = isValidJSON(text) ? JSON.stringify(JSON.parse(text), null, 2) : text;
    showCodemirror.value = true;
  }
</script>

<style scoped xml:lang="less"></style>
