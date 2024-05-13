<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    :title="t('toolBar.dagExecutionDetail.title')"
    width="60%"
    :footer="null"
  >
    <a-card>
      <a-tabs v-model:activeKey="activeKey">
        <a-tab-pane key="1" :tab="t('toolBar.dagExecutionDetail.title')">
          <a-descriptions :column="1" bordered :label-style="{ width: '20%' }">
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.id')">
              <a-typography-paragraph copyable underline strong>
                {{ form.executionId }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.status')">
              <a-tag :color="dagStatusColor(form.dagStatus)">
                {{ form.dagStatus }}
              </a-tag>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.process')">
              <a-progress :percent="form.process" />
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.startTime')">{{ form.startTime }}</a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.endTime')">{{ form.endTime }}</a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.errorMsg')">
              <a-typography-paragraph code>
                {{ errorMsg }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.context')" layout="vertical">
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
            <a-descriptions-item :label="t('toolBar.dagExecutionDetail.trace')">
              <a-typography-link :href="form.traceUrl" target="_bank">
                {{ form.traceUrl }}
              </a-typography-link>
            </a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>
        <a-tab-pane key="2" :tab="t('toolBar.dagExecutionDetail.other')" disabled>
          <a-card title="" />
        </a-tab-pane>
      </a-tabs>
    </a-card>
  </a-modal>
  <a-modal
    v-model:visible="showCodemirror"
    wrap-class-name="full-modal-to-xl"
    :title="t('toolBar.dagExecutionDetail.detail')"
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
  import { useI18n } from 'vue-i18n';
  const { t } = useI18n();

  const form = ref<DagExecutionShowInfo>();
  const open = ref<boolean>(false);
  const activeKey = ref('1');
  const formContextDataSource = ref([]);
  const errorMsg = ref(t('toolBar.dagExecutionDetail.defaultOtherMsg'));
  const maxTextLength = ref(100);
  const codeValue = ref({});
  const showCodemirror = ref(false);
  const codeOptions = ref({
    mode: 'application/json',
  });
  const formContextColumns = ref([
    {
      title: t('toolBar.dagExecutionDetail.paramsName'),
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: t('toolBar.dagExecutionDetail.paramsValue'),
      dataIndex: 'value',
      key: 'value',
      ellipsis: true,
      slots: { title: 'value', customRender: 'value' },
    },
  ]);
  function reset() {
    activeKey.value = '1';
    formContextDataSource.value = [];
    errorMsg.value = t('toolBar.dagExecutionDetail.defaultOtherMsg');
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
