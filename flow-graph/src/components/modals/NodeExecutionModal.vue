<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    :title="t('toolBar.executionNodeInfo.nodeDetail')"
    width="60%"
    :footer="null"
  >
    <a-card>
      <a-tabs v-model:activeKey="activeKey">
        <a-tab-pane key="1" :tab="t('toolBar.executionNodeInfo.nodeDetail')">
          <a-descriptions :column="1" bordered :label-style="{ width: '20%' }">
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.nodeName')" v-if="isShowTaskItem('name', taskDatail)">
              <a-typography-paragraph copyable underline strong>
                {{ taskDatail.name }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item
              :label="t('toolBar.executionNodeInfo.resourceProtocol')"
              v-if="isShowTaskItem('resourceProtocol', taskDatail)"
            >
              <a-typography-paragraph strong>
                {{ taskDatail['resourceProtocol'] }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.resourceName')" v-if="isShowTaskItem('resourceName', taskDatail)">
              <a-typography-paragraph strong>
                {{ taskDatail['resourceName'] }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.status')" v-if="isShowTaskItem('status', taskDatail)">
              <a-tag :color="dagStatusColor(taskDatail['status'])">
                {{ taskDatail['status'] }}
              </a-tag>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.sync')" v-if="isShowTaskItem('pattern', taskDatail)">
              <a-typography-paragraph >
                <a-switch v-model:checked="isSync" disabled="true" style="margin-bottom: 5px" />
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.startTime')">{{ taskStartTime }}</a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.endTime')">{{ taskEndTime }}</a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.nodeInputInfo')" layout="vertical">
              <a-table
                :data-source="nodeInputDataSource"
                :columns="formContextColumns"
                :pagination="false"
              >
                <template #value="{ text }">
                  <a @click="handleClick(text)">{{ truncateText(text) }}</a>
                </template>
              </a-table>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.nodeOutputInfo')" layout="vertical">
              <a-table
                :data-source="nodeOutputDataSource"
                :columns="formContextColumns"
                :pagination="false"
              >
                <template #value="{ text }">
                  <a @click="handleClick(text)">{{ truncateText(text) }}</a>
                </template>
              </a-table>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.errorMsg')">
              <a-typography-text code>
                {{ errorMsg }}
              </a-typography-text>
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.inputMappings')" layout="vertical">
              <a-table
                :data-source="nodeInputMappingsSource"
                :columns="nodeMappingsColumns"
                :pagination="false"
              />
            </a-descriptions-item>
            <a-descriptions-item :label="t('toolBar.executionNodeInfo.outputMappings')" layout="vertical">
              <a-table
                :data-source="nodeOutputMappingsDataSource"
                :columns="nodeMappingsColumns"
                :pagination="false"
              />
            </a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>
        <a-tab-pane key="2" :tab="t('toolBar.executionNodeInfo.other')" disabled>
          <a-card title="">{{ t('toolBar.executionNodeInfo.defaultOtherMsg') }}}</a-card>
        </a-tab-pane>
      </a-tabs>
    </a-card>
    <a-modal
      v-model:visible="showCodemirror"
      wrap-class-name="full-modal-to-xl"
      :title="t('toolBar.executionNodeInfo.detail')"
      width="50%"
      :footer="null"
    >
      <Codemirror v-if="showCodemirror" v-model:value="codeValue" :options="codeOptions" border />
    </a-modal>
  </a-modal>
</template>

<script setup lang="ts">
  import { ref, toRaw } from 'vue';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { getOptEnumByOpt, OptEnum } from '../../models/enums/optEnum';
  import { BaseTask } from '../../models/task/baseTask';
  import { dagStatusColor } from '@/src/common/dagStatusStyle';
  import moment from 'moment';
  import Codemirror from 'codemirror-editor-vue3';
  import { useI18n } from 'vue-i18n';
  const { t } = useI18n();

  const open = ref<boolean>(false);
  const activeKey = ref('1');
  const taskDatail = ref();
  const taskStartTime = ref('');
  const taskEndTime = ref('');
  const nodeInputDataSource = ref([]);
  const nodeOutputDataSource = ref([]);
  const nodeInputMappingsSource = ref([]);
  const nodeOutputMappingsDataSource = ref([]);
  const errorMsg = ref(t('toolBar.executionNodeInfo.defaultOtherMsg'));
  const maxTextLength = ref(100);
  const showCodemirror = ref(false);
  const codeValue = ref({});
  const codeOptions = ref({
    mode: 'application/json',
  });
  const formContextColumns = ref([
    {
      title: t('toolBar.executionNodeInfo.paramsName'),
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: t('toolBar.executionNodeInfo.paramsValue'),
      dataIndex: 'value',
      key: 'value',
      ellipsis: true,
      slots: { title: 'value', customRender: 'value' },
    },
  ]);
  const isSync = ref(false);
  function handleClick(text) {
    codeValue.value = isValidJSON(text) ? JSON.stringify(JSON.parse(text), null, 2) : text;
    showCodemirror.value = true;
  }
  const nodeMappingsColumns = ref([
    {
      title: t('toolBar.executionNodeInfo.paramsValue'),
      dataIndex: 'source',
      key: 'source',
    },
    {
      title: t('toolBar.executionNodeInfo.paramsValue'),
      dataIndex: 'target',
      key: 'target',
      ellipsis: true,
    },
  ]);
  function isOpenModel(opt: OptEnum) {
    return opt === OptEnum.DISPLAY;
  }
  function reset() {
    activeKey.value = '1';
    taskDatail.value = {};
    taskStartTime.value = '';
    taskEndTime.value = '';
    nodeInputDataSource.value = [];
    nodeOutputDataSource.value = [];
    nodeOutputDataSource.value = [];
    nodeInputMappingsSource.value = [];
    nodeOutputMappingsDataSource.value = [];
    errorMsg.value = t('toolBar.executionNodeInfo.defaultOtherMsg');
  }
  Channel.eventListener(CustomEventTypeEnum.NODE_CLICK, (nodeCell) => {
    reset();
    const flowGraphStore = useFlowStoreWithOut();
    if (!isOpenModel(getOptEnumByOpt(flowGraphStore.getFlowParams().opt))) {
      return;
    }
    const nodeId = nodeCell.getData().nodeId;
    const task = flowGraphStore.getFlowGraph().getNode(nodeId).task;
    isSync.value = task?.pattern === 'task_sync';
    taskDatail.value = task;
    const invokeMsg = toRaw(flowGraphStore.getFlowGraph().getNode(nodeId).task.invokeMsg);
    if (invokeMsg?.invoke_time_infos) {
      taskStartTime.value = moment(
        invokeMsg.invoke_time_infos[0].start_time,
      ).format('YYYY-MM-DD HH:mm:ss');
      taskEndTime.value = moment(
        invokeMsg.invoke_time_infos[0].end_time,
      ).format('YYYY-MM-DD HH:mm:ss');
    }

    if (invokeMsg?.msg) {
      errorMsg.value = invokeMsg.msg;
    }
    const inputData = invokeMsg?.input;
    for (const key in inputData) {
      let data = toRaw(inputData[key]);
      nodeInputDataSource.value.push({
        name: key,
        value: isValidJSON(data) ? JSON.stringify(data) : data,
      });
    }

    const outputData = invokeMsg?.output;
    for (const key in outputData) {
      let data = toRaw(outputData[key]);
      nodeOutputDataSource.value.push({
        name: key,
        value: isValidJSON(data) ? JSON.stringify(data) : data,
      });
    }

    const inputMappings = task.inputMappings;
    for (const key in inputMappings) {
      nodeInputMappingsSource.value.push({
        source: inputMappings[key].source,
        target: inputMappings[key].target,
      });
    }

    const outputMappings = task.outputMappings;
    for (const key in outputMappings) {
      nodeOutputMappingsDataSource.value.push({
        source: outputMappings[key].source,
        target: outputMappings[key].target,
      });
    }
    open.value = true;
  });

  function isShowTaskItem(taskItemName: string, taskDatail: BaseTask) {
    if (taskDatail.hasOwnProperty(taskItemName)) {
      if (taskDatail[taskItemName] !== '' || taskDatail[taskItemName] !== undefined) {
        return true;
      }
    }
    return false;
  }

  function truncateText(text) {
    if (text && text.length > maxTextLength.value) {
      return text.slice(0, maxTextLength.value) + '...';
    }
    return text;
  }

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
</script>

<style scoped xml:lang="less"></style>
