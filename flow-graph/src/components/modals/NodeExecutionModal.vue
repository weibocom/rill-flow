<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    title="节点详情"
    width="60%"
    :footer="null"
  >
    <a-card>
      <a-tabs v-model:activeKey="activeKey">
        <a-tab-pane key="1" tab="节点详情">
          <a-descriptions :column="1" bordered :label-style="{ width: '20%' }">
            <a-descriptions-item label="节点名称" v-if="isShowTaskItem('name', taskDatail)">
              <a-typography-paragraph copyable underline strong>
                {{ taskDatail.name }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item
              label="资源类型"
              v-if="isShowTaskItem('resourceProtocol', taskDatail)"
            >
              <a-typography-paragraph strong>
                {{ taskDatail['resourceProtocol'] }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item label="资源地址" v-if="isShowTaskItem('resourceName', taskDatail)">
              <a-typography-paragraph strong>
                {{ taskDatail['resourceName'] }}
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item label="状态" v-if="isShowTaskItem('status', taskDatail)">
              <a-tag :color="dagStatusColor(taskDatail['status'])">
                {{ taskDatail['status'] }}
              </a-tag>
            </a-descriptions-item>
            <a-descriptions-item label="同步执行" v-if="isShowTaskItem('pattern', taskDatail)">
              <a-typography-paragraph >
                <a-switch v-model:checked="isSync" disabled="true" style="margin-bottom: 5px" />
              </a-typography-paragraph>
            </a-descriptions-item>
            <a-descriptions-item label="开始时间">{{ taskStartTime }}</a-descriptions-item>
            <a-descriptions-item label="结束时间">{{ taskEndTime }}</a-descriptions-item>
            <a-descriptions-item label="节点输入信息" layout="vertical">
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
            <a-descriptions-item label="节点输出信息" layout="vertical">
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
            <a-descriptions-item label="异常信息">
              <a-typography-text code>
                {{ errorMsg }}
              </a-typography-text>
            </a-descriptions-item>
            <a-descriptions-item label="输入映射信息" layout="vertical">
              <a-table
                :data-source="nodeInputMappingsSource"
                :columns="nodeMappingsColumns"
                :pagination="false"
              />
            </a-descriptions-item>
            <a-descriptions-item label="输出映射信息" layout="vertical">
              <a-table
                :data-source="nodeOutputMappingsDataSource"
                :columns="nodeMappingsColumns"
                :pagination="false"
              />
            </a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>
        <a-tab-pane key="2" tab="其他" disabled>
          <a-card title="">暂无</a-card>
        </a-tab-pane>
      </a-tabs>
    </a-card>
    <a-modal
      v-model:visible="showCodemirror"
      wrap-class-name="full-modal-to-xl"
      title="详情"
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

  const open = ref<boolean>(false);
  const activeKey = ref('1');
  const taskDatail = ref();
  const taskStartTime = ref('');
  const taskEndTime = ref('');
  const nodeInputDataSource = ref([]);
  const nodeOutputDataSource = ref([]);
  const nodeInputMappingsSource = ref([]);
  const nodeOutputMappingsDataSource = ref([]);
  const errorMsg = ref('暂无');
  const maxTextLength = ref(100);
  const showCodemirror = ref(false);
  const codeValue = ref({});
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
  const isSync = ref(false);
  function handleClick(text) {
    codeValue.value = isValidJSON(text) ? JSON.stringify(JSON.parse(text), null, 2) : text;
    showCodemirror.value = true;
  }
  const nodeMappingsColumns = ref([
    {
      title: '参数来源',
      dataIndex: 'source',
      key: 'source',
    },
    {
      title: '参数目标位置',
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
    errorMsg.value = '暂无';
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
