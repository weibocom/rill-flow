<template>
  <div class="bar">
    <a-tooltip class="showStatus mx-20" v-if="opt === 'display'">
      当前执行状态:
      <a-tag :color="dagStatusColor(dagStatus)">
        {{ dagStatus }}
      </a-tag>
    </a-tooltip>
    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <template #title>
        <span>编辑DAG的基本信息</span>
      </template>
      <a-button name="edit" @click="handleClick" class="item-space" size="small">编辑</a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <template #title>
        <span>保存已修改内容</span>
      </template>
      <a-button name="save" @click="handleClick" class="item-space" size="small">保存</a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <template #title>
        <span>提交Dag的测试任务</span>
      </template>
      <a-button name="submitDagTestRun" @click="handleClick" class="item-space" size="small"
        >测试</a-button
      >
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <template #title>
        <span>DagYaml信息</span>
      </template>
      <a-button name="toJSON" @click="handleClick" class="item-space" size="small"> 流程详情 </a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt === 'display'">
      <template #title>
        <span>执行详情</span>
      </template>
      <a-button name="executionDetail" @click="handleClick" class="item-space" size="small"> 执行详情 </a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt === 'display'">
      <template #title>
        <span>刷新</span>
      </template>
      <a-button name="refresh" @click="handleClick" class="item-space" size="small"> 刷新
      </a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <template #title>
        <span>codeEdit</span>
      </template>
      <a-button name="codeEdit" @click="handleClick" class="item-space" size="small"> 代码执行节点 </a-button>
    </a-tooltip>

    <ShowDag />
    <SaveDag />
    <EditDagMeta />
    <DagTestRun />
    <DagExecutionModal />
    <CodeEdit />
  </div>
</template>

<script lang="ts" setup>
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import ShowDag from './ShowDag.vue';
  import SaveDag from './SaveDag.vue';
  import EditDagMeta from './EditDagMeta.vue';
  import DagTestRun from './DagTestRun.vue';
  import DagExecutionModal from "./DagExecutionModal.vue";
  import CodeEdit from "@/src/components/ToolBar/CodeEdit.vue";
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { ref, watch } from "vue";
  import { dagStatusColor } from "@/src/common/dagStatusStyle";


  const opt = ref('');
  const flowGraphStore = useFlowStoreWithOut();
  opt.value = flowGraphStore.getFlowParams().opt;
  const dagStatus = ref('SUCCEED');
  const handleClick = (event: Event) => {
    const name = (event.currentTarget as any).name!;

    switch (name) {
      case 'toJSON':
        Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_SHOW_DAG, event);
        break;
      case 'save':
        Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_SAVE_DAG, event);
        break;
      case 'edit':
        Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_EDIT_INPUT_SCHEMA, event);
        break;
      case 'submitDagTestRun':
        Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_SUBMIT_DAG_TEST_RUN, event);
        break;
      case 'executionDetail':
        Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_EXECUTION_DETAIL, event);
        break;
      case 'refresh':
        Channel.dispatchEvent(CustomEventTypeEnum.SHOW_EXECUTION_RESULT, flowGraphStore.getFlowParams().id);
        break;
      case 'codeEdit' :
        // TODO TEST
        Channel.dispatchEvent(CustomEventTypeEnum.TOOL_BAR_CODE_EDIT, event);
      default:
        break;
    }
  };

  watch(
    () => flowGraphStore.getFlowParams().opt,
    () => {
      opt.value = flowGraphStore.getFlowParams().opt;
    },
    { immediate: true },
  );
  watch(
    () => flowGraphStore.getFlowGraph().getDagExecutionInfo().dagStatus,
    () => {
      dagStatus.value = flowGraphStore.getFlowGraph().getDagExecutionInfo().dagStatus;
    },
    { immediate: true },
  );
</script>

<style xml:lang="scss" scoped>
  .bar {
    margin-right: 16px;
  }

  .item-space {
    margin-top: 7px !important;
    margin-left: 16px !important;
  }
</style>
