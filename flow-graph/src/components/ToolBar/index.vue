<template>
  <div class="bar">
    <a-tooltip class="showStatus mx-20" v-if="opt === 'display'">
      {{ t('toolBar.dagExecutionDetail.title') }}:
      <a-tag :color="dagStatusColor(dagStatus)">
        {{ dagStatus }}
      </a-tag>
    </a-tooltip>
    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <a-button name="edit" @click="handleClick" class="item-space" size="small">{{ t('toolBar.editDagMeta.title') }}</a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <a-button name="save" @click="handleClick" class="item-space" size="small">{{ t('toolBar.saveDag.title') }}</a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <a-button name="submitDagTestRun" @click="handleClick" class="item-space" size="small"
        >{{ t('toolBar.testRun.title') }}</a-button
      >
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt !== 'display'">
      <a-button name="toJSON" @click="handleClick" class="item-space" size="small"> {{ t('toolBar.showDag.title') }} </a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt === 'display'">
      <a-button name="executionDetail" @click="handleClick" class="item-space" size="small"> {{ t('toolBar.dagExecutionDetail.title') }} </a-button>
    </a-tooltip>

    <a-tooltip placement="bottom" v-if="opt === 'display'">
      <a-button name="refresh" @click="handleClick" class="item-space" size="small"> {{ t('toolBar.refresh') }}
      </a-button>
    </a-tooltip>

    <ShowDag />
    <SaveDag />
    <EditDagMeta />
    <DagTestRun />
    <DagExecutionModal />
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
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import { ref, watch } from "vue";
  import { dagStatusColor } from "@/src/common/dagStatusStyle";
  import { useI18n } from 'vue-i18n';
  const { t } = useI18n();
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
