<template>
  <div class="wrap">
    <div class="content">
      <div class="sider" v-if="config.showNodeGroups">
        <NodesBar />
      </div>

      <div class="panel">
        <!--流程图工具栏-->
        <div class="toolbar">
          <ToolBar />
        </div>
        <!--流程图画板-->
        <div ref="container" class="container"></div>
      </div>
    </div>
  </div>
  <NodeDefinitionModal class="definitionModal" />
  <NodeExecutionModal />
</template>

<script lang="ts" setup>
  import { useFlowStoreWithOut } from '../store/modules/flowGraphStore';
  import { onMounted, ref, watch } from 'vue';
  import { OptConfig, optConfig, OptEnum } from '../models/enums/optEnum';
  import { FlowParams } from '../models/flowParams';
  import { initFlowGraph, saveNodeGroups } from '../common/flowService';
  import NodesBar from './NodesBar/index.vue';
  import ToolBar from './ToolBar/index.vue';
  import NodeDefinitionModal from './modals/NodeDefinitionModal.vue';
  import NodeExecutionModal from './modals/NodeExecutionModal.vue';
  import { registerEventListener } from '../listener/eventListerer';
  const flowGraphStore = useFlowStoreWithOut();
  const params = ref<FlowParams>();
  params.value = flowGraphStore.getFlowParams();
  let config = ref<OptConfig>(optConfig[params.value.opt]);
  const container = ref(null);

  onMounted(() => {
    saveNodeGroups(params.value.queryTemplateNodesUrls);
    registerEventListener(container.value);
    if (flowGraphStore.getNodePrototypeRegistry().isEmpty()) {
      initFlowGraph(params.value, container.value);
    }
  });

  watch(
    () => flowGraphStore.getNodePrototypeRegistry().isEmpty(),
    () => {
      initFlowGraph(params.value, container.value);
    },
    { deep: true },
  );

  watch(
    () => flowGraphStore.getFlowParams().opt,
    () => {
      config.value = optConfig[params.value.opt];
    },
    { immediate: true },
  );
</script>

<style scoped xml:lang="less">
  .wrap {
    margin: 24px;
    background-color: #f7f9fb;
  }

  .wrap .content {
    display: flex;

    .sider {
      position: relative;

      z-index: 4;
      border: 1px solid rgb(0 0 0 / 8%);
      background-color: #f7f9fb;
    }

    .panel {
      height: 100%;
      width: 100%;
      position: relative;
      .toolbar {
        align-items: center;
        height: 38px;
        width: 100%;
        background-color: #f7f9fb;
        border-bottom: 1px solid rgb(0 0 0 / 8%);
        border-top: 1px solid rgb(0 0 0 / 8%);
      }

      .container {
        width: 100%;
      }
    }
  }

</style>
