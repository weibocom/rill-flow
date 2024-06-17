<template>
  <div class="title">{{t('nodeBar.list')}}</div>
  <div class="context">
    <a-tabs v-model:activeKey="activeKey">
      <a-tab-pane
        v-for="nodeCategory in Object.values(NodeCategory)"
        :key="nodeCategory"
        :tab="getNodeCategoryName(nodeCategory)"
      >
        <div
          class="tab-pane"
          v-for="nodeVO in nodePrototypeMap.get(nodeCategory)"
          :key="nodeVO.id"
          @mousedown="startDrag(nodeVO, $event)"
        >
          <NodeTemplate :label="nodeVO.title" :icon="nodeVO.icon" />
        </div>
      </a-tab-pane>
    </a-tabs>
  </div>
</template>

<script lang="ts" setup>
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import NodeTemplate from './NodeTemplate.vue';
  import { NodePrototype } from '../../models/nodeTemplate';
  import {
    getNodeCategoryByNumber,
    getNodeCategoryName,
    NodeCategory,
  } from '../../models/enums/nodeCategory';
  import { ref, watch } from 'vue';
  import { useI18n } from 'vue-i18n';
  const { t } = useI18n();

  const flowGraphStore = useFlowStoreWithOut();
  const nodePrototypeRegistry = ref(flowGraphStore.getNodePrototypeRegistry());

  const nodePrototypeMap = ref(new Map<NodeCategory, NodeVO[]>());
  const activeKey = ref('0');

  watch(
    () => nodePrototypeRegistry.value.getNodePrototypes(),
    () => {
      if (flowGraphStore.getNodePrototypeRegistry().isEmpty()) {
        return;
      }
      nodePrototypeMap.value = new Map<NodeCategory, NodeVO[]>();
      nodePrototypeRegistry.value.getNodePrototypes().forEach((nodePrototype) => {
        const nodeCategoryNumber = nodePrototype.node_category;
        const nodeCategory = getNodeCategoryByNumber(nodeCategoryNumber);
        if (nodeCategory === undefined) {
          return;
        }
        const nodeVO = getNodeVOByPrototype(nodePrototype, nodeCategory);

        const nodeVOs = nodePrototypeMap.value.get(nodeCategory);
        if (nodeVOs === undefined) {
          nodePrototypeMap.value.set(nodeCategory, [nodeVO]);
        } else {
          nodeVOs.push(nodeVO);
          nodePrototypeMap.value.set(nodeCategory, nodeVOs);
        }
      });
    },
    { deep: true },
  );

  class NodeVO {
    id: string;
    nodeCategory: NodeCategory;
    name: string;
    title: string;
    icon: string;
  }

  function getNodeVOByPrototype(nodePrototype: NodePrototype, nodeCategory: NodeCategory) {
    const nodeVO = new NodeVO();
    nodeVO.id = nodePrototype.id;
    nodeVO.nodeCategory = nodeCategory;
    if (nodeVO.nodeCategory === NodeCategory.BASE_NODE) {
      nodeVO.name = nodePrototype.meta_data.category;
      nodeVO.title = nodePrototype.meta_data.category;
    } else {
      nodeVO.name = '';
      nodeVO.title = nodePrototype.template.name;
    }

    nodeVO.icon = nodePrototype.icon;
    return nodeVO;
  }

  function startDrag(nodeVO: NodeVO, event: MouseEvent) {
    const flowGraphStore = useFlowStoreWithOut();
    const flowGraph = flowGraphStore.getFlowGraph();
    flowGraph.addNodeByPrototype(nodeVO.id, event);
  }
</script>

<style lang="less" scoped>
  .title {
    text-align: center;
    height: 38px;
    background-color: #f7f9fb;
    border-bottom: 1px solid rgb(0 0 0 / 8%);
    padding-top: 8px;
    width: 200px
  }
  .context {
    margin: 10px;

    .tab-pane {
      margin-top: 10px;
    }
  }

</style>
