<template>
  <a-modal v-model:visible="open" title="代码执行" width="70%" :footer=null>
    <a-card>
      <CodeEditWidget :show="open" @update:show="handleOk"/>
    </a-card>
  </a-modal>
</template>

<script lang="ts" setup>
import { ref } from 'vue';
import { Channel } from '../../common/transmit';
import { CustomEventTypeEnum } from '../../common/enums';
import CodeEditWidget from "@/src/components/Widget/CodeEditWidget.vue";
import { useFlowStoreWithOut } from "@/src/store/modules/flowGraphStore";
import { getOptEnumByOpt, OptEnum } from "@/src/models/enums/optEnum";

const open = ref<boolean>(false);
const form = ref();
const nodeRef = ref(null);
const nodeType = ref(0);
function isOpenModel(opt: OptEnum) {
  return opt === OptEnum.CREATE || opt === OptEnum.EDIT;
}

Channel.eventListener(CustomEventTypeEnum.NODE_CLICK, (nodeCell) => {
  console.log("========== codeEdit")
  const flowGraphStore = useFlowStoreWithOut();
  nodeRef.value = nodeCell;
  if (!isOpenModel(getOptEnumByOpt(flowGraphStore.getFlowParams().opt))) {
    return;
  }


  const node = flowGraphStore.getFlowGraph().getNode(nodeCell.id);
  const nodePrototype = flowGraphStore
    .getNodePrototypeRegistry()
    .getNodePrototype(nodeCell.getData().nodePrototype + '');
  console.log("NodeDefinitionModal", nodeCell, nodePrototype)
  nodeType.value = nodePrototype.template?.type
  if (nodePrototype.template?.type !== 3) {
    open.value = false;
    return;
  }
  open.value = true;
});

const handleOk = () => {
  open.value = false;
};

</script>

<style scoped xml.lang="less"></style>
