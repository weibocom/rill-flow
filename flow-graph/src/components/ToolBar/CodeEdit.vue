<template>
  <a-modal v-model:visible="open" title="测试任务" width="70%" @ok="handleOk()">
    <a-card>
      <CodeEditWidget/>
    </a-card>
  </a-modal>
</template>

<script lang="ts" setup>
import { ref } from 'vue';
import { Channel } from '../../common/transmit';
import { CustomEventTypeEnum } from '../../common/enums';
import CodeEditWidget from "@/src/components/Widget/CodeEditWidget.vue";

const open = ref<boolean>(false);
const form = ref();

Channel.eventListener(CustomEventTypeEnum.TOOL_BAR_CODE_EDIT, () => {
  open.value = true;
});

const handleOk = () => {
  open.value = false;
};

</script>

<style scoped xml.lang="less"></style>
