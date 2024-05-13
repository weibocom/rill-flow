<template>
  <a-modal
    v-model:visible="open"
    wrap-class-name="full-modal-to-xl"
    :title="t('toolBar.showDag.detail')"
    width="70%"
    :footer="null"
  >
    <a-card>
      <a-tabs v-model:activeKey="activeKey">
        <a-tab-pane key="1" tab="Yaml">
          <Codemirror v-model:value="yamlCode" :options="yamlOptions" border />
        </a-tab-pane>
        <a-tab-pane key="2" tab="JSON">
          <Codemirror v-model:value="jsonCode" :options="jsonOptions" border />
        </a-tab-pane>
      </a-tabs>
    </a-card>
  </a-modal>
</template>

<script lang="ts" setup>
  import { ref } from 'vue';
  import { Channel } from '../../common/transmit';
  import { CustomEventTypeEnum } from '../../common/enums';
  import { useFlowStoreWithOut } from '../../store/modules/flowGraphStore';
  import Codemirror from 'codemirror-editor-vue3';
  import 'codemirror/addon/display/placeholder.js';
  import 'codemirror/mode/javascript/javascript.js';
  import { useI18n } from "vue-i18n";
  const { t } = useI18n();

  const activeKey = ref('1');
  const open = ref<boolean>(false);
  const jsonOptions = ref({
    mode: 'application/json',
    readOnly: true,
  });
  const yamlOptions = ref({
    mode: 'yaml',
    readOnly: true,
  });
  const jsonCode = ref({});
  const yamlCode = ref({});
  const dagId = ref();
  Channel.eventListener(CustomEventTypeEnum.TOOL_BAR_SHOW_DAG, () => {
    const flowGraphStore = useFlowStoreWithOut();
    dagId.value = flowGraphStore.flowParams.id;
    jsonCode.value = JSON.stringify(flowGraphStore.getFlowGraph().toJSON(), null, 2);
    yamlCode.value = flowGraphStore.getFlowGraph().toYaml();
    open.value = true;
  });
</script>

<style scoped xml:lang="scss"></style>
