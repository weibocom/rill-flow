<template>
  <a-tabs v-model:activeKey="activeKey">
    <a-tab-pane key="1" tab="基础模式">
      <VueForm v-model="outputData" :schema="ouputJsonSchema" :formProps="formProps">
        <div slot-scope="{ outputData }"></div>
      </VueForm>
    </a-tab-pane>
    <a-tab-pane key="2" tab="高级模式">
      <div>xxx</div>
      {{modelValue}}
      <Codemirror v-model:value="outputData"
                  :options="codeOptions"
                  border
                  style="width: 100%; height: 300px"
      />
    </a-tab-pane>
  </a-tabs>
</template>

<script lang="ts" setup>

  import Codemirror from "codemirror-editor-vue3";
  import VueForm from "@lljj/vue3-form-ant";
  import { defineProps, reactive, ref } from "vue";
  import { getOutputSchema } from "@/src/components/ToolBar/data.ts";
  const activeKey = ref('1');
  const formProps = ref({});
  const props = defineProps({
    modelValue: {
      type: Object,
    },
    fieldProps: {},
    formProps: {}
  });

  const codeOptions = ref({
    mode: 'application/json',
  });

  const ouputJsonSchema = ref(getOutputSchema());

  const outputData = ref("");
  let outputDataSchema = reactive({});
</script>

<style scoped lang="less">

</style>