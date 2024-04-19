<template>
  <div>
    <a-tree-select
      v-model:value="props.modelValue"
      show-search
      style="width: 100%"
      :dropdown-style="{ maxHeight: '400px', overflow: 'auto' }"
      placeholder="Please select"
      allow-clear
      :tree-data="treeData"
      :field-names="{
        children: 'children',
        value: 'value',
        label: 'title',
      }"
      tree-node-filter-prop="title"
      @change="handleChange"
    >
      <template #suffixIcon><SmileOutlined /></template>
    </a-tree-select>
  </div>
</template>

<script lang="ts" setup>
  import { defineProps, ref, defineEmits } from 'vue';

  const props = defineProps({
    value: {
      type: String,
    },
    modelValue: {
      type: String,
    },
    fieldProps: {},
    treeData: {},
  });

  const emit = defineEmits(['update:modelValue']);

  function handleChange(e: string) {
    emit('update:modelValue', e);
  }
</script>

<style scoped></style>
