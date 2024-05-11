<template>

  <Codemirror v-model:value="props.modelValue"
              :options="codeOptions"
              border
              style="width: 100%; height: 300px"
              @change="handleChange"/>
</template>
<script lang="ts" setup>

  import { defineEmits, defineProps } from "vue";
  import Codemirror from "codemirror-editor-vue3";
  import 'codemirror/addon/display/placeholder.js';
  import 'codemirror/mode/javascript/javascript.js';
  import 'codemirror/mode/python/python.js';
  import 'codemirror/mode/shell/shell.js';
  const props = defineProps({
    value: {
      type: String,
    },
    modelValue: {
      type: String,
    },
    fieldProps: {},
    codeOptions: {},
  });

  const emit = defineEmits(['update:modelValue']);

  function handleChange(e: string) {
    emit('update:modelValue', e);
  }
</script>

