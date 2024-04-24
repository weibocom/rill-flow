<template>
  <FlowGraph />
</template>

<script setup>
  import FlowGraph from './components/FlowGraph.vue';
</script>

<style scoped>
  .logo {
    height: 6em;
    padding: 1.5em;
    will-change: filter;
    transition: filter 300ms;
  }
  .logo:hover {
    filter: drop-shadow(0 0 2em #646cffaa);
  }
  .logo.vue:hover {
    filter: drop-shadow(0 0 2em #42b883aa);
  }
  .ant-form-item {
    margin-bottom: 0 !important;
  }
  .ant-form-item-control {
    height: 0 !important;
  }
</style>
