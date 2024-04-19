<template>
    <div class="node">
      <img class="logo" v-if="icon?.startsWith('data')" :src="icon" alt="" />
      <component
        class="logo"
        v-if="icon?.startsWith('ant-design')"
        :is="icon.replace('ant-design:', '')"
      />
      <span class="label"> {{ label }}</span>
    </div>
</template>

<script lang="ts" setup>
  import { defineProps } from 'vue';

  const props = defineProps({
    label: { type: String },
    icon: { type: String },
    options: { type: Object },
    optShow: { type: Boolean },
  });
</script>

<style lang="less" scoped>
  div.node.node {
    display: flex;
    align-items: center;
    width: 100%;
    height: 50px;
    background-color: #fff;
    border: 1px solid #c2c8d5;
    border-left: 4px solid #5f95ff;
    border-radius: 4px;
    box-shadow: 0 2px 5px 1px rgba(0, 0, 0, 0.06);
    cursor: grab;
  }

  .node img {
    width: 20%;
    height: 20px;
    flex-shrink: 0;
    margin-left: 8px;
  }

  .node .logo {
    width: 20%;
    height: 25px;
    padding-top: 2px;
    padding-left: 10px;
    text-align: center;
    font-size: 18px;
  }

  .node .label {
    color: #666;
    height: 25px;
    font-size: 14px;
    padding-left: 10px;
    width: 80%;
  }

  .node .status {
    flex-shrink: 0;
  }

  .node.SUCCEEDED {
    border-left: 4px solid #52c41a;
  }

  .node.SKIPPED {
    border-left: 4px solid #a67d3d;
  }

  .node.FAILED {
    border-left: 4px solid #ff4d4f;
  }

  .node.NOT_STARTED {
    border-left: 4px solid #545454;
  }

  .node.READY {
    border-left: 4px solid #ffff00;
  }

  .node.running .status img {
    animation: spin 1s linear infinite;
  }

  .x6-node-selected .node {
    border-color: #1890ff;
    border-radius: 2px;
    box-shadow: 0 0 0 4px #d4e8fe;
  }

  .x6-node-selected .node.SUCCEEDED {
    border-color: #52c41a;
    border-radius: 2px;
    box-shadow: 0 0 0 4px #ccecc0;
  }

  .x6-node-selected .node.FAILED {
    border-color: #ff4d4f;
    border-radius: 2px;
    box-shadow: 0 0 0 4px #fedcdc;
  }

  .x6-edge:hover path:nth-child(2) {
    stroke: #1890ff;
    stroke-width: 1px;
  }

  .x6-edge-selected path:nth-child(2) {
    stroke: #1890ff;
    stroke-width: 1.5px !important;
  }

</style>
