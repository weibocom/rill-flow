<template>
    <div class="node flex" :class="status">
      <span class="logo" ref="elRef" v-if="!base64IconShow" style="font-size: 20px"></span>
      <img class="img-logo" v-if="base64IconShow" :src="base64Icon" alt="" style="font-size: 20px"/>
      <span class="label"></span>
      <span class="status">
        <img class="node-status" v-if="statusShow" :src="statusImg" alt="" />
      </span>
    </div>
</template>

<script lang="ts" setup>
  import { inject, onMounted, ref, unref } from 'vue';
  import { NodeLogo, NodeStatus } from "./nodeConfig";
  import Iconify from '@purge-icons/generated';

  const getNode = inject<() => any>('getNode');
  const statusShow = ref(false);
  const statusImg = ref();
  const status = ref(undefined);
  const name = ref(undefined);
  const svg = ref(null);
  const elRef = ref(null);
  const base64Icon = ref(null);
  const base64IconShow = ref(false);

  onMounted(() => {
    if (getNode !== undefined) {
      status.value = getNode().store.data.data.status;
      name.value = getNode().store.data.data.name;

      if (NodeStatus[status.value] !== undefined) {
        statusImg.value = NodeStatus.prefix + NodeStatus[status.value];
        statusShow.value = true;
      }
      console.log('icon ===> ', getNode().store.data.data, base64Icon.value);
      if (getNode().store.data.data?.icon !== undefined && !getNode().store.data.data?.icon.startsWith('ant-design')) {
        base64Icon.value = NodeLogo.default;
        base64Icon.value = NodeLogo.prefix + getNode().store.data.data?.icon;

        console.log('icon', getNode().store.data.data, base64Icon.value);
        base64IconShow.value = true;
      } else {
        const el: any = unref(elRef);
        const icon = getNode().store.data.data.icon;
        svg.value = Iconify.renderSVG(icon, {});
        const span = document.createElement('span');
        span.className = 'iconify';
        span.dataset.icon = icon;
        el.textContent = '';
        el.appendChild(span);
      }

    }

  });
</script>
<style xml:lang="scss" scoped>

  .node {
    display: flex;
    align-items: center;
    width: 100%;
    height: 100%;
    background-color: #fff;
    border: 1px solid #c2c8d5;
    border-left: 4px solid #5f95ff;
    border-radius: 4px;
    box-shadow: 0 2px 5px 1px rgba(0, 0, 0, 0.06);
    cursor: grab;
  }

  .node .status .node-status {
    height: 25px;
    flex-shrink: 0;
    margin-right: 8px;
  }

  .node .logo {
    width: 25px;
    flex-shrink: 0;
    margin-left: 4px;
    padding-top: 4px;
  }

  .node .img-logo {
    width: 25px;
    flex-shrink: 0;
    margin-left: 4px;
  }

  .node .status {
    height: 25px;
  }

  .node .label {
    display: inline-block;
    flex-shrink: 0;
    width: 70%;
    padding-left: 8px;
    color: #666;
    font-size: 14px;
  }

  .node .options {
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

  @keyframes running-line {
    to {
      stroke-dashoffset: -1000;
    }
  }

  @keyframes spin {
    from {
      transform: rotate(0deg);
    }
    to {
      transform: rotate(360deg);
    }
  }
</style>
