<template>
  <div class="node " :class="status">
    <img class="logo" v-if="logoSwitch" :src="logo" alt="">
    <Icon v-if="!logoSwitch"
          :icon="logo"
          :size="18"
          class="logo pt-1"
    />
    <span class="label"></span>
    <span class="status">
          <img v-if="statusShow" :src="statusImg" alt="">
        </span>
  </div>
</template>

<script lang="ts" setup>
import {defineComponent, ref, inject} from 'vue';
import Icon from '@/components/Icon/Icon.vue';
import {NodeLogo, NodeStatus} from "@/components/FlowGraph/components/nodeConfig";

const status = ref("init");
const label = ref();
const logo = ref(NodeLogo.default);
const statusImg = ref();
const statusShow = ref(false);
const logoSwitch = ref(true);
const getNode = inject<() => any>('getNode');

if (getNode !== undefined) {
  status.value = getNode().store.data.attrs.status
  const type = getNode().store.data.attrs.type

  if (getNode().store.data.attrs.icon !== undefined) {
    logo.value = NodeLogo.prefix + getNode().store.data.attrs.icon
    logoSwitch.value = true
  } else if (type !== undefined && NodeLogo[type] !== undefined) {
    logo.value = NodeLogo[type]
    logoSwitch.value = false
  }

  if (NodeStatus[status.value] !== undefined) {
    statusImg.value = NodeStatus.prefix + NodeStatus[status.value]
    statusShow.value = true
  }
}


</script>
<style xml:lang="scss" scoped>
.node {
  display: flex;
  align-items: center;
  width: 100%;
  height: 100%;
  background-color: #fff;
  border: 1px solid #c2c8d5;
  border-left: 4px solid #5F95FF;
  border-radius: 4px;
  box-shadow: 0 2px 5px 1px rgba(0, 0, 0, 0.06);
}

.node img {
  width: 20px;
  height: 20px;
  flex-shrink: 0;
  margin-left: 8px;
}

.node .logo {
  width: 25px;
  height: 25px;
  flex-shrink: 0;
  margin-left: 8px;
}

.node .label {
  display: inline-block;
  flex-shrink: 0;
  width: 104px;
  margin-left: 8px;
  color: #666;
  font-size: 14px;
}

.node .status {
  flex-shrink: 0;
}

.node.SUCCEEDED {
  border-left: 4px solid #52c41a;
}

.node.SKIPPED {
  border-left: 4px solid #A67D3D;
}

.node.FAILED {
  border-left: 4px solid #ff4d4f;
}

.node.NOT_STARTED {
  border-left: 4px solid #545454;
}

.node.READY {
  border-left: 4px solid #FFFF00;
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
