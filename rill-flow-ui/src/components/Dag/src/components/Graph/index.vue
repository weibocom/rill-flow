<template>
  <div ref="container"></div>
</template>

<script lang="ts" setup>
import {ref, watch, inject} from 'vue';
import '@antv/x6-vue-shape';
import {MODE} from "../../typing";
import {initGraph} from "@/components/FlowGraph";
const container = ref(null);

const graph: any = inject('graph');
const initGraphParams: any = inject('initGraphParams');
const initGraphStatus: any = inject('initGraphStatus');
const dagInfo: any = inject('dagInfo');
const nodeGroups: any = inject('nodeGroups');

const props = defineProps({
  mode: {
    type: String as PropType<MODE>,
    default: MODE.INSTANCE,
  },
})

watch(() => dagInfo.value, (n) => {
  graph.value = initGraph(dagInfo.value, nodeGroups.value, container.value)
  initGraphStatus.value = true
}, {deep: true})

</script>
