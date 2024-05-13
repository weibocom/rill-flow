<template>
  <div id="sub-container" class="demobox"></div>
</template>

<script lang="ts" setup>
  import { onMounted, toRaw } from 'vue';

  import { start } from 'qiankun';
  import { registerApps } from './qiankun';
  import config from './config';
  import { useFlowGraphStoreWithOut } from '@/store/modules/flowGraphStore';
  import { useLocale } from '@/locales/useLocale';

  const subApps: any[] = [];
  const { flowGraphApp } = config;
  onMounted(() => {
    const flowGraphStore = useFlowGraphStoreWithOut();
    flowGraphApp.props['flowParams'] = toRaw(flowGraphStore.getFlowGraphParams);
    flowGraphApp.props['language'] = useLocale().getLocale.value;
    subApps.push(flowGraphApp);
    if (!window.qiankunStarted) {
      window.qiankunStarted = true;
      registerApps(subApps);
      start({
        sandbox: {
          experimentalStyleIsolation: false, // 样式隔离
        },
      });
    }
  });
</script>

<style scoped lang="less"></style>
