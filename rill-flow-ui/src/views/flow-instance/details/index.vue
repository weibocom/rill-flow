<template>
  <Dag
    :mode="MODE.INSTANCE"
  />
</template>

<script lang="ts" setup>
  import { Dag, MODE } from '/@/components/Dag'
  import {onMounted, provide, ref} from "vue";
  const route = useRoute()
  const { createMessage } = useMessage();
  const go = useGo();
  const {t} = useI18n();

  import {flowGroupDetailApi, flowInstanceDetailApi} from "@/api/table";
  import {useRoute} from "vue-router";
  import {useMessage} from "@/hooks/web/useMessage";
  import {useGo} from "@/hooks/web/usePage";
  import {useI18n} from "@/hooks/web/useI18n";
  /**
   * 类型
   * 节点分组信息
   * ID
   * 图信息
   */
  const nodeGroups = ref();
  const dagInfo = ref();

  onMounted(async () => {
    if (route.query.execution_id === undefined) {
      createMessage.warn(t('routes.flow.instances.graph.execution_detail_none_message'));
      go("/flow-instance/list");
      return;
    }
    const response = await flowInstanceDetailApi({"id": route.query.execution_id}, {});
    if (response.tasks === '{}') {
      createMessage.error(t('routes.flow.instances.graph.execution_detail_expire_message'));
      go("/flow-instance/list");
      return;
    }

    const groups = await flowGroupDetailApi();
    dagInfo.value = response
    nodeGroups.value = groups.data
  });

  provide("nodeGroups", nodeGroups)
  provide("dagInfo", dagInfo)

</script>
