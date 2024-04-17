<template>
  <BasicTable @register="registerTable">
    <template #bodyCell="{ column, record }">
      <template v-if="column.key === 'status'">
        <FlowStatus :status="record.status" />
      </template>
      <template v-if="column.key === 'action'">
        <TableAction
          stopButtonPropagation
          :actions="[
            {
              label: t('routes.flow.instances.option.detail'),
              onClick: handleDetail.bind(null, record),
            },
          ]"
          :dropDownActions="[]"
        />
      </template>
    </template>
  </BasicTable>
</template>
<script lang="ts" setup>
  import { BasicTable, useTable, TableAction } from '/@/components/Table';
  import { getInstanceColumns, getFormConfig } from './methods';
  import FlowStatus from '@/components/Templates/FlowStatus.vue';

  import { instanceListApi } from '@/api/table';
  import { useGo } from '@/hooks/web/usePage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { FlowParams } from '@/modules/flowParams';
  import { OptEnum } from '@/enums/FlowEnum';
  import { useFlowGraphStoreWithOut } from '@/store/modules/flowGraphStore';
  import { getFlowApiHost } from '@/views/flow-definition/methods';

  const { t } = useI18n();
  const go = useGo();
  const flowGraphStore = useFlowGraphStoreWithOut();

  const [registerTable] = useTable({
    title: t('routes.flow.instances.record'),
    api: instanceListApi,
    columns: getInstanceColumns(),
    useSearchForm: true,
    formConfig: getFormConfig(),
    showTableSetting: false,
    tableSetting: { fullScreen: true },
    showIndexColumn: false,
    rowKey: 'execution_id',
    actionColumn: {
      width: 100,
      title: t('routes.flow.instances.opt'),
      dataIndex: 'action',
      fixed: 'right',
    },
  });

  function handleDetail(record: Recordable) {
    flowGraphStore.setFlowGraphParams(
      new FlowParams(
        record.execution_id,
        OptEnum.DISPLAY,
        getFlowApiHost(),
        import.meta.env.VITE_QUERY_DAG_URL,
        import.meta.env.VITE_SUBMIT_DAG_URL,
        import.meta.env.VITE_SUBMIT_EXECUTE_URL,
        import.meta.env.VITE_QUERY_EXECUTION_URL,
        import.meta.env.VITE_QUERY_META_NODES_URL,
        import.meta.env.VITE_QUERY_TEMPLATE_NODES_URLS.split(',').filter((item) => item !== ''),
      ),
    );
    go({
      path: '/micro/graph/',
    });
  }
</script>
