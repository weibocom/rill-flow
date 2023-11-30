<template>
  <BasicTable @register="registerTable">
    <template #bodyCell="{ column, record }">
      <template v-if="column.key === 'status'">
        <FlowStatus :status="record.status"/>
      </template>
      <template v-if="column.key === 'action'">
        <TableAction
          stopButtonPropagation
          :actions="[
              {
                label: t('routes.flow.instances.option.detail'),
                onClick: handleDetail.bind(null, record)
              },
            ]"
          :dropDownActions="[]"
        />
      </template>
    </template>
  </BasicTable>
</template>
<script lang="ts" setup>
import {BasicTable, useTable, TableAction} from '/@/components/Table';
import {getInstanceColumns, getFormConfig} from './methods';
import FlowStatus from "@/components/Dag/src/components/Templates/FlowStatus.vue";

import {instanceListApi} from '@/api/table';
import {useGo} from "@/hooks/web/usePage";
import {useI18n} from '/@/hooks/web/useI18n';

const {t} = useI18n();
const go = useGo();

const [registerTable] = useTable({
  title: t('routes.flow.instances.record'),
  api: instanceListApi,
  columns: getInstanceColumns(),
  useSearchForm: true,
  formConfig: getFormConfig(),
  showTableSetting: false,
  tableSetting: {fullScreen: true},
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
  go({
    "path": "/flow-instance/detail",
    "query": {
      "execution_id": record.execution_id
    }
  });
}
</script>
