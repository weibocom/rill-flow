<template>
  <BasicTable @register="registerTable1">
    <template #bodyCell="{ column, record }">
      <template v-if="column.key === 'status'">
        <FlowStatus :status="record.status" />
      </template>
      <template v-if="column.key === 'action'">
        <TableAction
          stopButtonPropagation
          :actions="[
            {
              label: t('routes.flow.definitions.option.edit'),
              onClick: handleDetail.bind(null, record),
            },
            {
              label: t('routes.flow.definitions.option.versions'),
              onClick: handleVersion.bind(null, record),
            },
            {
              label: t('routes.flow.definitions.option.showYaml'),
              onClick: handleShowYaml.bind(null, record),
            }
          ]"
          :dropDownActions="[]"
        />
        <Versions @register="register" />
        <FlowDetail @register="register1" />
      </template>
    </template>
    <template #toolbar>
      <Tooltip>
        <a-button type="primary" @click="handleCreate()"> {{t('routes.flow.definitions.option.create')}} </a-button>
      </Tooltip>
    </template>
  </BasicTable>
</template>
<script lang="ts" setup>
  import { BasicTable, useTable, TableAction } from '/@/components/Table';
  import { getInstanceColumns, getFormConfig, getFlowApiHost } from "./methods";
  import FlowStatus from '@/components/Templates/FlowStatus.vue';

  import { definitionListApi, getFlowDetailApi, getFlowVersionsApi } from '@/api/table';
  import { useGo } from '@/hooks/web/usePage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useModal } from '@/components/Modal';
  import Versions from './Versions.vue';
  import FlowDetail from '@/views/flow-definition/FlowDetail.vue';
  import { Tooltip } from 'ant-design-vue';
  import { useFlowGraphStoreWithOut } from '@/store/modules/flowGraphStore';
  import { OptEnum } from '@/enums/FlowEnum';
  import { FlowParams } from '@/modules/flowParams';
  import { type Recordable } from '@vben/types';
  import { useRouter } from 'vue-router';

  const { t } = useI18n();
  const go = useGo();
  const flowGraphStore = useFlowGraphStoreWithOut();
  const router = useRouter();

  const [registerTable1] = useTable({
    title: t('routes.flow.definitions.list'),
    api: definitionListApi,
    columns: getInstanceColumns(),
    useSearchForm: true,
    formConfig: getFormConfig(),
    showTableSetting: false,
    tableSetting: { fullScreen: true },
    showIndexColumn: false,
    rowKey: 'descriptor_id',
    actionColumn: {
      width: 100,
      title: t('routes.flow.instances.opt'),
      dataIndex: 'action',
      fixed: 'right',
    },
  });

  function handleDetail(record: Recordable) {
    console.log('router.currentRoute.value.fullPath',router.currentRoute.value.fullPath, window.location.origin, import.meta.env.MODE, getFlowApiHost());
    flowGraphStore.setFlowGraphParams(
      new FlowParams(
        record.descriptor_id,
        OptEnum.EDIT,
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
  function handleCreate() {
    flowGraphStore.setFlowGraphParams(
      new FlowParams(
        '',
        OptEnum.CREATE,
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

  const [register, { openModal: openModal }] = useModal();
  const [register1, { openModal: openModal1 }] = useModal();
  function handleVersion(record: Recordable) {
    getFlowVersionsApi({
      business_id: record.business_id,
      feature_name: record.feature_id,
      alias: record.alias,
    }).then((result) => {
      openModal(true, {
        feature: result.feature,
        alias: result.alias,
        business_id: result.business_id,
        versions: result.versions,
      });
    });
  }

  function handleShowYaml(record: Recordable) {
    getFlowDetailApi({
      descriptor_id: record.descriptor_id,
    }).then((result) => {
      openModal1(true, {
        data: result,
      });
    });
  }
</script>
