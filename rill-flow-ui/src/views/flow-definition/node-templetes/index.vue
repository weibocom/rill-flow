<template>
  <BasicTable @register="registerTable">
    <template #bodyCell="{ column, record }">
      <template v-if="column.key === 'action'">
        <TableAction
          stopButtonPropagation
          :actions="[
            {
              label: t('routes.flow.definitions.node_templates_detail.option.edit'),
              ifShow: (_action) => {
                return record.node_type !== 'meta';
              },
              onClick: handleEdit.bind(null, record),
            },
            {
              label: t('routes.flow.definitions.node_templates_detail.option.enable'),
              ifShow: (_action) => {
                return record.node_type !== 'meta' && record.enable === 1;
              },
              onClick: handleEnableDisable.bind(null, record.id, false),
            },
            {
              label: t('routes.flow.definitions.node_templates_detail.option.disable'),
              ifShow: (_action) => {
                return record.node_type !== 'meta' && record.enable === 0;
              },
              onClick: handleEnableDisable.bind(null, record.id, true),
            },
          ]"
          :dropDownActions="[]"
        />
        <TaskTemplateEditDrawer @register="registerTaskTemplateEditDrawer" @response="reloadPage" />
      </template>
    </template>
    <template #enable="{ record }">
      {{ record.enable === 1 ? t('routes.flow.definitions.node_templates_detail.option.enable') : t('routes.flow.definitions.node_templates_detail.option.disable') }}
    </template>
    <template #node_type="{ record }">
      {{ record.node_type === 'meta' ? t('routes.flow.definitions.node_templates_detail.node_type.meta') : t('routes.flow.definitions.node_templates_detail.node_type.template') }}
    </template>
    <template #toolbar>
      <Tooltip>
        <template #title>
          <p>{{t('routes.flow.definitions.node_templates_detail.option.create')}}</p>
        </template>
        <a-button type="primary" @click="handleCreate()"> {{t('routes.flow.definitions.node_templates_detail.option.create')}} </a-button>
      </Tooltip>
    </template>
  </BasicTable>
</template>

<script lang="ts">
  import { defineComponent } from 'vue';
  import { BasicTable, TableAction, useTable } from '/@/components/Table';
  import { getBasicColumns, getFormConfig } from './tableData';
  import { Drawer, Tooltip } from 'ant-design-vue';
  import { useI18n } from '/@/hooks/web/useI18n';

  import { disableTemplateApi, enableTemplateApi, templateListApi } from '@/api/table';
  import TaskTemplateEditDrawer from '@/views/flow-definition/node-templetes/taskTemplateEditDrawer.vue';
  import { useDrawer } from '@/components/Drawer';

  export default defineComponent({
    components: { TableAction, BasicTable, Drawer, TaskTemplateEditDrawer, Tooltip },
    setup() {
      const { t } = useI18n();
      const [registerTable, { reload }] = useTable({
        api: templateListApi,
        fetchSetting: {
          listField: 'data',
        },
        columns: getBasicColumns(),
        useSearchForm: true,
        formConfig: getFormConfig(),
        showTableSetting: true,
        tableSetting: { fullScreen: true },
        showIndexColumn: false,
        pagination: { pageSize: 20 },
        rowKey: 'name',
        actionColumn: {
          width: 100,
          title: t('routes.flow.definitions.node_templates_detail.opt'),
          dataIndex: 'action',
          fixed: 'right',
        },
      });

      const [registerTaskTemplateEditDrawer, { openDrawer }] = useDrawer();

      function handleEdit(record: Recordable) {
        openDrawer(true, {
          action: 'update',
          ...record,
        });
      }

      function handleCreate() {
        openDrawer(true, {
          action: 'create',
        });
      }

      async function handleEnableDisable(id, enable) {
        let res;
        if (enable) {
          res = await enableTemplateApi(id);
        } else {
          res = await disableTemplateApi(id);
        }
        console.log(res);
        reloadPage();
      }

      function reloadPage() {
        reload();
      }

      return {
        registerTaskTemplateEditDrawer,
        registerTable,
        handleEdit,
        handleCreate,
        handleEnableDisable,
        reloadPage,
        t,
      };
    },
  });
</script>
