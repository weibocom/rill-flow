<template>
  <BasicModal
    v-bind="$attrs"
    destroyOnClose
    @register="register"
    :title="t('routes.flow.definitions.node_templates_detail.option.preview_json_schema')"
  >
    <VueForm :formData="formData" :schema="templateSchema" :formProps="formProps">
      <template #default="{}">
        <div></div>
      </template>
    </VueForm>
  </BasicModal>
</template>
<script lang="ts" setup>
  import { BasicModal, useModalInner } from '@/components/Modal';
  import VueForm from '@lljj/vue3-form-ant';
  import { ref } from 'vue';
  import { templateSchema } from '@/views/flow-definition/node-templetes/tableData';
  import { useI18n } from '/@/hooks/web/useI18n';

  const [register] = useModalInner();
  const { t } = useI18n();

  const formData = {};
  const formProps = ref();
  formProps.value = {
    labelPosition: 'right',
    labelSuffix: '：',
    hideSave: true,
    hideCancel: true,
  };
</script>
