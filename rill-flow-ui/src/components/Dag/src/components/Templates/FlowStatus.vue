<template>
  <Tag :color="statusColor" style="font-size: 14px">
    {{statusText}}
  </Tag>
</template>

<script>
import {Typography, Tag} from "ant-design-vue";
import { useI18n } from '/@/hooks/web/useI18n';

export default {
  name: "FlowStatus",
  props: ['status'],
  components: {
    ATypographyParagraph: Typography.Paragraph,
    Tag
  },
  computed: {
    statusColor() {
      const { t } = useI18n();
      let color = "processing";
      const status = (this.status || '').toUpperCase();

      if (status === "SUCCEED" || status === "SUCCEEDED") {
        color = "success";
      } else if (status === "FAILED") {
        color = "error";
      } else if (status === "READY") {
        color = "warning";
      } else if (status === "NOT_STARTED") {
        color = "#545454";
      } else if (status === "SKIPPED") {
        color = "#A67D3D";
      }

      return color;
    },
    statusText() {
      const { t } = useI18n();
      const status = (this.status || '').toUpperCase();

      if (status === "SUCCEED" || status === "SUCCEEDED") {
        return t('routes.flow.instances.status.SUCCEEDED');
      } else if (status === "FAILED") {
        return t('routes.flow.instances.status.FAILED');
      } else if (status === "READY") {
        return t('routes.flow.instances.status.READY');
      } else if (status === "NOT_STARTED") {
        return t('routes.flow.instances.status.NOT_STARTED');
      } else if (status === "SKIPPED") {
        return t('routes.flow.instances.status.SKIPPED');
      } else {
        return t('routes.flow.instances.status.RUNNING');
      }
    }
  }
}

</script>

<style scoped>

</style>
