<template>
  <div class="blockText">
    <a-typography-paragraph>
      <pre>{{ context!== undefined? context: '暂无' }}</pre>
    </a-typography-paragraph>
  </div>
</template>

<script>
import {Typography} from "ant-design-vue";

export default {
  name: "BlockText",
  props: ['context'], // Declare the 'label' prop,
  components: {
    ATypographyParagraph: Typography.Paragraph,
  },
  data() {
    return {}
  }
}

</script>

<style scoped  lang="less" >
.blockText {
  width: 100%;
}
</style>
