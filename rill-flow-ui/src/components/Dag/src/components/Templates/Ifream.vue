<template>
  <div>
    <button @click="showModal">{{ text }}</button>
    <Modal v-model="modalVisible" @ok="handleOk" @cancel="handleCancel">
      hello {{ text }}
    </Modal>
  </div>
</template>

<script>
import { defineComponent, ref } from 'vue';
import {Modal, Button} from "ant-design-vue";

export default defineComponent({
  name: 'Ifream',
  props: ['text'], // 接受文本属性
  components: {
    Modal,
    Button
  },
  setup() {
    const modalVisible = ref(false);
    const modalText = ref('hello');

    const showModal = () => {
      modalVisible.value = true;
    };

    const handleOk = () => {
      // 处理模态框确定按钮的逻辑
      modalVisible.value = false;
    };

    const handleCancel = () => {
      // 处理模态框取消按钮的逻辑
      modalVisible.value = false;
    };

    return {
      modalVisible,
      modalText,
      showModal,
      handleOk,
      handleCancel,
    };
  },
});
</script>


<style scoped>

</style>
