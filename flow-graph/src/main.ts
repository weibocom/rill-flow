import { createApp, nextTick } from "vue";
import App from './App.vue';
import router from './router';
import { renderWithQiankun, qiankunWindow } from 'vite-plugin-qiankun/dist/helper';
import { setupStore } from './store';
import { useFlowStoreWithOut } from './store/modules/flowGraphStore';
import { FlowParams } from './models/flowParams';
import Antd from 'ant-design-vue';
import 'ant-design-vue/dist/antd.css';

import * as Icons from '@ant-design/icons-vue';
let app;
const flowGraphStore = useFlowStoreWithOut();

const render = (container) => {
  app = createApp(App);
  setupStore(app);
  app.use(router)
    .use(Antd)
    .mount(container ? container.querySelector('#flow-graph') : '#flow-graph');

  nextTick(() => {
    for (const i in Icons) {
      app.component(i, Icons[i]);
    }
  });
};

const initQianKun = () => {
  renderWithQiankun({
    mount(props) {
      console.log('flow-graph-props', props);
      const { container } = props;
      const originParams: FlowParams = props['flowParams'];
      let params: FlowParams;
      if (originParams?.id === undefined) {
        // 刷新页面的时候，如果没有传入id，则从localStorage中获取
        params = flowGraphStore.getFlowParams();
      } else {
        params = new FlowParams(
          originParams.id,
          originParams.opt,
          originParams.flowApiHost,
          originParams.queryDagUrl,
          originParams.submitDagUrl,
          originParams.queryExecutionUrl,
          originParams.queryMetaNodesUrl,
          originParams.submitExecuteUrl,
          originParams.queryTemplateNodesUrls,
        );
      }

      flowGraphStore.setFlowParams(params);
      render(container);
    },
    bootstrap() {},
    unmount() {
      app.unmount();
    },
  });
};

qiankunWindow.__POWERED_BY_QIANKUN__ ? initQianKun() : render();
