import type { App } from 'vue';
import { Button } from './Button';
import {
  Input,
  Layout,
  InputNumber,
  Form,
  Modal,
  Select,
  Radio,
  TreeSelect,
  Tabs,
  AutoComplete,
  Rate,
  Switch,
  Card,
  Tooltip,
} from 'ant-design-vue';
import VXETable from 'vxe-table';
import CodeDiff from 'v-code-diff';

export function registerGlobComp(app: App) {
  app
    .use(Input)
    .use(Button)
    .use(Layout)
    .use(VXETable)
    .use(InputNumber)
    .use(Form)
    .use(Modal)
    .use(Select)
    .use(Radio)
    .use(TreeSelect)
    .use(AutoComplete)
    .use(Rate)
    .use(Switch)
    .use(Tabs)
    .use(CodeDiff)
    .use(Tooltip)
    .use(Card);
}
