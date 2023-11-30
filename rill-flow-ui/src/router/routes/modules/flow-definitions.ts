import {LAYOUT} from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';
import {ExceptionEnum} from "@/enums/exceptionEnum";
const ExceptionPage = () => import('/@/views/sys/exception/Exception.vue');

const flowDefinitions: { redirect: string; path: string; component: () => Promise<any>; children: { path: string; component: () => Promise<any>; meta: { title: string }; name: string }[]; meta: { icon: string; title: string }; name: string } = {
  path: '/flow-definitions',
  name: 'flow-definition',
  component: LAYOUT,
  redirect: '/flow-definitions/index',
  meta: {
    icon: 'tabler:chart-dots',
    title: t('routes.flow.definitions.record'),
    orderNo: 100,
  },
  children: [
    {
      path: 'index',
      name: 'FlowDefinitionPage',
      meta: {
        title: t('routes.flow.definitions.list'),
      },
      component: ExceptionPage,
      props: {
        status: ExceptionEnum.DEVELOPING,
      },
    },
  ],
};

export default flowDefinitions;
