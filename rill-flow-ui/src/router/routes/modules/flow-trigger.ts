import {LAYOUT} from '/@/router/constant';
import {t} from '/@/hooks/web/useI18n';
import {ExceptionEnum} from "@/enums/exceptionEnum";

const ExceptionPage = () => import('/@/views/sys/exception/Exception.vue');

const flowTrigger: { redirect: string; path: string; component: () => Promise<any>; children: { path: string; component: () => Promise<any>; meta: { title: string }; name: string }[]; meta: { icon: string; title: string }; name: string } = {
  path: '/flow-trigger',
  name: 'flow-trigger',
  component: LAYOUT,
  redirect: '/flow-trigger/index',
  meta: {
    icon: 'ion:key-outline',
    title: t('routes.flow.trigger.index'),
    orderNo: 200,
  },
  children: [
    {
      path: 'index',
      name: 'FlowTriggerPage',
      meta: {
        title: t('routes.flow.trigger.list'),
      },
      component: ExceptionPage,
      props: {
        status: ExceptionEnum.DEVELOPING,
      },
    },
  ],
};

export default flowTrigger;
