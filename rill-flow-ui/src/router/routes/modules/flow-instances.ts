import { LAYOUT } from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';

const flowInstances: { redirect: string; path: string; component: () => Promise<any>; children: ({ path: string; component: () => Promise<any>; meta: { title: string }; name: string } | { path: string; component: () => Promise<any>; meta: { title: string }; name: string })[]; meta: { icon: string; title: string }; name: string } = {
  path: '/flow-instance',
  name: 'flow',
  component: LAYOUT,
  redirect: '/flow-instance/list',
  meta: {
    icon: 'ion:bar-chart-outline',
    title: t('routes.flow.instances.detail'),
    orderNo: 300,
  },
  children: [
    {
      path: 'list',
      name: 'FlowInstanceList',
      component: () => import('/@/views/flow-instance/index.vue'),
      meta: {
        title: t('routes.flow.instances.record'),
        open: true
      },
    },
    {
      path: 'detail',
      name: 'FlowPage',
      component: () => import('@/views/flow-instance/details/index.vue'),
      meta: {
        title: t('routes.flow.instances.record_detail'),
        hideMenu: true,
        ignoreKeepAlive: true,
        showMenu: false,
        currentActiveMenu: '/flow-instance/list',
      },
    },
    ],
};

export default flowInstances;
