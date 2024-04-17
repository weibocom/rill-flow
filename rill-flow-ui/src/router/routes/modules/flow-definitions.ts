import { LAYOUT } from '/@/router/constant';
import { t } from '/@/hooks/web/useI18n';

const flowDefinitions: { redirect: string; path: string; component: () => Promise<any>; children: { path: string; component: () => Promise<any>; meta: { title: string }; name: string }[]; meta: { icon: string; title: string }; name: string } = {
  path: '/flow-definition',
  name: 'flow-definition',
  component: LAYOUT,
  redirect: '/flow-definition/list',
  meta: {
    icon: 'tabler:chart-dots',
    title: t('routes.flow.definitions.record'),
    orderNo: 100,
  },
  children: [
    {
      path: 'list',
      name: 'FlowDefinitionPage',
      meta: {
        title: t('routes.flow.definitions.list'),
        open: true,
      },
      component: () => import('/@/views/flow-definition/index.vue'),
    },
    {
      path: 'node-templetes',
      name: 'FlowNodeTempletes',
      component: () => import('@/views/flow-definition/node-templetes/index.vue'),
      meta: {
        title: t('routes.flow.definitions.node_templetes'),
        open: true,
      },
    },
  ],
};

export default flowDefinitions;
