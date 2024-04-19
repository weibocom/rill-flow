import {LAYOUT} from '/@/router/constant';

const microGraph: { redirect: string; path: string; component: () => Promise<any>; children: { path: string; component: () => Promise<any>; meta: { title: string }; name: string }[]; meta: { icon: string; title: string }; name: string } = {
  path: '/micro',
  name: 'micro',
  component: LAYOUT,
  redirect: '/flow-definition/list',
  meta: {
    hideMenu: true,
  },
  children: [
    {
      path: 'graph/',
      name: 'FlowGraphPage',
      component: () => import('@/views/micro/SubContainer.vue'),
      meta: {
        hideMenu: true,
        showMenu: false,
      },
    },
  ],
};

export default microGraph;
