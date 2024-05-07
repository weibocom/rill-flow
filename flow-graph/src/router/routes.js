const routes = [
  {
    path: '/flow-graph/',
    component: () => import('@/src/components/FlowGraph.vue'),
    meta: { title: 'FlowGraph' },
  },
];

export default routes;
