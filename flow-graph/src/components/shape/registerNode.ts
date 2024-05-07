import { Graph } from '@antv/x6';
import '@antv/x6-vue-shape';
import NodeTemplate from './NodeTemplate.vue';

export function registerVueNode() {
  Graph.registerVueComponent(
    'vue-node',
    {
      template: `<NodeTemplate />`,
      components: {
        NodeTemplate,
      },
    },
    true,
  );
}
