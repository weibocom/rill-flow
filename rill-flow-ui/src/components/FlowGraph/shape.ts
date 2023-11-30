import {Dom, Graph, Node} from '@antv/x6';
import NodeTemplate from "./components/NodeTemplate.vue";
import '@antv/x6-vue-shape'
import {createVNode} from "vue";
import {buildUUID} from "@/utils/uuid";

Graph.registerNode("flow-node", {  //将vue组件注册到系统中
  inherit: "vue-shape",  //指定节点类型为vue-shape
  width: 200,
  height: 50,
  component: {
    render: () => {
      return createVNode(NodeTemplate);
    }
  },
});

Graph.registerNode('flow-chart-rect', {
  inherit: 'rect',
  width: 80,
  height: 42,
  attrs: {
    body: {
      stroke: '#5F95FF',
      strokeWidth: 1,
      fill: '#ffffff',
    },
    fo: {
      refWidth: '100%',
      refHeight: '100%',
    },
    foBody: {
      xmlns: Dom.ns.xhtml,
      style: {
        width: '100%',
        height: '100%',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
      },
    },
    'edit-text': {
      contenteditable: 'true',
      class: 'x6-edit-text',
      style: {
        width: '100%',
        textAlign: 'center',
        fontSize: 12,
        color: 'rgba(0,0,0,0.85)',
      },
    },
    text: {
      fontSize: 12,
      fill: '#080808',
    },
  },
  markup: [
    {
      tagName: 'rect',
      selector: 'body',
    },
    {
      tagName: 'text',
      selector: 'text',
    },
    {
      tagName: 'foreignObject',
      selector: 'fo',
      children: [
        {
          ns: Dom.ns.xhtml,
          tagName: 'body',
          selector: 'foBody',
          children: [
            {
              tagName: 'div',
              selector: 'edit-text',
            },
          ],
        },
      ],
    },
  ],
  ports: {
    groups: {
      top: {
        position: 'top',
        attrs: {
          circle: {
            r: 3,
            magnet: true,
            stroke: '#5F95FF',
            strokeWidth: 1,
            fill: '#fff',
            style: {
              visibility: 'hidden',
            },
          },
        },
      },
      right: {
        position: 'right',
        attrs: {
          circle: {
            r: 3,
            magnet: true,
            stroke: '#5F95FF',
            strokeWidth: 1,
            fill: '#fff',
            style: {
              visibility: 'hidden',
            },
          },
        },
      },
      bottom: {
        position: 'bottom',
        attrs: {
          circle: {
            r: 3,
            magnet: true,
            stroke: '#5F95FF',
            strokeWidth: 1,
            fill: '#fff',
            style: {
              visibility: 'hidden',
            },
          },
        },
      },
      left: {
        position: 'left',
        attrs: {
          circle: {
            r: 3,
            magnet: true,
            stroke: '#5F95FF',
            strokeWidth: 1,
            fill: '#fff',
            style: {
              visibility: 'hidden',
            },
          },
        },
      },
    },
    items: [
      {
        group: 'top',
      },
      {
        group: 'right',
      },
      {
        group: 'bottom',
      },
      {
        group: 'left',
      },
    ],
  },
});

export class NodeGroup extends Node {
  private collapsed = true;

  protected postprocess() {
    this.toggleCollapse(true);
  }

  isCollapsed() {
    return this.collapsed;
  }

  toggleCollapse(collapsed?: boolean) {
    const target = collapsed == null ? !this.collapsed : collapsed;
    if (target) {
      this.attr('buttonSign', {d: 'M 1 5 9 5 M 5 1 5 9'});
      this.resize(200, 40);
    } else {
      this.attr('buttonSign', {d: 'M 2 5 8 5'});
      if (this.store.data?.toggleCollapseResize !== undefined) {
        this.resize(this.store.data?.toggleCollapseResize.width, this.store.data?.toggleCollapseResize.height);
      } else {
        this.resize(540, 240);
      }
    }
    this.collapsed = target;
  }
}

NodeGroup.config({
  shape: 'rect',
  markup: [
    {
      tagName: 'rect',
      selector: 'body',
    },
    {
      tagName: 'image',
      selector: 'image',
    },
    {
      tagName: 'text',
      selector: 'text',
    },
    {
      tagName: 'g',
      selector: 'buttonGroup',
      children: [
        {
          tagName: 'rect',
          selector: 'button',
          attrs: {
            'pointer-events': 'visiblePainted',
          },
        },
        {
          tagName: 'path',
          selector: 'buttonSign',
          attrs: {
            fill: 'none',
            'pointer-events': 'none',
          },
        },
      ],
    },
  ],
  attrs: {
    body: {
      refWidth: '100%',
      refHeight: '100%',
      strokeWidth: 1,
      fill: 'rgba(219,218,218,0.3)',
      stroke: '#5F95FF',
    },
    text: {
      fontSize: 12,
      fill: 'rgba(0,0,0,0.85)',
      refX: 30,
      refY: 15,
    },
    buttonGroup: {
      refX: '100%',
      refX2: -25,
      refY: 13,
    },
    button: {
      height: 14,
      width: 16,
      rx: 2,
      ry: 2,
      fill: '#f5f5f5',
      stroke: '#ccc',
      cursor: 'pointer',
      event: 'node:collapse',
    },
    buttonSign: {
      refX: 3,
      refY: 2,
      stroke: '#808080',
    },
  },
});

Graph.registerNode('groupNode', NodeGroup);


export const generatePositions = (nodes, middle, isRelative) => {
  // 创建节点映射表，用于快速查找节点
  const nodeMap = new Map();
  nodes.forEach(node => {
    nodeMap.set(node.name, node);
  });

  // 计算节点的深度
  function calculateDepth(node) {
    if (node.depth !== undefined) {
      return node.depth;
    }

    let maxDepth = 0;
    node.next.forEach(nextNode => {
      const next = nodeMap.get(nextNode);
      const depth = calculateDepth(next);
      maxDepth = Math.max(maxDepth, depth + 1);
    });

    node.depth = maxDepth;
    return maxDepth;
  }

  let depthNodeMap = new Map();
  nodes.forEach(node => {
    node.depth = calculateDepth(node);
    setValueForKey(node.depth, node)
  });

  let keys = Array.from(depthNodeMap.keys()); // 将键转换为数组
  keys.sort((a, b) => b - a); // 数字从大到小排序

  let positions = {};

  const maxDepth = keys[0]
  let maxWidth = 0
  keys.forEach((key) => {
    let depthList = depthNodeMap.get(key)
    if (maxWidth <= depthList.length) {
      maxWidth = depthList.length
    }
  })

  if (isRelative) {
    middle = middle + (maxWidth+1)*300/2 + 5
  }

  keys.forEach((key) => {
    let depthList = depthNodeMap.get(key)
    let y = 100 + (maxDepth - key) * 150
    if (depthList.length % 2 === 1) {
      let x = Math.ceil((middle) - 300 * Math.ceil(depthList.length / 2))
      depthList.forEach(item => {
        positions[item.name] = {name: item.name,
          "position": {
            "x": x,
            "y": y,
            "width_total": maxWidth * 300,
            "height_total": (maxDepth + 1) * 150
          }
        };
        x = x + 300
      })
    } else {
      let x = Math.ceil((middle - 150) - 300 * Math.ceil(depthList.length / 2))
      depthList.forEach(item => {
        positions[item.name] = {name: item.name, "position": {
            "x": x,
            "y": y,
            "width_total": maxWidth * 300,
            "height_total": (maxDepth + 1) * 150
          }};
        x = x + 300
      })
    }
  })

  // 创建并设置值为列表的函数
  function setValueForKey(key, value) {
    if (!depthNodeMap.has(key)) {
      depthNodeMap.set(key, []);
    }
    let list = depthNodeMap.get(key);
    list.push(value);
  }


  return positions;
}

export const generateRelations = (nodes) => {
  const relations = [];
  for (const node of nodes) {
    const sourceId = node.id;
    const sourcePortId = node.ports.items.find(port => port.group === 'bottom').id;
    if (node.next === undefined) {
      continue
    }
    let nexts = node.next
    // 判断 node.next 是否是字符串
    if (typeof node.next === "string") {
      nexts = node.next.split(",");
    }

    for (const nextNodeName of nexts) {
      const targetNode = nodes.find(n => n.name === nextNodeName);
      const targetId = targetNode.id;
      const targetPortId = targetNode.ports.items.find(port => port.group === 'top').id;

      const relation = {
        source: {
          cell: sourceId,
          port: sourcePortId
        },
        target: {
          cell: targetId,
          port: targetPortId
        }
      };

      relations.push(relation);
    }
  }

  return relations;
}

export const generatePorts = {
  groups: {
    top: {
      position: 'top',
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
    right: {
      position: {
        name: 'right',
      },
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
    bottom: {
      position: 'bottom',
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
    left: {
      position: 'left',
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
  },
  items: [
    {
      group: 'top',
      id: buildUUID()
    },
    {
      group: 'right',
      id: buildUUID()
    },
    {
      group: 'bottom',
      id: buildUUID()
    },
    {
      group: 'left',
      id: buildUUID()
    },
  ],
}

export const defaultPorts = {
  groups: {
    top: {
      position: 'top',
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
    right: {
      position: {
        name: 'right',
      },
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
    bottom: {
      position: 'bottom',
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
    left: {
      position: 'left',
      attrs: {
        circle: {
          r: 3,
          magnet: true,
          stroke: '#5F95FF',
          strokeWidth: 1,
          fill: '#fff',
          style: {
            visibility: 'hidden',
          },
        },
      },
    },
  },
  items: [
    {
      group: 'top',
    },
    {
      group: 'right',
    },
    {
      group: 'bottom',
    },
    {
      group: 'left',
    },
  ],
}

export const defaultNode = {
  shape: 'flow-node',
  width: 200,
  height: 50,
  attrs: {
    label: {
      fontSize: 14,
      text: '',
    },
  },
  ports: defaultPorts,
  status: "success",
  visible: true,
  parent: ''
}

export const groupNode = {
  shape: 'groupNode',
  width: 200,
  height: 50,
  attrs: {
    label: {
      fontSize: 14,
      text: '',
    },
  },
  ports: defaultPorts,
  data: {parent: true,},
  children: [],
  visible: true,
  status: "success",
  toggleCollapseResize: {width: 240, height: 240},
  zIndex:15
}

export const defaultEdge = {
  attrs: {
    line: {
      stroke: '#5F95FF',
      strokeWidth: 1,
      targetMarker: {
        name: 'classic',
        size: 8,
      },
    },
  },
  router: {
    name: 'manhattan',
  },
  visible: true,
}

