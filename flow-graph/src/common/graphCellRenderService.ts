import { RillNode } from "../models/node";
import { getEdgeConfig, getVueNodeConfig } from "./transform";
import { Model } from "@antv/x6";

export class GraphCellRenderService {
  public static render(node: RillNode, icon: string, data: Model.FromJSONData): [] {
    const cells = [];
    const renderNodeConfig = GraphCellRenderService.renderNode(node, icon);
    cells.push(renderNodeConfig);
    data?.nodes.push(renderNodeConfig);

    const renderEdgeConfigs = GraphCellRenderService.renderNodeEdge(node, data);
    cells.concat(renderEdgeConfigs);
    return cells.concat(renderEdgeConfigs);
  }

  private static renderNode(node: RillNode, icon: string): {} {

    return getVueNodeConfig({
      size: {
        width: node.task.name.length * 13 > 180 ?  node.task.name.length * 13 : 180,
        height: 40,
      },
      icon: icon,
      label: (node.task.title === undefined || node.task.title === '') ? node.task.name : node.task.title,
      nodePrototype: node.nodePrototypeId,
      position: '',
      id: node.id,
      status: node.task.status,
      ports: GraphCellRenderService.generatePorts(node),
    });
  }

  private static renderNodeEdge(node: RillNode, data: Model.FromJSONData): [] {
    // 根据node的outgoingEdges渲染Edge
    const edgeCells: [] = [];
    node.outgoingEdges.forEach((edge) => {
      const edgeConfig = getEdgeConfig();
      edgeConfig.source.cell = edge.sourceNodeId;
      edgeConfig.source.port = edge.sourcePortId;
      edgeConfig.target.cell = edge.targetNodeId;
      edgeConfig.target.port = edge.targetPortId;
      edgeCells.push(edgeConfig);
      data?.edges.push(edgeConfig);
    });
    return edgeCells;
  }

  private static generatePorts(node: RillNode) {
    return {
      groups: {
        top: {
          position: 'top',
          attrs: {
            circle: {
              r: 4,
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
              r: 4,
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
              r: 4,
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
              r: 4,
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
          id: node.id + '-up',
        },
        {
          group: 'right',
          id: node.id + '-right',
        },
        {
          group: 'bottom',
          id: node.id + '-down',
        },
        {
          group: 'left',
          id: node.id + '-left',
        },
      ],
    };
  }
}
