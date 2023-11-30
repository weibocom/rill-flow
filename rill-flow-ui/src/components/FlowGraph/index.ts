import {Graph, Shape} from "@antv/x6";
import {
  defaultEdge,
  defaultNode,
  generatePorts,
  generatePositions, generateRelations, groupNode
} from "./shape";
import {buildUUID} from "@/utils/uuid";
import {cloneDeep} from 'lodash-es';
import {Options as GraphOptions} from "@antv/x6/src/graph/options";

const defaultGraphConfig: Partial<GraphOptions.Manual> = {
  container: null,
  panning: true,
  width: 400,
  height: 500,
  grid: {
    size: 10,
    visible: false,
    type: 'doubleMesh',
    args: [
      {
        color: '#cccccc',
        thickness: 1,
      },
      {
        color: '#5F95FF',
        thickness: 1,
        factor: 4,
      },
    ],
  },
  scroller: {
    enabled: false,
    autoResize: false,
  },
  mousewheel: {
    enabled: true,
    modifiers: ['ctrl', 'meta'],
    minScale: 0.5,
    maxScale: 2,
  },
  connecting: {
    anchor: 'center',
    connectionPoint: 'anchor',
    allowBlank: false,
    highlight: true,
    snap: true,
    createEdge() {
      return new Shape.Edge(defaultEdge);
    },
    validateConnection() {
      return false;
    },
  },
  highlighting: {
    magnetAvailable: {
      name: 'stroke',
      args: {
        padding: 4,
        attrs: {
          strokeWidth: 4,
          stroke: 'rgba(223,234,255)',
        },
      },
    },
  },
  snapline: true,
  keyboard: {
    enabled: true,
  },
}

export function initGraph(dagInfo, nodeGroups, container) {
  defaultGraphConfig.container = container
  const graph = new Graph(defaultGraphConfig)
  initGraphShape(graph, dagInfo, nodeGroups)
  initGraphEvent(graph)
  graph.resize(document.body.offsetWidth, document.body.offsetHeight);
  return graph
}

function initGraphShape(graphInstance, flowInfo, nodeGroups) {
  const cells = []

  let data = JsonToGraphCell(flowInfo, nodeGroups)
  data.forEach((item) => {
    if (item.shape === 'edge') {
      cells.push(graphInstance.createEdge(item))
    } else if (item.shape === 'groupNode') {
      groupNode.id = item.id
      groupNode.position = item.position
      groupNode.ports = item.ports
      groupNode.attrs = item.attrs
      groupNode.children = item.children
      groupNode.toggleCollapseResize = item.toggleCollapseResize
      cells.push(graphInstance.createNode(groupNode))
    } else {
      defaultNode.id = item.id
      defaultNode.position = item.position
      defaultNode.ports = item.ports
      defaultNode.attrs = item.attrs
      defaultNode.visible = item.visible
      defaultNode.parent = item.parent
      defaultNode.shape = item.shape
      delete item.component
      cells.push(graphInstance.createNode(defaultNode))
    }
  })
  graphInstance.resetCells(cells)

}

function initGraphEvent(graph) {
  graph.on('node:mouseenter', ({e, node, view}) => {
    const ports = view.container.querySelectorAll('.x6-port-body') as NodeListOf<SVGAElement>;
    showPorts(ports, true);
  });
  graph.on('node:mouseleave', ({e, node, view}) => {
    const ports = view.container.querySelectorAll('.x6-port-body') as NodeListOf<SVGAElement>;
    showPorts(ports, false);
  });
  graph.on('node:collapse', ({node, e}: any) => {
    e.stopPropagation();
    node.toggleCollapse();
    const collapsed = node.isCollapsed();
    const cells = node.getDescendants();
    cells.forEach((n: any) => {
      if (collapsed) {
        n.hide();
      } else {
        n.hide();
        n.show();
      }
    });
  });
}

function showPorts(ports: NodeListOf<SVGAElement>, show: boolean) {
  for (let i = 0, len = ports.length; i < len; i = i + 1) {
    ports[i].style.visibility = show ? 'visible' : 'hidden';
  }
}

function JsonToGraphCell(graphDataJson, nodeGroups) {
  const cells = []

  let nodeList = []
  for (let task in graphDataJson?.tasks) {
    nodeList.push({"name": task, "next": graphDataJson?.tasks[task]?.next})
  }

  let nodePositions = generatePositions(nodeList, document.body.offsetWidth / 2, false)

  for (let task in graphDataJson?.tasks) {
    let taskInfo = graphDataJson?.tasks[task]
    let icon
    for (const key in nodeGroups[2]?.operatorList) {
      if (nodeGroups[2]?.operatorList[key].name === taskInfo.task.resourceProtocol) {
        icon = nodeGroups[2]?.operatorList[key].icon
      }
    }

    let type = (taskInfo?.task?.category !== "function") ? taskInfo?.task?.category : ((taskInfo?.task?.resourceProtocol === undefined || taskInfo?.task?.resourceProtocol === "") ? taskInfo?.task?.resourceName?.split(":")[0] : taskInfo?.task?.resourceProtocol)
    if (taskInfo.contains_sub) {
      if (taskInfo?.task?.category === 'foreach') {
        const groupId = buildUUID();
        let childrenIds = []
        let toggleCollapseResize
        if (taskInfo?.task?.tasks !== undefined) {
          let subTasks = taskInfo?.task?.tasks
          let subTaskNodeList = []
          for (let task in subTasks) {
            subTaskNodeList.push({
              "name": subTasks[task].name,
              "next": subTasks[task]?.next === undefined ? [] : subTasks[task]?.next.split(",")
            })
          }

          let subTaskNodePositions = generatePositions(subTaskNodeList, nodePositions[task]?.position.x, true)
          for (let subTask in subTasks) {
            let subTaskInfo = subTasks[subTask]
            for (const key in nodeGroups[1]?.operatorList) {
              if (nodeGroups[2]?.operatorList[key].name === subTaskInfo.resourceProtocol) {
                icon = nodeGroups[2]?.operatorList[key].icon
              }
            }
            const x = subTaskNodePositions[subTaskInfo.name].position.x
            const y = subTaskNodePositions[subTaskInfo.name].position.y + nodePositions[task]?.position.y
            let subTaskType = (subTaskInfo?.category !== "function") ? subTaskInfo?.category : ((subTaskInfo?.resourceProtocol === undefined || subTaskInfo?.resourceProtocol === "") ? subTaskInfo?.resourceName?.split(":")[0] : subTaskInfo?.resourceProtocol)
            let subCell = {
              "id": buildUUID(),
              "name": subTaskInfo.name,
              "shape": "flow-node",
              "ports": generatePorts,
              "attrs": {
                "label": {"text": subTaskInfo.name},
                "status": subTaskInfo?.status,
                "icon": icon,
                type: subTaskType,
                category: subTaskInfo?.category,
                nodeDetail: subTaskInfo
              },
              "position": {x: x, y: y},
              "parent": groupId,
              "next": subTaskInfo.next,
              "visible": false,
            }
            toggleCollapseResize = {
              width: subTaskNodePositions[subTaskInfo.name].position.width_total,
              height: subTaskNodePositions[subTaskInfo.name].position.height_total
            }
            childrenIds.push(subCell.id)
            cells.push(subCell)
          }
        }

        let cell = {
          "id": groupId,
          "name": task,
          "shape": "groupNode",
          "children": childrenIds,
          "data": {parent: true,},
          "ports": generatePorts,
          "attrs": {
            "label": {"text": task},
            "text": {"text": task},
            "status": taskInfo?.status,
            "icon": icon,
            type: type,
            category: taskInfo?.task?.category,
            nodeDetail: taskInfo
          },
          "position": nodePositions[task]?.position,
          "next": taskInfo.next,
          "toggleCollapseResize": toggleCollapseResize
        }
        cells.push(cell)
      } else if (taskInfo?.task?.category === 'choice') {
        const groupId = buildUUID();
        let childrenIds = []
        let toggleCollapseResize
        if (taskInfo?.task?.choices !== undefined) {
          let choices = taskInfo?.task?.choices
          let subTaskNodeList = []
          let subTasksDatail = []


          // 具体条件的子节点
          const innerChoiceNext = []
          for (let choice in choices) {

            if (choices[choice].tasks.length > 0) {
              innerChoiceNext.push(choices[choice].tasks[0].name)
            }
            for (const task in choices[choice].tasks) {
              subTaskNodeList.push({
                "name": choices[choice].tasks[task].name,
                "next": choices[choice].tasks[task]?.next === undefined ? [] : choices[choice].tasks[task]?.next.split(",")
              })
              subTasksDatail.push(choices[choice].tasks[task])
            }
          }
          // 选择节点
          subTaskNodeList.push({
            "name": taskInfo?.task?.name + "choices",
            "next": innerChoiceNext
          })
          let virtualNode = cloneDeep(taskInfo?.task)
          virtualNode.name = taskInfo?.task?.name + "choices"
          virtualNode.next = innerChoiceNext
          subTasksDatail.push(virtualNode)

          let subTaskNodePositions = generatePositions(subTaskNodeList, nodePositions[task]?.position.x, true)
          // 开始布局choice节点
          for (let subTask in subTasksDatail) {
            let subTaskInfo = subTasksDatail[subTask]
            for (const key in nodeGroups[1]?.operatorList) {
              if (nodeGroups[2]?.operatorList[key].name === subTaskInfo.resourceProtocol) {
                icon = nodeGroups[2]?.operatorList[key].icon
              }
            }
            const x = subTaskNodePositions[subTaskInfo.name].position.x
            const y = subTaskNodePositions[subTaskInfo.name].position.y + nodePositions[task]?.position.y
            let subTaskType = (subTaskInfo?.category !== "function") ? subTaskInfo?.category : ((subTaskInfo?.resourceProtocol === undefined || subTaskInfo?.resourceProtocol === "") ? subTaskInfo?.resourceName?.split(":")[0] : subTaskInfo?.resourceProtocol)
            let subCell = {
              "id": buildUUID(),
              "name": subTaskInfo.name,
              "shape": "flow-node",
              "ports": generatePorts,
              "attrs": {
                "label": {"text": subTaskInfo.name},
                "status": subTaskInfo?.status,
                "icon": icon,
                type: subTaskType,
                category: subTaskInfo?.category,
                nodeDetail: subTaskInfo
              },
              "position": {x: x, y: y},
              "parent": groupId,
              "next": subTaskInfo.next,
              "visible": false,
              "zIndex": 50,
            }
            toggleCollapseResize = {
              width: subTaskNodePositions[subTaskInfo.name].position.width_total,
              height: subTaskNodePositions[subTaskInfo.name].position.height_total
            }
            childrenIds.push(subCell.id)
            cells.push(subCell)
          }
        }
        let cell = {
          "id": groupId,
          "name": task,
          "shape": "groupNode",
          "children": childrenIds,
          "data": {parent: true,},
          "ports": generatePorts,
          "attrs": {
            "label": {"text": task},
            "text": {"text": task},
            "status": taskInfo?.status,
            "icon": icon,
            type: type,
            category: taskInfo?.task?.category,
            nodeDetail: taskInfo
          },
          "position": nodePositions[task]?.position,
          "next": taskInfo.next,
          "toggleCollapseResize": toggleCollapseResize,
          "zIndex": 50,
        }
        cells.push(cell)
      }

    } else {
      let cell = {
        "id": buildUUID(),
        "name": task,
        "shape": "flow-node",
        "ports": generatePorts,
        "attrs": {
          "label": {"text": task},
          "status": taskInfo?.status,
          "icon": icon,
          type: type,
          category: taskInfo?.task?.category,
          nodeDetail: taskInfo
        },
        "position": nodePositions[task]?.position,
        "next": taskInfo.next,
        "zIndex": 1,
      }
      cells.push(cell)
    }
  }


  const relations = generateRelations(cells);

  relations.forEach(relation => {
    relation.id = buildUUID();
    relation.shape = "edge";
    relation.zIndex = 1;
    relation.router = defaultEdge.router;
    relation.connector = "rounded";
    relation.attrs = defaultEdge.attrs;
    relation.visible = true;

    cells.filter(item => item.shape === 'groupNode').forEach(item => {
      item.children.filter(node => {
        return node === relation.source.cell || node === relation.target.cell
      }).forEach(node => {
        if (!item.children.includes(relation.id)) {
          item.children.push(relation.id)
        }
        relation.parent = item.id
      })
    })
    cells.push(relation);
  })
  return cells
}
