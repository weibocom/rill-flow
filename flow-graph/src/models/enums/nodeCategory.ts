export enum NodeCategory {
  BASE_NODE = "0",
  TEMPLATE_NODE = "1"
}

export function getNodeCategoryName(nodeCategory: NodeCategory) {
  switch (nodeCategory) {
    case NodeCategory.BASE_NODE:
      return '基础节点';
    case NodeCategory.TEMPLATE_NODE:
      return '模板节点';
  }
}

export function getNodeCategoryByNumber(nodeCategory: number) {
  switch (nodeCategory) {
    case 0:
      return NodeCategory.BASE_NODE;
    case 1:
      return NodeCategory.TEMPLATE_NODE;
    default:
      return undefined;
  }
}

