import { useI18n } from "vue-i18n";
export enum NodeCategory {
  BASE_NODE = "0",
  TEMPLATE_NODE = "1"
}

export function getNodeCategoryName(nodeCategory: NodeCategory) {
  const { t } = useI18n();
  switch (nodeCategory) {
    case NodeCategory.BASE_NODE:
      return t('nodeBar.baseNodes');
    case NodeCategory.TEMPLATE_NODE:
      return t('nodeBar.templateNodes');
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

