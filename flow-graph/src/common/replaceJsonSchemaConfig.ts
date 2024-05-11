import TreeSelectWidget from '../components/Widget/TreeSelectWidget.vue';
import CodeEditWidget from "@/src/components/Widget/CodeEditWidget.vue";

function buildSchemaProperties(typeData: any, references: any, bizType: string) {
  return {
    attr: {
      title: '类型',
      type: 'string',
      enum: ['input', 'reference'],
      enumNames: ['input', 'reference'],
      'ui:width': '40%',
    },
    input: {
      title: '文本',
      type: typeData,
      'ui:hidden': "{{parentFormData.attr !== 'input'}}",
      'ui:width': '60%',
    },
    reference: {
      title: 'reference',
      type: 'string',
      default: '',
      'ui:hidden': "{{parentFormData.attr !== 'reference'}}",
      'ui:width': '60%',
      'ui:widget': TreeSelectWidget,
      'ui:treeData': references
    },
  };
}

export function getBaseTypeSet(): Set<string> {
  const result: Set<string> = new Set();
  result.add('string');
  result.add('boolean');
  result.add('number');
  return result;
}

export function replaceUIWidget(json, references) {
  if (typeof json === 'object') {
    if (getBaseTypeSet().has(json?.type)) {
      // 进行替换操作
      // TODO
      //  1. 根据参数类型进行替换 ps: ui:type: none, code, params, select
      if (json?.bizType === 'none' || json?.bizType === 'select') {
        json['ui:width'] = '40%'
        return;
      }

      if (json?.bizType === 'code') {
        json['ui:widget'] = CodeEditWidget
        json['ui:codeOptions'] = {mode: 'python'}
        return;
      }
      json['properties'] = buildSchemaProperties(json?.type, references, json?.bizType);
      delete json['type'];
      return;
    }
    for (const jsonKey in json) {
      replaceUIWidget(json[jsonKey], references);
    }
  } else if (Array.isArray(json)) {
    json.forEach((element) => {
      replaceUIWidget(element, references);
    });
  }
}
