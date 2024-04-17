import TreeSelectWidget from '../components/Widget/TreeSelectWidget.vue';

function buildSchemaProperties(typeData: any, references: any) {
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
      'ui:treeData': references,
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
      json['properties'] = buildSchemaProperties(json?.type, references);
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
