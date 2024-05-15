import { InputSchemaTypeEnum } from "../models/enums/InputSchemaTypeEnum";
import TreeSelectWidget from '../components/Widget/TreeSelectWidget.vue';
import CodeEditWidget from "../components/Widget/CodeEditWidget.vue";
import { Mapping } from "../models/task/mapping";
import { FlowGraph } from "../models/flowGraph";
import { MappingParameters } from "../models/mappingParameters";
import { getMappingEditTypeEnumByType, MappingEditTypeEnum } from "../models/enums/mappingEditTypeEnum";
import { useI18nStoreWithOut } from "../store/modules/i18nStore";

abstract class NodeInputSchemaHandler {
  abstract getBizType(): string;
  abstract updateUiWidget(schema: any, data: any);
  abstract showSchemaValueHandle(key: string, schema: any, inputMappingMap: Map<string, Mapping>, flowGraph: FlowGraph): any;
  abstract saveSchemaValueHandle(key: string, schemaValue: any): any;
}

class ReferenceNodeInputSchemaHandler extends NodeInputSchemaHandler {
  getBizType(): string {
    return InputSchemaTypeEnum.REFERENCE;
  }

  updateUiWidget(schema: any, data: any) {
    schema['properties'] = this.buildSchemaProperties(schema?.type, data, schema?.bizType);
    delete schema['type'];
  }

  showSchemaValueHandle(key: string, schema: any, inputMappingMap: Map<string, Mapping>, flowGraph: FlowGraph): any {
    const inputMapping = inputMappingMap.get(key);
    if (inputMapping === undefined || inputMapping?.source === undefined) {
      return null;
    }

    const inputFormData = {
      key: key,
      value: {
        attr: 'input',
        input: inputMapping.source,
        reference: '',
      },
    };

    const formData = inputMapping.source.toString().startsWith('$.')
      ? this.getSchemaFormDataByReference(inputMapping, flowGraph, key)
      : inputFormData;
    return formData;
  }
  saveSchemaValueHandle(key: string, schemaValue: any): any {
    const result = new Map<string, MappingParameters>();
    const parameters: MappingParameters = new MappingParameters();
    parameters.key = key;
    parameters.type = getMappingEditTypeEnumByType(schemaValue['attr']);
    if (parameters.type === undefined) {
      console.error(`未知的类型：`, key, schemaValue);
      return result;
    } else if (parameters.type === MappingEditTypeEnum.REFERENCE) {
      if (schemaValue['reference'] === undefined) {
        return result;
      }
      parameters.reference = schemaValue['reference'];
    } else if (parameters.type === MappingEditTypeEnum.INPUT) {
      if (schemaValue['input'] === undefined) {
        return result;
      }
      parameters.input = schemaValue['input'];
    }
    result.set('$.input.' + key, parameters);

    return result;
  }
  buildSchemaProperties(typeData: any, references: any, bizType: string) {
    const { t } = useI18nStoreWithOut().getI18n().global;
    return {
      attr: {
        title: t('inputSchema.attr'),
        type: 'string',
        enum: ['input', 'reference'],
        enumNames: [t('inputSchema.intput'), t('inputSchema.reference')],
        'ui:width': '40%',
      },
      input: {
        title: t('inputSchema.intput'),
        type: typeData,
        'ui:hidden': "{{parentFormData.attr !== 'input'}}",
        'ui:width': '60%',
      },
      reference: {
        title: t('inputSchema.reference'),
        type: 'string',
        default: '',
        'ui:hidden': "{{parentFormData.attr !== 'reference'}}",
        'ui:width': '60%',
        'ui:widget': TreeSelectWidget,
        'ui:treeData': references
      },
    };
  }

  getSchemaFormDataByReference(
    inputMapping: Mapping,
    flowGraph: FlowGraph,
    inputTargetParam: string,
  ): object {
    let containNode = false;
    if (inputMapping.source.startsWith('$.context.')) {
      const maybeTaskName = inputMapping.source.split('.')[2];
      containNode = flowGraph.containNode(maybeTaskName);
    }
    return {
      key: inputTargetParam,
      value: {
        attr: 'reference',
        reference: containNode
          ? inputMapping.source.replace('.context', '')
          : inputMapping.source,
        input: ''
      },
    };
  }
}

class CodeNodeInputSchemaHandler extends NodeInputSchemaHandler {
  getBizType(): string {
    return InputSchemaTypeEnum.CODE;
  }
  updateUiWidget(schema: object, data: any) {
    schema['ui:widget'] = CodeEditWidget
    schema['ui:codeOptions'] = { mode: 'python'}
  }
  showSchemaValueHandle(key: string, schema: any, inputMappingMap: Map<string, Mapping>, flowGraph: FlowGraph): any {
    const inputMapping = inputMappingMap.get(key);
    if (inputMapping === undefined || inputMapping?.source === undefined) {
      return null;
    }
    return {
      key: key,
      value: inputMapping.source
    };
  }
  saveSchemaValueHandle(key: string, schemaValue: any): any {
    const result = new Map<string, MappingParameters>();
    const parameters: MappingParameters = new MappingParameters();
    parameters.key = key
    parameters.type = MappingEditTypeEnum.INPUT
    parameters.input = schemaValue
    result.set('$.input.' + key, parameters);
    return result;  }
}

class NormalNodeInputSchemaHandler extends NodeInputSchemaHandler {
  getBizType(): string {
    return InputSchemaTypeEnum.NORMAL;
  }
  updateUiWidget(schema: any, data: any) {
    schema['ui:width'] = '40%'
  }
  showSchemaValueHandle(key: string, schema: any, inputMappingMap: Map<string, Mapping>, flowGraph: FlowGraph): any {
    const inputMapping = inputMappingMap.get(key);
    if (inputMapping === undefined || inputMapping?.source === undefined) {
      return null;
    }
    return {
      key: key,
      value: inputMapping.source
    };
  }
  saveSchemaValueHandle(key: string, schemaValue: any):  Map<string, MappingParameters> {
    const result = new Map<string, MappingParameters>();
    const parameters: MappingParameters = new MappingParameters();
    parameters.key = key
    parameters.type = MappingEditTypeEnum.INPUT
    parameters.input = schemaValue
    result.set('$.input.' + key, parameters);
    return result;
  }
}


class ArrayToMapNodeInputSchemaHandler extends NodeInputSchemaHandler {
  getBizType(): string {
    return InputSchemaTypeEnum.ARRAY_TO_MAP;
  }
  updateUiWidget(schema: any, data: any) {}
  showSchemaValueHandle(key: string, schema: any, inputMappingMap: Map<string, Mapping>, flowGraph: FlowGraph): any {
    const properties = schema[key]?.items?.properties;
    const arrayValue = [];
    for (const inputMappingKey of inputMappingMap.keys()){
      if (inputMappingKey.split('.')[0] === key) {
        const itemKey = inputMappingKey.split('.')[1]
        inputMappingMap.set(itemKey, inputMappingMap.get(inputMappingKey));
        const formData = InputSchemaHandlerFactory.getHandler(properties['value']?.bizType).showSchemaValueHandle(itemKey, null, inputMappingMap, flowGraph);

        if (formData === null) {
          continue;
        }
        arrayValue.push(formData)
      }
    }
    return {
      key: key,
      value: arrayValue,
    };
  }
  saveSchemaValueHandle(key: string, schemaValue: any): Map<string, MappingParameters> {
    const result = new Map<string, MappingParameters>();
    for (const id in schemaValue) {
      const parameters: MappingParameters = new MappingParameters();
      parameters.key = schemaValue[id].key
      const editType =  typeof schemaValue[id].value === "object" ? getMappingEditTypeEnumByType(schemaValue[id].value.attr) : MappingEditTypeEnum.INPUT
      parameters.type = editType
      if (editType === undefined) {
        continue;
      }
      parameters[editType] = typeof schemaValue[id].value === "object" ? schemaValue[id].value[editType] : schemaValue[id].value
      result.set('$.input.' + key + '.' + schemaValue[id].key, parameters);
    }
    return result;
  }
}


export class InputSchemaHandlerFactory {
  public static getHandler(type: string) {
    switch (type) {
      case InputSchemaTypeEnum.NORMAL:
        return new NormalNodeInputSchemaHandler();
      case InputSchemaTypeEnum.REFERENCE:
        return new ReferenceNodeInputSchemaHandler();
      case InputSchemaTypeEnum.CODE:
        return new CodeNodeInputSchemaHandler();
      case InputSchemaTypeEnum.ARRAY_TO_MAP:
        return new ArrayToMapNodeInputSchemaHandler();
      default:
        return new ReferenceNodeInputSchemaHandler();
    }
  }

}