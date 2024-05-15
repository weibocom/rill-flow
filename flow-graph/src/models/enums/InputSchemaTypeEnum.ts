export enum InputSchemaTypeEnum {

  NORMAL = 'normal', // 仅支持原生操作
  REFERENCE = 'reference', // 支持引用其他参数
  CODE = 'code', // 支持填写代码
  ARRAY_TO_MAP = 'array-to-map', // 支持将数组转换为map,ps: 模版输入参数为列表时，可通过此类型转换为map
}

