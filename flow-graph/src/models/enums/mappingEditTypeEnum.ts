export enum MappingEditTypeEnum {
  INPUT = 'input',
  REFERENCE = 'reference'
}

export function getMappingEditTypeEnumByType(type: string) {
  switch (type) {
    case 'input':
      return MappingEditTypeEnum.INPUT;
    case 'reference':
      return MappingEditTypeEnum.REFERENCE;
    default:
      return undefined;
  }
}
