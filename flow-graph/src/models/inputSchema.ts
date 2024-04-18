export class InputSchemaValueItem {
  paramsName: string;
  paramsType: string;
  paramsValue: string;
  paramsDesc: string;
  paramsRequired: string;

  constructor(
    paramsName: string,
    paramsType: string,
    paramsValue: string,
    paramsDesc: string,
    paramsRequired: string,
  ) {
    this.paramsName = paramsName;
    this.paramsType = paramsType;
    this.paramsValue = paramsValue;
    this.paramsDesc = paramsDesc;
    this.paramsRequired = paramsRequired;
  }
}
