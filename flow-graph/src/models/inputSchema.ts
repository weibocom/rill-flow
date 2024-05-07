export class InputSchemaValueItem {
  name: string;
  type: string;
  value: string;
  desc: string;
  required: string;

  constructor(
    name: string,
    type: string,
    value: string,
    desc: string,
    required: string,
  ) {
    this.name = name;
    this.type = type;
    this.value = value;
    this.desc = desc;
    this.required = required;
  }
}
