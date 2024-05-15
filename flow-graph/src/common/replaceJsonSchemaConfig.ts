import { InputSchemaHandlerFactory } from "./inputSchemaStyleHandler";
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
      InputSchemaHandlerFactory.getHandler(json?.bizType).updateUiWidget(json, references);
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
