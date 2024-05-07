export enum OptEnum {
  CREATE = 'create',
  EDIT = 'edit',
  DISPLAY = 'display',
}

export function getOptEnumByOpt(opt: string): OptEnum {
  switch (opt.toLowerCase()) {
    case 'create':
      return OptEnum.CREATE;
    case 'edit':
      return OptEnum.EDIT;
    case 'display':
      return OptEnum.DISPLAY;
    default:
      return null;
  }
}

export class OptConfig {
  showNodeGroups: boolean;
  showToolBar: boolean;
  showRightTool: boolean;
  readonly: boolean;
  showConfigPanel: boolean;

  constructor(
    showNodeGroups: boolean,
    showToolBar: boolean,
    showRightTool: boolean,
    readonly: boolean,
    showConfigPanel: boolean,
  ) {
    this.showNodeGroups = showNodeGroups;
    this.showToolBar = showToolBar;
    this.showRightTool = showRightTool;
    this.readonly = readonly;
    this.showConfigPanel = showConfigPanel;
  }
}

export const optConfig: Record<OptEnum, OptConfig> = {
  [OptEnum.CREATE]: new OptConfig(true, true, false, false, false),
  [OptEnum.EDIT]: new OptConfig(true, true, false, false, false),
  [OptEnum.DISPLAY]: new OptConfig(false, false, true, true, true),
};
