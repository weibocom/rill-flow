import { DagStatusEnum } from '../models/enums/dagStatusEnum';

export function dagStatusColor(status: DagStatusEnum): string {
  switch (status) {
    case DagStatusEnum.RUNNING:
      return 'processing';
    case DagStatusEnum.SUCCEED:
      return '#87d068';
    case DagStatusEnum.SUCCEEDED:
      return '#87d068';
    case DagStatusEnum.FAILED:
      return 'error';
    case DagStatusEnum.NOT_STARTED:
      return 'warning';
    case DagStatusEnum.KEY_SUCCEED:
      return '#87d068';
    default:
      return 'default';
  }
}
