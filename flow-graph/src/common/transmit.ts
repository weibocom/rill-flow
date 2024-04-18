import { CustomEventTypeEnum } from './enums';

/**
 * @class 通信
 */
export class Channel {
  /**
   * 事件分发
   * @param {CustomEventTypeEnum<string>} TypeEnum 事件分发类型
   * @param {any} detail 详情
   * @static
   */
  static dispatchEvent(TypeEnum: CustomEventTypeEnum<string>, detail: any): void {
    if (!TypeEnum) throw new Error('TypeEnum not found');
    window.dispatchEvent(
      new CustomEvent(TypeEnum, {
        detail,
        bubbles: false,
        cancelable: true,
      }),
    );
  }

  /**
   * 事件监听
   * @param {CustomEventTypeEnum<string>} TypeEnum 事件分发类型
   * @param {Function} callback 回调
   * @static
   */
  static eventListener(TypeEnum: CustomEventTypeEnum<string>, callback: Function): void {
    if (!TypeEnum) throw new Error('TypeEnum not found');
    window.addEventListener(
      TypeEnum,
      function (event) {
        callback(event.detail);
      },
      false,
    );
  }
}
