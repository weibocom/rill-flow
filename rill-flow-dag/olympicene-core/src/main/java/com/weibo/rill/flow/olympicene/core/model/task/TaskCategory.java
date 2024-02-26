/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.olympicene.core.model.task;


import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum TaskCategory {
    // 调用函数服务的Task
    FUNCTION("function", 0),

    // 流程控制Task，执行分支语句
    CHOICE("choice", 1),

    // 流程控制Task，执行循环语句
    FOREACH("foreach", 1),

    // 本身无处理逻辑，等待外部通知，然后执行 output 更新数据, 兼容olympiadane1.0
    SUSPENSE("suspense", 2),

    // 空 task
    PASS("pass", 2),

    // return task
    RETURN("return", 2),
    ;

    private final String value;

    /**
     * TaskCategory类型
     * 0: 计算类任务 需调用外部系统资源执行 如 程序语言中 运算符 方法
     * 1: 流程控制类任务 生成子任务 如 程序语言中 选择结构与循环结构
     * 2: 流程控制类任务 控制任务执行顺序 如 程序语言中 wait return
     */
    private final int type;

    public static TaskCategory getEnumByValue(String category) {
        if (category == null) {
            return null;
        }
        return switch (category) {
            case "function" -> FUNCTION;
            case "choice" -> CHOICE;
            case "foreach" -> FOREACH;
            case "suspense" -> SUSPENSE;
            case "pass" -> PASS;
            case "return" -> RETURN;
            default -> null;
        };
    }
}
