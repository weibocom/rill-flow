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

package com.weibo.rill.flow.interfaces.model.task;

public enum TaskStatus {

    /**
     * 初始状态
     * 新构建TaskInfo时的状态
     */
    NOT_STARTED,

    /**
     * 即将开始运行
     * 遍历模块遍历到readyToRun时，由TaskRunners设置为READY，并且立即更新存储
     */
    READY,

    /**
     * 运行中
     * 进入TaskRun的run函数后由对应的Run对象设置为RUNNING，并且立即更新存储
     */
    RUNNING,

    /**
     * 换出状态
     */
    STASHED,

    /**
     * 关键路径完成 foreach,sub_flow
     */
    KEY_SUCCEED,

    /**
     * 成功
     * 1. 由FinishNotify引入,
     * 2. 同步FunctionTask和逻辑控制类(foreachTask和choiceTask)Task，
     * 在执行完outputMappings后设置为SUCCEED
     */
    SUCCEED,

    /**
     * 失败
     * 1. 由FinishNotify引入,
     * 2. 同步FunctionTask和逻辑控制类(foreachTask和choiceTask)Task，
     * 在执行完outputMappings后设置为FAILED
     */
    FAILED,

    /**
     * 跳过
     * 1. 由FinishNotify引入,
     * 2. 同步FunctionTask和逻辑控制类(foreachTask和choiceTask)Task，
     * 3. choiceTask condition计算为false
     * 在执行完outputMappings后，并且Task属性tolerance值为true时设置为SKIPPED
     */
    SKIPPED;

    /**
     * 无论成功与否的完成
     */
    public boolean isCompleted() {
        return this.ordinal() >= SUCCEED.ordinal();
    }

    public boolean isSuccessOrSkip() {
        return this == SUCCEED || this == SKIPPED;
    }

    public boolean isSuccessOrKeySuccessOrSkip() {
        return this == SUCCEED || this == SKIPPED || this == KEY_SUCCEED;
    }

    public boolean isKeyModeStatus() {
        return this == KEY_SUCCEED || this == STASHED;
    }

    public boolean isFailed() {
        return this == FAILED;
    }
}
