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

package com.weibo.rill.flow.olympicene.core.model.task


import spock.lang.Specification

/***
 * test for this class:

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
  private final int type;

  public static com.weibo.rill.flow.olympicene.core.model.task.TaskCategory getEnumByValue(String category) {
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
 */
class TaskCategoryTest extends Specification {
    /**
     * test enum
     * @return
     */
    def "test getEnumByValue"() {
        when:
        TaskCategory taskCategory = TaskCategory.getEnumByValue(category)
        then:
        taskCategory == expected
        where:
        category    | expected
        null        | null
        "choice"    | TaskCategory.CHOICE
        "foreach"   | TaskCategory.FOREACH
        "function"  | TaskCategory.FUNCTION
        "pass"      | TaskCategory.PASS
        "suspense"  | TaskCategory.SUSPENSE
        "return"    | TaskCategory.RETURN
        "break"     | null
    }

    def "test getType"() {
        when:
        var category = given
        then:
        category.getType() == expected
        where:
        given                   | expected
        TaskCategory.FUNCTION   | 0
        TaskCategory.FOREACH    | 1
        TaskCategory.CHOICE     | 1
        TaskCategory.SUSPENSE   | 2
        TaskCategory.RETURN     | 2
        TaskCategory.PASS       | 2
    }
}
