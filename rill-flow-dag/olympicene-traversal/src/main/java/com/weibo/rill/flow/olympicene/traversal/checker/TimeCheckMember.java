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

package com.weibo.rill.flow.olympicene.traversal.checker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.*;


@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class TimeCheckMember {
    private CheckMemberType checkMemberType;
    private String executionId;
    private String taskCategory;
    private String taskInfoName;

    @AllArgsConstructor
    public enum CheckMemberType {
        DAG_TIMEOUT_CHECK("dag_timeout_check"),
        TASK_TIMEOUT_CHECK("task_timeout_check"),
        TASK_WAIT_CHECK("task_wait_check")
        ;

        private final String type;

        @JsonCreator
        public static CheckMemberType parse(String type) {
            for (CheckMemberType checkMemberType : values()) {
                if (checkMemberType.getType().equals(type)) {
                    return checkMemberType;
                }
            }
            return null;
        }

        @JsonValue
        public String getType() {
            return type;
        }
    }
}
