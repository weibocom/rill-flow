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

package com.weibo.rill.flow.common.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.*;

import java.util.List;


@Builder
@Getter
@Setter
@ToString
@NoArgsConstructor
public class ResourceCheckConfig {
    private CheckType checkType;
    private List<String> keyResources;

    @JsonCreator
    public ResourceCheckConfig(@JsonProperty("check_type") CheckType checkType,
                               @JsonProperty("key_resources") List<String> keyResources) {
        this.checkType = checkType;
        this.keyResources = keyResources;
    }

    @AllArgsConstructor
    public enum CheckType {
        SHORT_BOARD("short_board"),
        LONG_BOARD("long_board"),
        KEY_RESOURCE("key_resource"),
        SKIP("skip");

        private final String value;

        @JsonCreator
        public CheckType parse(String type) {
            for (CheckType checkType : values()) {
                if (checkType.value.equals(type)) {
                    return checkType;
                }
            }

            return SKIP;
        }

        @JsonValue
        public String getValue() {
            return this.value;
        }
    }
}
