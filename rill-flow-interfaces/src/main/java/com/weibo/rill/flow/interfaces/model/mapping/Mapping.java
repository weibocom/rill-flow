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

package com.weibo.rill.flow.interfaces.model.mapping;

import lombok.*;

import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
/**
 * 映射规则
 * 1. source 代表源集合以json path表示的值
 * 2. target 代表目标集合以json path表示的key
 *
 * 映射的过程是将 源集合中以source为key的value赋值到目标集合以target为key的值上
 * 如：
 * 集合 input {"key1": "value1"} context {"key1": "value2"}
 * 其中规则为:
 *  - source: 'key1'
 *    target: 'key3'
 *映射结果为:
 * 集合 input {"key1": "value1"} context {"key1": "value2", "key3": "value1"}
 *
 */
public class Mapping {
    private String reference;
    private Boolean tolerance;
    private String source;
    private String transform;
    private String target;
    private String variable;

    public Mapping(String source, String target) {
        this.source = source;
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mapping mapping = (Mapping) o;
        return Objects.equals(reference, mapping.reference) && Objects.equals(source, mapping.source)
                && Objects.equals(transform, mapping.transform) && Objects.equals(target, mapping.target)
                && Objects.equals(variable, mapping.variable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reference, source, transform, target, variable);
    }
}
