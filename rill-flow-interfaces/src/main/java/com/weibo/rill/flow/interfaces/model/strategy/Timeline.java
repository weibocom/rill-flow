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

package com.weibo.rill.flow.interfaces.model.strategy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
public class Timeline {
    private String timeoutInSeconds;
    private String suspenseIntervalSeconds;
    private String suspenseTimestamp;

    @JsonCreator
    public Timeline(@JsonProperty("timeoutInSeconds") String timeoutInSeconds,
                    @JsonProperty("suspenseIntervalSeconds") String suspenseIntervalSeconds,
                    @JsonProperty("suspenseTimestamp") String suspenseTimestamp) {
        this.timeoutInSeconds = timeoutInSeconds;
        this.suspenseIntervalSeconds = suspenseIntervalSeconds;
        this.suspenseTimestamp = suspenseTimestamp;
    }
}
