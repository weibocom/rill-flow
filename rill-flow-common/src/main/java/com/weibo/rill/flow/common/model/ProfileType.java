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

package com.weibo.rill.flow.common.model;

/**
 * profile资源的类型相关信息, 如MC, REDIS, API, KAFKA等
 * 
 * @author Lee on 16/5/30.
 */
public class ProfileType {
    private final String type;
    private final long interval1;
    private final long interval2;
    private final long interval3;
    private final long interval4;
    private final long slowThreshold;

    public ProfileType() {
        this("DEFAULT");
    }

    public ProfileType(String type) {
        this(type, 50, 200, 500, 1000, 200);
    }

    public ProfileType(String type, long interval1, long interval2, long interval3, long interval4, long slowThreshold) {
        this.type = type;
        this.interval1 = interval1;
        this.interval2 = interval2;
        this.interval3 = interval3;
        this.interval4 = interval4;
        this.slowThreshold = slowThreshold;
    }

    public String getType() {
        return type;
    }

    public long getInterval1() {
        return interval1;
    }

    public long getInterval2() {
        return interval2;
    }

    public long getInterval3() {
        return interval3;
    }

    public long getInterval4() {
        return interval4;
    }

    public long getSlowThreshold() {
        return slowThreshold;
    }
}
