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
 * @author zilong6 on 2017/12/25.
 */
public enum HttpFeature {

    /**
     * 启用打印访问日志的功能
     */
    ACCESS_LOG,
    /**
     * 当连接失败的时候自动failfast
     */
    CONN_FAILFAST,
    /**
     * 当服务端处理失败的时候（报5xx）自动failfast
     */
    SERVER_ERROR_FAILFAST,
    /**
     * 打印profile日志
     */
    PROFILE

}
