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

package com.weibo.rill.flow.olympicene.core.constant;


public class ReservedConstant {
    // PLACEHOLDER 值在脚本中有使用, 属性值修改后需同步修改脚本
    public static final String PLACEHOLDER = "_placeholder_";
    public static final String KEY_PREFIX = "_key_prefix_";
    public static final String SUB_CONTEXT_PREFIX = "__";
    public static final String ROUTE_NAME_CONNECTOR = "_";
    public static final String TASK_NAME_CONNECTOR = "-";

    public static final String FUNCTION_TASK_RESOURCE_NAME_SCHEME_CONNECTOR = "://";

    private ReservedConstant() {

    }
}
