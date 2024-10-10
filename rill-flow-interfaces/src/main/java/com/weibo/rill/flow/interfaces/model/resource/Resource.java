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

package com.weibo.rill.flow.interfaces.model.resource;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;


@Getter
public class Resource {
    public static final String CONNECTOR = "://";

    private final String resourceName;
    private String schemeProtocol;
    private final String schemeValue;

    public Resource(String resourceName) {
        this.resourceName = resourceName;
        int connectorIndex = resourceName.indexOf(CONNECTOR);
        this.schemeProtocol = connectorIndex == -1 ? "" : resourceName.substring(0, connectorIndex);
        this.schemeValue = connectorIndex == -1 ? resourceName : resourceName.substring(connectorIndex + CONNECTOR.length());
    }

    public Resource(String resourceName, String resourceProtocol) {
        this(resourceName);
        if (StringUtils.isNotBlank(resourceProtocol)) {
            this.schemeProtocol = resourceProtocol;
        }
    }

    public String getScheme() {
        return String.format("%s%s%s", schemeProtocol, CONNECTOR, schemeValue);
    }

    public boolean isHttpResource() {
        return "http".equals(schemeProtocol) ||
                "https".equals(schemeProtocol);
    }

    public boolean isAbResource() {
        return "ab".equals(schemeProtocol);
    }
}
