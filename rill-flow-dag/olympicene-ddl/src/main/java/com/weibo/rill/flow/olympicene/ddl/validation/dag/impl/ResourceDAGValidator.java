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

package com.weibo.rill.flow.olympicene.ddl.validation.dag.impl;

import com.google.common.collect.Sets;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.model.dag.DAGType;
import com.weibo.rill.flow.interfaces.model.resource.BaseResource;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;
import com.weibo.rill.flow.olympicene.ddl.exception.DDLException;
import com.weibo.rill.flow.olympicene.ddl.exception.ValidationException;
import com.weibo.rill.flow.olympicene.ddl.validation.DAGValidator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;


public class ResourceDAGValidator implements DAGValidator {
    private final Pattern namePattern = Pattern.compile("^[a-zA-Z0-9]+$");

    @Override
    public boolean match(DAG target) {
        return target.getType() == DAGType.RESOURCE;
    }

    @Override
    public void validate(DAG target) {
        if (CollectionUtils.isNotEmpty(target.getTasks())) {
            throw new DDLException(DDLErrorCode.DAG_DESCRIPTOR_INVALID.getCode(), "resource type dag do not support tasks property");
        }

        List<BaseResource> resources = target.getResources();
        if (CollectionUtils.isEmpty(resources)) {
            throw new DDLException(DDLErrorCode.DAG_DESCRIPTOR_INVALID.getCode(), "dag resources empty");
        }

        Set<String> names = Sets.newHashSet();
        for (BaseResource resource : resources) {
            String name = resource.getName();

            if (StringUtils.isBlank(name) || name.length() > 100 || !namePattern.matcher(name).find()) {
                throw new ValidationException(DDLErrorCode.NAME_INVALID.getCode(), "resource name invalid:" + name);
            }

            if (names.contains(name)) {
                throw new ValidationException(DDLErrorCode.NAME_INVALID.getCode(), "resource name duplicate:" + name);
            }
            names.add(name);
        }
    }
}
