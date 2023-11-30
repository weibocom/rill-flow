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

package com.weibo.rill.flow.olympicene.ddl.parser;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.olympicene.core.exception.SerializationException;
import com.weibo.rill.flow.olympicene.core.model.dag.DAG;
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser;
import com.weibo.rill.flow.olympicene.ddl.constant.DDLErrorCode;
import com.weibo.rill.flow.olympicene.ddl.exception.DDLException;
import com.weibo.rill.flow.olympicene.ddl.serialize.Serializer;
import com.weibo.rill.flow.olympicene.ddl.validation.DAGValidator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * 将string类型的dag描述内容转换为DAG bean
 */
public class DAGStringParser implements DAGParser<String> {

    private final Serializer serializer;
    private final List<DAGValidator> dagValidators = Lists.newArrayList();

    public DAGStringParser(Serializer serializer, List<DAGValidator> dagValidators) {
        this.serializer = serializer;
        Optional.ofNullable(dagValidators).ifPresent(this.dagValidators::addAll);
    }

    /**
     * 将符合{@link DAG DAG} 格式的yaml或json解析为bean
     *
     * @param dagDescriptor yaml或json格式的dag描述字符串
     * @return DAG bean
     */
    @Override
    public DAG parse(String dagDescriptor) {
        dagDescriptorValidate(dagDescriptor);

        DAG dag = deserialize(dagDescriptor);

        dagValidate(dag);

        return dag;
    }

    private void dagValidate(DAG dag) {
        List<DAGValidator> matchedValidators = dagValidators.stream()
                .filter(validator -> validator.match(dag))
                .toList();
        if (CollectionUtils.isEmpty(matchedValidators)) {
            throw new DDLException(DDLErrorCode.DAG_TYPE_INVALID);
        }
        matchedValidators.forEach(validator -> validator.validate(dag));
    }

    private void dagDescriptorValidate(String text) {
        if (StringUtils.isEmpty(text)) {
            throw new DDLException(DDLErrorCode.DAG_DESCRIPTOR_EMPTY);
        }
    }

    public String serialize(DAG dag) {
        try {
            return serializer.serializeToString(dag);
        } catch (SerializationException e) {
            throw new DDLException(DDLErrorCode.SERIALIZATION_FAIL);
        }
    }

    /**
     * 文本反序列化为DAG
     */
    public DAG deserialize(String text) {
        try {
            return serializer.deserialize(text.getBytes(StandardCharsets.UTF_8), DAG.class);
        } catch (SerializationException e) {
            throw new DDLException(DDLErrorCode.DAG_DESCRIPTOR_INVALID.getCode(), e.getMessage());
        }
    }
}
