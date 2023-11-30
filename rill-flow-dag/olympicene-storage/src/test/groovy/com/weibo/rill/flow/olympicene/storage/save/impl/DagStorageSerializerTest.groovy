package com.weibo.rill.flow.olympicene.storage.save.impl

import com.google.common.collect.ImmutableMap
import com.weibo.rill.flow.interfaces.model.task.TaskInfo
import spock.lang.Specification

import java.util.stream.Collectors

class DagStorageSerializerTest extends Specification {
    def "serializeHash should add value class type"() {
        when:
        Map<String, Object> content = ImmutableMap.of("key", new TaskInfo())
        Map<String, String> ret = DagStorageSerializer.serializeHash(content)

        then:
        ret.get(DagStorageSerializer.buildTypeKeyPrefix("key")) == TaskInfo.class.getName()
    }

    def "deserializeHash should get the original type"() {
        when:
        Map<String, Object> content = ImmutableMap.of("key", new TaskInfo())
        List<byte[]> serial = DagStorageSerializer.serializeHashToList(content).stream()
                .map{DagStorageSerializer.getBytes(it)}
                .collect(Collectors.toList())
        Map<String, Object> ret = DagStorageSerializer.deserializeHash(serial)

        then:
        ret.get("key") in TaskInfo
    }
}
