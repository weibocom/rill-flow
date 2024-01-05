package com.weibo.rill.flow.olympicene.traversal.json

import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.weibo.rill.flow.interfaces.model.mapping.Mapping
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping
import spock.lang.Specification
import spock.lang.Unroll

class JSONPathTest extends Specification {
    JSONPathInputOutputMapping mapping = new JSONPathInputOutputMapping()
    Configuration conf = Configuration.builder()
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .build()
    Configuration conf2 = Configuration.builder()
            .options(Option.SUPPRESS_EXCEPTIONS)
            .options(Option.AS_PATH_LIST)
            .build()
    String data = "{\n" +
            "    \"store\": {\n" +
            "        \"book\": [\n" +
            "            {\n" +
            "                \"category\": \"reference\",\n" +
            "                \"author\": \"Nigel Rees\",\n" +
            "                \"title\": \"Sayings of the Century\",\n" +
            "                \"price\": 8.95\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"Evelyn Waugh\",\n" +
            "                \"title\": \"Sword of Honour\",\n" +
            "                \"price\": 12.99\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"Herman Melville\",\n" +
            "                \"title\": \"Moby Dick\",\n" +
            "                \"isbn\": \"0-553-21311-3\",\n" +
            "                \"price\": 8.99\n" +
            "            },\n" +
            "            {\n" +
            "                \"category\": \"fiction\",\n" +
            "                \"author\": \"J. R. R. Tolkien\",\n" +
            "                \"title\": \"The Lord of the Rings\",\n" +
            "                \"isbn\": \"0-395-19395-8\",\n" +
            "                \"price\": 22.99\n" +
            "            }\n" +
            "        ],\n" +
            "        \"bicycle\": {\n" +
            "            \"color\": \"red\",\n" +
            "            \"price\": 19.95\n" +
            "        }\n" +
            "    },\n" +
            "    \"expensive\": 10\n" +
            "}"

    def "set value intermediate route test"() {
        given:
        String path = "\$.input.meta.user.id"
        long value = 1

        when:
        mapping.setValue(map, value, path)

        then:
        mapping.getValue(map, path) == value

        where:
        map                        | _
        [:]                        | _
        ['context': 123]           | _
        ['input': ['type': 'gif']] | _
    }

    def "parse string source to object"() {
        when:
        def value = mapping.parseSource(source)

        then:
        value == target

        where:
        source                | target
        'true'                | true
        'false '              | false
        '123'                 | 123
        '123.1'               | 123.1
        '00.1'                | 0.1
        '.1'                  | 0.1
        '1.1.1'               | '1.1.1'
        '{"a":1}'             | ['a': 1]
        '{"a":{"b":1}}'       | ['a': ['b': 1]]
        '{"a":{"b":xxx}}'     | '{"a":{"b":xxx}}'
        '{"a":{"b":[1,2,3]}}' | ['a': ['b': [1, 2, 3]]]
        '[{"a":1},{"b":1}]'   | [['a': 1], ['b': 1]]
        '[[1],2,{"a":3}]'     | [[1], 2, ['a': 3]]
        'string'              | 'string'
        ' string\t'           | ' string\t'
    }

    def "mapping transform rule handle source value test"() {
        when:
        Map<String, Object> emptyMap = Collections.emptyMap()
        def value = mapping.transformSourceValue(source, emptyMap, emptyMap, emptyMap, "source.followerCount > 3000 ? true : false")

        then:
        value == target

        where:
        source                  | target
        ['followerCount': 1000] | false
        ['followerCount': 3000] | false
        ['followerCount': 3100] | true

    }

    def "mapping const and dynamic source test"() {
        given:
        List<Mapping> rules = []
        rules.add(new Mapping('source': '$.context.a', 'target': '$.input.number.a'))
        rules.add(new Mapping('source': '[{"url":"a", "name":"name1"},{"url":"b", "name":"name2"}]', 'target': '$.input.meta.array'))
        rules.add(new Mapping('source': '$.context.url', 'target': '$.input.meta.array[0].url'))
        Map<String, Object> context = ['a': '123', 'url': 'http1']
        Map<String, Object> input = [:]

        when:
        mapping.mapping(context, input, [:], rules)

        then:
        noExceptionThrown()
        input == ['number': ['a': '123'], 'meta': ['array': [['url': 'http1', 'name': 'name1'], ['url': 'b', 'name': 'name2']]]]
    }

    @Unroll
    def "test json path get value should work well"() {
        when:
        def result = JsonPath.using(conf).parse(data).read(path)
        then:
        expected == result
        where:
        path                                 || expected
        '$.expensive'                         | 10
        '$.store.bicycle.color'               | 'red'
        '$.store.book[0].author'              | 'Nigel Rees'
        '$.store.book.length()'               | 4
        '$.store.book[?(@.price > 20)].price' | [22.99]
    }

    @Unroll
    def "test json path should match well"() {
        when:
        List<String> result = JsonPath.using(conf2).parse(data).read(path)
        then:
        result.size() > 0
        where:
        path                                   || expected
        '$.expensive'                           | 10
        '$.store.bicycle.[?(@.color == "red")]' | [22.99]
        '$.store.bicycle.[?(@.price > 10)]'     | [22.99]
    }

    @Unroll
    def "test json path should not match "() {
        when:
        List<String> result = JsonPath.using(conf2).parse(data).read(path)
        then:
        result.size() == 0
        where:
        path                                    || expected
        '$.store.bicycle.[?(@.color == "red1")]' | [22.99]
        '$.store.bicycle.[?(@.price > 100)]'     | [22.99]
    }
}
