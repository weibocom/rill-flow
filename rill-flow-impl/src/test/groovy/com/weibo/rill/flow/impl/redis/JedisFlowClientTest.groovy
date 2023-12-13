package com.weibo.rill.flow.impl.redis

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import spock.lang.Specification

import java.util.function.Consumer

class JedisFlowClientTest extends Specification {
    JedisFlowClient jedisFlowClient
    JedisPool jedisPool = Mock()
    Jedis jedis = Mock()

    def setup() {
        jedisFlowClient = new JedisFlowClient(jedisPool)

        jedisPool.getResource() >> jedis
    }

    def "client should invoke return resource after query"() {
        when:
        jedisFlowClient.set('a', 'b')

        then:
        1 * jedis.set(_, _)
        1 * jedisPool.returnResource(jedis)
        0 * jedisPool.returnBrokenResource(_)
    }

    def "client should return broken resource if client throws an exception"() {
        given:
        jedis.set(*_) >> {
            throw new RuntimeException()
        }

        when:
        jedisFlowClient.set('a', 'b')

        then:
        thrown(RuntimeException)
        0 * jedisPool.returnResource(jedis)
        1 * jedisPool.returnBrokenResource(_)
    }

    def "Pipelined should close jedis pipeline automatically"() {
        given:
        Pipeline mockPipeline = Mock()
        jedis.pipelined() >> mockPipeline

        when:
        jedisFlowClient.pipelined().accept(new Consumer<Pipeline>() {
            @Override
            void accept(Pipeline pipeline) {
                pipeline.set('a', 'b')
            }
        })

        then:
        1 * mockPipeline.set(*_)

        then:
        1 * mockPipeline.close()

        then:
        1 * jedisPool.returnResource(jedis)
        0 * jedisPool.returnBrokenResource(jedis)
    }
}
