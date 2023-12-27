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

package com.weibo.rill.flow.impl.redis;

import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JedisFlowClient implements RedisClient {
    private JedisPool jedisPool;

    public JedisFlowClient(String host, int port) {
        jedisPool = new JedisPool(host, port);
    }

    public JedisFlowClient(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private <R> R doExecute(Function<Jedis, R> callable) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return callable.apply(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return doExecute(jedis -> jedis.hmset(key, hash));
    }

    @Override
    public String hmset(String shardingKey, String key, Map<String, String> hash) {
        return doExecute(jedis -> jedis.hmset(key, hash));
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return doExecute(jedis -> jedis.hmset(key, hash));
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return doExecute((jedis -> jedis.hmget(key, fields)));
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return doExecute(jedis -> jedis.hmget(key, fields));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return doExecute(jedis -> jedis.hgetAll(key));
    }

    @Override
    public Map<String, String> hgetAll(String shardingKey, String key) {
        return doExecute(jedis -> jedis.hgetAll(key));
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return doExecute(jedis -> jedis.hgetAll(key));
    }

    @Override
    public void hdel(byte[] key, byte[]... fields) {
        doExecute(jedis -> jedis.hdel(key, fields));
    }

    @Override
    public int hdel(String key, String... fields) {
        doExecute(jedis -> jedis.hdel(key, fields));
        return 0;
    }

    @Override
    public void hdel(String shardingKey, String key, Collection<String> fields) {
        doExecute(jedis -> jedis.hdel(key, fields.toArray(new String[0])));
    }

    @Override
    public Long expire(String key, int seconds) {
        return doExecute(jedis -> jedis.expire(key, seconds));
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return doExecute(jedis -> jedis.expire(key, seconds));
    }

    @Override
    public Boolean exists(String key) {
        return doExecute(jedis -> jedis.exists(key));
    }

    @Override
    public Boolean exists(byte[] key) {
        return doExecute(jedis -> jedis.exists(key));
    }

    @Override
    public String get(String key) {
        return doExecute(jedis -> jedis.get(key));
    }

    @Override
    public String get(String shardingKey, String key) {
        return doExecute(jedis -> jedis.get(key));
    }

    @Override
    public byte[] get(byte[] key) {
        return doExecute(jedis -> jedis.get(key));
    }

    @Override
    public String set(String key, String value) {
        return doExecute(jedis -> jedis.set(key, value));
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return doExecute(jedis -> jedis.set(key, value));
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return doExecute(jedis -> jedis.setex(key, seconds, value));
    }

    @Override
    public Long setnx(String key, String value) {
        return doExecute(jedis -> jedis.setnx(key, value));
    }

    @Override
    public Long del(byte[] key) {
        return doExecute(jedis -> jedis.del(key));
    }

    @SuppressWarnings("UnusedReturnValue")
    public Long del(String... keys) {
        return doExecute(jedis -> jedis.del(keys));
    }

    @Override
    public Long sadd(String key, String... members) {
        return doExecute(jedis -> jedis.sadd(key, members));
    }

    @Override
    public Long sadd(String shardingKey, String key, Collection<String> members) {
        return doExecute(jedis -> jedis.sadd(key, members.toArray(new String[0])));
    }

    @Override
    public Long srem(String key, String... members) {
        return doExecute(jedis -> jedis.srem(key, members));
    }

    @Override
    public Long srem(String shardingKey, String key, Collection<String> members) {
        return doExecute(jedis -> jedis.srem(key, members.toArray(new String[0])));
    }

    @Override
    public Set<String> smembers(String shardingKey, String key) {
        return doExecute(jedis -> jedis.smembers(key));
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return doExecute(jedis -> jedis.zadd(key, score, member));
    }

    @Override
    public Long zadd(String key, Map<Double, String> scoreMembers) {
        return doExecute(jedis -> jedis.zadd(key, scoreMembers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey))));
    }

    @Override
    public Long zrem(String key, String member) {
        return doExecute(jedis -> jedis.zrem(key, member));
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return doExecute(jedis -> jedis.zremrangeByScore(key, start, end));
    }

    @Override
    public Set<String> zrange(String shardingKey, String key, int start, int end) {
        return new HashSet<>(doExecute(jedis -> jedis.zrange(key, start, end)));
    }

    @Override
    public Set<Pair<String, Double>> zrangeWithScores(String shardingKey, String key, int start, int end) {
        return tupleListToPairSet(doExecute(jedis -> jedis.zrangeWithScores(key, start, end)));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return new HashSet<>(doExecute(jedis -> jedis.zrangeByScore(key, min, max)));
    }

    @Override
    public Set<Pair<String, Double>> zrangeByScoreWithScores(String key, double min, double max) {
        return tupleListToPairSet(doExecute(jedis -> jedis.zrangeByScoreWithScores(key, min, max)));
    }

    @NotNull
    private Set<Pair<String, Double>> tupleListToPairSet(List<Tuple> redisRes) {
        Set<Pair<String, Double>> result = new HashSet<>();
        for (Tuple tuple : redisRes) {
            Pair<String, Double> pair = ImmutablePair.of(tuple.getElement(), tuple.getScore());
            result.add(pair);
        }
        return result;
    }

    @Override
    public Set<Pair<String, Double>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return tupleListToPairSet(doExecute(jedis -> jedis.zrevrangeByScoreWithScores(key, max, min, offset, count)));
    }

    @Override
    public long incr(String key) {
        return doExecute(jedis -> jedis.incr(key));
    }

    @Override
    public long hset(String key, String field, String value) {
        return doExecute(jedis -> jedis.hset(key, field, value));

    }

    @Override
    public Set<Pair<String, Double>> zrevrangeByScoreWithScores(String shardingKey, String key, double max, double min, int offset, int count) {
        return zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Long zcard(String key) {
        return doExecute(jedis -> jedis.zcard(key));
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return doExecute(jedis -> jedis.zcount(key, min, max));
    }

    @Override
    public List<String> configGet(String shardingKey, String pattern) {
        return new ArrayList<>();
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return turnStringToByteArray(doExecute(jedis -> jedis.eval(script, keys, args)));
    }

    @Override
    public Object eval(String script, String shardingKey, List<String> keys, List<String> args) {
        return eval(script, keys, args);
    }

    @SuppressWarnings("unchecked")
    private Object turnStringToByteArray(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof List) {
            List<Object> result = new ArrayList<>();
            //noinspection unchecked
            for (Object element : (List<Object>) object) {
                result.add(turnStringToByteArray(element));
            }
            return result;
        } else if (object instanceof String) {
            return ((String) object).getBytes(StandardCharsets.UTF_8);
        } else {
            return object;
        }
    }

    @Override
    public Set<String> hkeys(String key) {
        return doExecute(jedis -> jedis.hkeys(key));
    }

    public Consumer<Consumer<Pipeline>> pipelined() {
        return ((Consumer<Pipeline> consumer) -> doExecute(jedis -> {
            try (Pipeline pipeline = jedis.pipelined()) {
                consumer.accept(pipeline);
            }
            return 0;
        }));
    }

    @Override
    public String hget(String key, String field) {
        return doExecute(jedis -> jedis.hget(key, field));
    }

}
