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

package com.weibo.rill.flow.olympicene.storage.redis.api;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisClient {
    // hash
    String hmset(String key, Map<String, String> hash);

    String hmset(String shardingKey, String key, Map<String, String> hash);

    String hmset(byte[] key, Map<byte[], byte[]> hash);

    List<byte[]> hmget(byte[] key, byte[]... fields);

    List<String> hmget(String key, String... fields);

    Map<String, String> hgetAll(String key);

    Map<String, String> hgetAll(String shardingKey, String key);

    Map<byte[], byte[]> hgetAll(byte[] key);

    void hdel(byte[] key, byte[]... fields);

    int hdel(String key, String... fields);

    void hdel(String shardingKey, String key, Collection<String> fields);

    // key
    Long expire(String key, int seconds);

    Long expire(byte[] key, int seconds);

    Boolean exists(String key);

    Boolean exists(byte[] key);

    String get(String key);

    String get(String shardingKey, String key);

    byte[] get(byte[] key);

    String set(String key, String value);

    String set(byte[] key, byte[] value);

    String setex(String key, int seconds, String value);

    Long setnx(String key, String value);

    Long del(byte[] key);

    // set
    Long sadd(String key, String... members);

    Long sadd(String shardingKey, String key, Collection<String> members);

    Long srem(String key, String... members);

    Long srem(String shardingKey, String key, Collection<String> members);

    Set<String> smembers(String shardingKey, String key);

    // zset
    Long zadd(String key, double score, String member);

    Long zadd(String key, Map<Double, String> scoreMembers);

    Long zrem(String key, String member);

    Long zremrangeByScore(String key, double start, double end);

    Set<String> zrange(String shardingKey, String key, int start, int end);

    Set<Pair<String, Double>> zrangeWithScores(String shardingKey, String key, int start, int end);

    Set<String> zrangeByScore(String key, double min, double max);

    Set<Pair<String, Double>> zrangeByScoreWithScores(String key, double min, double max);

    Set<Pair<String, Double>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count);

    Set<Pair<String, Double>> zrevrangeByScoreWithScores(String shardingKey, String key, double max, double min, int offset, int count);

    Long zcard(String key);

    Long zcount(String key, double min, double max);

    List<String> configGet(String shardingKey, String pattern);

    // script
    Object eval(String script, List<String> keys, List<String> args);
    Object eval(String script, String shardingKey, List<String> keys, List<String> args);
    long incr(String key);
    long hset(String key, String field, String value);

    Set<String> hkeys(String key);

    String hget(String key, String field);
}
