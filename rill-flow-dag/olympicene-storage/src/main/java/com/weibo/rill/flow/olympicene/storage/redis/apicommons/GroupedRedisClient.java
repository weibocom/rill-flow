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

package com.weibo.rill.flow.olympicene.storage.redis.apicommons;

import com.weibo.rill.flow.olympicene.storage.redis.api.RedisClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class GroupedRedisClient implements RedisClient {

    public abstract RedisClient choose(String shardingKey);

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return choose(key).hmset(key, hash);
    }

    @Override
    public String hmset(String shardingKey, String key, Map<String, String> hash) {
        return choose(shardingKey).hmset(shardingKey, key, hash);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return choose(key).hmset(key, hash);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return choose(key).hmget(key, fields);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return choose(key).hmget(key, fields);
    }


    @Override
    public long hset(String key, String field, String value) {
        return choose(key).hset(key, field, value);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return choose(key).hgetAll(key);
    }

    @Override
    public Map<String, String> hgetAll(String shardingKey, String key) {
        return choose(shardingKey).hgetAll(shardingKey, key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return choose(key).hgetAll(key);
    }

    @Override
    public void hdel(byte[] key, byte[]... fields) {
        choose(key).hdel(key, fields);
    }

    @Override
    public int hdel(String key, String... fields) {
        return choose(key).hdel(key, fields);
    }

    @Override
    public void hdel(String shardingKey, String key, Collection<String> fields) {
        choose(shardingKey).hdel(shardingKey, key, fields);
    }

    @Override
    public Long expire(String key, int seconds) {
        return choose(key).expire(key, seconds);
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return choose(key).expire(key, seconds);
    }

    @Override
    public Boolean exists(String key) {
        return choose(key).exists(key);
    }

    @Override
    public Boolean exists(byte[] key) {
        return choose(key).exists(key);
    }

    @Override
    public long incr(String key) {
        return choose(key).incr(key);
    }

    @Override
    public String get(String key) {
        return choose(key).get(key);
    }

    @Override
    public String get(String shardingKey, String key) {
        return choose(shardingKey).get(shardingKey, key);
    }

    @Override
    public byte[] get(byte[] key) {
        return choose(key).get(key);
    }

    @Override
    public String set(String key, String value) {
        return choose(key).set(key, value);
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return choose(key).set(key, value);
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return choose(key).setex(key, seconds, value);
    }

    @Override
    public Long setnx(String key, String value) {
        return choose(key).setnx(key, value);
    }

    @Override
    public Long del(byte[] key) {
        return choose(key).del(key);
    }

    @Override
    public Long sadd(String key, String... members) {
        return choose(key).sadd(key, members);
    }

    @Override
    public Long sadd(String shardingKey, String key, Collection<String> members) {
        return choose(shardingKey).sadd(shardingKey, key, members);
    }

    @Override
    public Long srem(String key, String... members) {
        return choose(key).srem(key, members);
    }

    @Override
    public Long srem(String shardingKey, String key, Collection<String> members) {
        return choose(shardingKey).srem(shardingKey, key, members);
    }

    @Override
    public Set<String> smembers(String shardingKey, String key) {
        return choose(shardingKey).smembers(shardingKey, key);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return choose(key).zadd(key, score, member);
    }

    @Override
    public Long zadd(String key, Map<Double, String> scoreMembers) {
        return choose(key).zadd(key, scoreMembers);
    }

    @Override
    public Long zrem(String key, String member) {
        return choose(key).zrem(key, member);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return choose(key).zremrangeByScore(key, start, end);
    }

    @Override
    public Set<String> zrange(String shardingKey, String key, int start, int end) {
        return choose(shardingKey).zrange(shardingKey, key, start, end);
    }

    @Override
    public Set<Pair<String, Double>> zrangeWithScores(String shardingKey, String key, int start, int end) {
        return choose(shardingKey).zrangeWithScores(shardingKey, key, start, end);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return choose(key).zrangeByScore(key, min, max);
    }

    @Override
    public Set<Pair<String, Double>> zrangeByScoreWithScores(String key, double min, double max) {
        return choose(key).zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Pair<String, Double>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return choose(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Set<Pair<String, Double>> zrevrangeByScoreWithScores(String shardingKey, String key, double max, double min, int offset, int count) {
        return choose(shardingKey).zrevrangeByScoreWithScores(shardingKey, key, max, min, offset, count);
    }

    @Override
    public Long zcard(String key) {
        return choose(key).zcard(key);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return choose(key).zcount(key, min, max);
    }

    @Override
    public List<String> configGet(String shardingKey, String pattern) {
        return choose(shardingKey).configGet(shardingKey, pattern);
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        String shardingKey = CollectionUtils.isEmpty(keys) ? "null" : keys.get(0);
        return choose(shardingKey).eval(script, keys, args);
    }

    @Override
    public Object eval(String script, String shardingKey, List<String> keys, List<String> args) {
        return choose(shardingKey).eval(script, shardingKey, keys, args);
    }

    @Override
    public Set<String> hkeys(String key) {
        return choose(key).hkeys(key);
    }

    @Override
    public String hget(String key, String field) {
        return choose(key).hget(key, field);
    }

    private RedisClient choose(byte[] shardingKey) {
        return choose(new String(shardingKey, StandardCharsets.UTF_8));
    }

}
