--redis_get_timeout.lua
local key = KEYS[1]
local minScore = ARGV[1]
local maxScore = ARGV[2]
local offset = ARGV[3]
local count = ARGV[4]
local members = redis.call('zrangebyscore', key, minScore, maxScore, 'limit', offset, count)
if (#members > 0)
then
    redis.call('zrem', key, unpack(members))
end
return members