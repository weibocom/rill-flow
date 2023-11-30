--redis_lock.lua
local val = redis.call("get", KEYS[1]);
if not val or val == ARGV[1] then
    return redis.call("setex", KEYS[1], ARGV[2], ARGV[1])
else
    return "FAIL"
end