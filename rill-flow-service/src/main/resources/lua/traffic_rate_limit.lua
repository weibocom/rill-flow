--traffic_rate_limit.lua
local rate_limit_key = KEYS[1]
local max_permits = ARGV[1]
local incr_by_count = 1

local result = redis.call('incrby', rate_limit_key, incr_by_count)
if result == incr_by_count then
    redis.call('expire', rate_limit_key, 1)
end
if tonumber(max_permits) >= result then
    return 1
end
return 0