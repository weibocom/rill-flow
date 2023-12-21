--redis_get_by_field_and_key.lua
local ret = {};
local argvIndex = 1;
for k = 1, #KEYS, 1 do
    local key = KEYS[k];
    local args = {};
    for _ = argvIndex, #ARGV, 1 do
        local arg = ARGV[argvIndex];
        argvIndex = argvIndex + 1;
        if (arg == "_placeholder_")
        then
            break;
        else
            table.insert(args, arg);
        end
    end

    if (#args == 0) then
        table.insert(ret, redis.call("hgetall", key));
    elseif (args[1] == "_key_prefix_") then
        local keyPrefix = args[2]
        local filteredMapKeys = {}

        local mapKeys = redis.call("hkeys", key)
        for mapKeyIndex = 1, #mapKeys, 1 do
            if (string.find(mapKeys[mapKeyIndex], keyPrefix) == 1) then
                table.insert(filteredMapKeys, mapKeys[mapKeyIndex])
            end
        end

        if (#filteredMapKeys == 0) then
            table.insert(ret, {})
        else
            table.insert(ret, redis.call("hmget", key, unpack(filteredMapKeys)));
        end
    else
        table.insert(ret, redis.call("hmget", key, unpack(args)));
    end
end

return ret;