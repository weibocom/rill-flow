--redis_set_with_expire.lua
local expireTime = ARGV[1];
local argvIndex = 3;
for keyIndex = 1, #KEYS, 1 do
    local key = KEYS[keyIndex];
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

    redis.call("hmset", key, unpack(args));
    redis.call("expire", key, expireTime)
end