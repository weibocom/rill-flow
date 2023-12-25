--dag_info_get_by_field.lua
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
            break ;
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
        local hmgetContent = redis.call("hmget", key, unpack(args));
        if (string.find(key, "dag_info_") == 1) then
            for hmgetIndex = 1, #hmgetContent, 1 do
                local field = args[hmgetIndex];
                local value = hmgetContent[hmgetIndex];
                if (field == "dag" and string.find(value, "\"dag_descriptor_") == 1) then
                    local descriptorKey = string.gsub(value, "\"", "");
                    hmgetContent[hmgetIndex] = redis.call("get", descriptorKey);
                elseif (field == "@class_dag") then
                    hmgetContent[hmgetIndex] = "com.weibo.rill.flow.olympicene.core.model.dag.DAG";
                end
            end
        end
        table.insert(ret, hmgetContent);
    end
end

return ret;