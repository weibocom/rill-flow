--dag_info_get.lua
local ret = {};
local rootContent = {};
local rootHGetAll = redis.call("hgetall", KEYS[1]);
for rootIndex = 1, #rootHGetAll, 2 do
    local rootField = rootHGetAll[rootIndex];
    local rootValue = rootHGetAll[rootIndex + 1];

    if (rootField == "dag" and string.find(rootValue, "\"dag_descriptor_") == 1) then
        local descriptorKey = string.gsub(rootValue, "\"", "");
        rootHGetAll[rootIndex + 1] = redis.call("get", descriptorKey);
    elseif (rootField == "@class_dag") then
        rootHGetAll[rootIndex + 1] = "com.weibo.rill.flow.olympicene.core.model.dag.DAG";
    end
end
table.insert(rootContent, {"name", KEYS[1]});
table.insert(rootContent, rootHGetAll);
table.insert(ret, rootContent);

if (#rootHGetAll == 0 or not KEYS[2])
then
    return ret;
end
local contentNameToContentRedisKey = redis.call("hgetall", KEYS[2])
for index = 1, #contentNameToContentRedisKey, 2 do
    local contentName = contentNameToContentRedisKey[index];
    local contentRedisKey = contentNameToContentRedisKey[index + 1];
    local subContent = {};
    table.insert(subContent, { "name", contentName});
    table.insert(subContent, redis.call("hgetall", contentRedisKey));
    table.insert(ret, subContent);
end
return ret;