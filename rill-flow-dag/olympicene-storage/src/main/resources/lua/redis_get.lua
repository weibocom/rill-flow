--redis_get.lua
local ret = {};
local rootContent = {};
local rootHGetAll = redis.call("hgetall", KEYS[1]);
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