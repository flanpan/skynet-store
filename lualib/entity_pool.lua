local skynet = require "skynet"
local single_entity = require "single_entity"
local multi_entity = require "multi_entity"
local util = require "store_util"

local traceback = debug.traceback

local mt = {}
mt.__index = mt

function mt:ctor(opts)
    self.conf = util.config(opts)
    self.entities = {}
end

function mt:load(id)
    local conf = self.conf
    local entity
    if conf.multi then
        entity = multi_entity.new(id, conf)
    else
        entity = single_entity.new(id, conf)
    end
    assert(entity)
    self.entities[id] = entity
    return entity
end

function mt:unload(id)
    local entity = self.entities[id]
    if not entity then return end
    entity:flush()
    self.entities[id] = nil
end

function mt:unload_all()
    self:flush()
    self.entities = {}
end

function mt:entity(id)
    return self.entities[id]
end

function mt:get(id, ...)
    local entity = self.entities[id]
    assert(entity, id)
    if self.conf.multi then
        local subid = ...
        return entity:get(subid)
    else
        return entity:get()
    end
end

function mt:set(id, ...)
    local data1, data2 = ...
    local entity = self.entities[id]
    assert(entity, id)
    if self.conf.multi then
        entity:set(data1, data2)
    else
        entity:set(data1)
    end
end

function mt:flush()
    local rediscmds = {}
    local sqls = {}

    for _, entity in pairs(self.entities) do
        entity:collect_dirty(rediscmds, sqls)
    end

    for addr, cmds in pairs(rediscmds) do
        xpcall(util.redis_execute, traceback, addr, cmds)
    end

    for addr, sqls in pairs(sqls) do
        xpcall(util.mysql_execute, traceback, addr, sqls)
    end
end

function mt:dump()
    local obj = {}
    for key, entity in pairs(self.entities) do
        obj[key] = entity.rs
    end
    return util.encode(obj)
end

return {
    new = function(opts)
        local obj = setmetatable({}, mt)
        obj:ctor(opts)
        return obj
    end
}