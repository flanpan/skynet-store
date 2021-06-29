local skynet = require "skynet"
local util = require "store_util"

local strformat = string.format
local tinsert = table.insert
local mysql_execute = util.mysql_execute
local redis_execute = util.redis_execute
local redis_key_field = util.redis_execute
local quote_sql_str = util.quote_sql_str
local decode = util.decode
local encode = util.encode
local guid = util.guid
local traceback = debug.traceback

local mt = {}
mt.__index = mt

function mt:ctor(id, conf)
    assert(conf)
    assert(type(id) == "number")
    self.conf = conf
    self.id = id

    local redisd_addr = conf.redisd_addr
    local mysqld_addr = conf.mysqld_addr

    -- load from redis
    if redisd_addr then
        local key, _ = redis_key_field(conf, 0)
        local res = redis_execute(redisd_addr, "hgetall", key)
        local sz = #res
        if sz > 0 then
            local rs = {}
            for i=1, sz, 2 do
                local id = tonumber(res[i])
                rs[id] = decode(res[i+1])
            end
            self.rs = rs
        end
    end

    -- load from mysql
    if not self.rs and mysqld_addr then
        local mysql_idname = conf.mysql_idname
        local mysql_valname = conf.mysql_valname
        local sql = strformat([[SELECT `%s`, `%s` FROM `%s` where `%s` = %d]],
            mysql_idname, mysql_valname,
            conf.tbl, conf.mysql_owner_idname, self.id)
        local res = mysql_execute(mysqld_addr, sql)
        local sz = #res
        if sz > 0 then
            local rs = {}
            for i=1, sz do
                local data = res[i]
                local id = data[mysql_idname]
                assert(type(id) == "number", id)
                rs[id] = data[mysql_valname]
            end
            self.rs = rs
        end
    end

    if not self.rs then
        self.rs = {}
    end

    -- dirty id
    self.dirty = {}
end

function mt:guid()
    return guid()
end

function mt:get(id)
    return self.rs[id]
end

function mt:set(id, val)
    local conf = self.conf
    if self.rs[id] then
        if val then
            -- update
            self.dirty[id] = true
        else
            -- delete
            self.dirty[id] = nil
            if conf.mysqld_addr then
                local sql = strformat([[delete from `%s` where `%s` = %d and `%s` = %d limit 1]],
                    conf.tbl, conf.mysql_idname, id, conf.mysql_owner_idname, self.id)
                mysql_execute(conf.mysqld_addr, sql)
            end
            if conf.redisd_addr then
                local key, field = redis_key_field(conf, id)
                redis_execute(conf.redisd_addr, "hdel", key, field )
            end
        end
    elseif val then
        -- insert
        local sval = encode(val)
        assert(sval)
        if conf.mysqld_addr then
            local sql= strformat([[insert into `%s`(`%s`,`%s`,`%s`) values (%d,%d,%s)]],
                conf.tbl, conf.mysql_idname, conf.mysql_owner_idname,
                conf.mysql_valname, id, self.id, quote_sql_str(sval))
            mysql_execute(conf.mysqld_addr, sql)
        end
        if conf.redisd_addr then
            local key, field = redis_key_field(conf, id)
            redis_execute(conf.redisd_addr, "hset", key, field, sval)
        end
    end

    self.rs[id] = val
end

function mt:_collect_dirty(id, rediscmds, sqls)
    local val = self.rs[id]
    val = encode(val)
    assert(val)
    local conf = self.conf
    local redisd_addr = conf.redid_addr
    local mysqld_addr = conf.mysqld_addr

    if redisd_addr then
        local key, field = redis_key_field(conf, id)
        local list = rediscmds[redisd_addr]
        if not list then
            list = {}
            rediscmds[redisd_addr] = list
        end
        tinsert(list, {"HSET", key, field, val})
    end

    if mysqld_addr then
        local sql = strformat([[UPDATE `%s` SET `%s` = %s where `%s` = %d and `%s` = %d limit 1]],
            conf.tbl, conf.mysql_valname, quote_sql_str(val),
            conf.mysql_idname, id, conf.mysql_owner_idname, self.id)

        local list = sqls[mysqld_addr]
        if not list then
            list = {}
            sqls[mysqld_addr] = list
        end
        tinsert(list, sql)
    end
end

function mt:collect_dirty(rediscmds, sqls)
    if not next(self.dirty) then
        return
    end
    for id, _ in pairs(self.dirty) do
        xpcall(self._collect_dirty, traceback, self, id, rediscmds, sqls)
    end
    self.dirty = {}
end

function mt:flush()
    local conf = self.conf
    local rediscmds = {}
    local sqls = {}

    self:collect_dirty(rediscmds, sqls)

    if #rediscmds > 0 then
        xpcall(redis_execute, traceback, conf.redisd_addr, rediscmds)
    end

    if #sqls > 0 then
        mysql_execute(conf.mysqld_addr, sqls)
    end
end

return {
    new = function(...)
        local obj = setmetatable({}, mt)
        obj:ctor(...)
        return obj
    end
}