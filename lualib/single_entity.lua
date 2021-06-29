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
local traceback = debug.traceback

local mt = {}
mt.__index = mt

function mt:ctor(id, conf)
    assert(conf)
    assert(type(id) == "number")
    self.conf = conf
    self.id = id
    -- load record
    if conf.redisd_addr then
        local key, field = redis_key_field(conf)
        local ret = redis_execute(conf.redisd_addr, "hget", key, field)
        self.rs = decode(ret)
    end

    if not self.rs and conf.mysqld_addr then
        local sql = strformat("select `%s` from `%s` where `%s` = %d limit 1",
            conf.mysql_valname, conf.tbl, conf.mysql_idname, self.id)

        local ret = mysql_execute(conf.mysqld_addr, sql)
        if ret then
            self.rs = decode(ret[1][conf.mysql_valname])
        end
    end

    if not self.rs and not conf.readonly then
        self:set({})
    end

    self.dirty = false
end

function mt:get()
    return self.rs
end

function mt:set(val)
    local conf = self.conf
    assert(not conf.readonly, conf.tbl)

    local sval = encode(val)
    if not sval then
        assert(false, ("[tbl] %s [id] %s"):format(conf.tbl, self.id))
        return
    end

    if not self.rs then
        -- insert
        if conf.mysqld_addr then
            local sql = strformat([[insert into `%s`(`%s`, `%s`) values (%d, %s)]],
                conf.tbl, conf.mysql_idname, conf.mysql_valname,
                self.id, quote_sql_str(sval))

            mysql_execute(conf.mysqld_addr, sql)
        end

        if conf.redisd_addr then
            local key, field = redis_key_field(conf)
            redis_execute(conf.redisd_addr, "hset", key, field, sval)
        end
    else
        self.dirty = true
    end
    self.rs = val
end

function mt:collect_dirty(rediscmds, sqls)
    local conf = self.conf
    if not self.dirty then return end
    local val = encode(self.rs)
    local redisd_addr = conf.redisd_addr
    local mysqld_addr = conf.mysqld_addr

    if redisd_addr then
        local key, field = redis_key_field(conf)
        local list = rediscmds[redisd_addr]
        if not list then
            list = {}
            rediscmds[redisd_addr] = list
        end
        tinsert(list, {"hset", key, field, val })
    end

    if mysqld_addr then
        local sql = strformat([[update `%s` set `%s` = %s where `%s` = %d limit 1]],
            conf.tbl, conf.mysql_valname, quote_sql_str(val), 
            conf.mysql_idname, self.id)
        local list = sqls[mysqld_addr]
        if not list then
            list = {}
            sqls[mysqld_addr] = list
        end
        tinsert(list, sql)
    end

    self.dirty = false
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