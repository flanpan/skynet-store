local skynet = require "skynet"
local cjson = require "cjson"

local strformat = string.format
local strgsub = string.gsub
local random = math.random
local traceback = debug.traceback

local mysqld_service_num = {}
local redisd_service_num = {}
local guid_generator_addrs

cjson.encode_sparse_array(true)

local M = {}

function M.config(opts)
    local schema = opts.schema
    assert(type(schema.tbl) == "string", schema.tbl)

    local conf = {}
    conf.mysqld_addr = opts.mysqld_addr
    conf.redisd_addr = opts.redisd_addr
    conf.tbl = schema.tbl
    conf.readonly = schema.readonly
    conf.multi = false
    if schema.multi then
        conf.multi = true
    end
    if conf.mysqld then
        local field = schema.field or {}
        conf.mysql_valname = field.data or "data"
        conf.mysql_idname = field.id or "id"
        if conf.multi then
            conf.mysql_owner_idname = field.owner_id or "owner_id"
        end
    end
    return conf
end

function M.mysql_sname(dbkey, index)
    return ".mysqld." .. dbkey .. "." .. index
end

function M.redis_sname(dbkey, index)
    return ".redisd." .. dbkey .. "." .. index
end

function M.balance_mysqld_addr(dbkey, id)
    local num = mysqld_service_num[dbkey]
    if not num then
        num = skynet.call(".dbmgr", "lua", "mysql_service_num", dbkey)
        mysqld_service_num[dbkey] = num
    end
    assert(num)
    local sname = M.mysqld_sname(dbkey, (id%num)+1)
    return skynet.localname(sname)
end

function M.balance_redisd_addr(dbkey, id)
    local num = redisd_service_num[dbkey]
    if not num then
        num = skynet.call(".dbmgr", "lua", "redis_service_num", dbkey)
        redisd_service_num[dbkey] = num
    end
    assert(num)
    local sname = M.redis_sname(dbkey, (id%num)+1)
    return skynet.localname(sname)
end

function M.guid()
    if not guid_generator_addrs then
        guid_generator_addrs = skynet.call(".dbmgr", "lua", "guid_generators")
    end
    assert(guid_generator_addrs, ".dbmgr must be init")
    local num = #guid_generator_addrs
    return skynet.call(guid_generator_addrs[random(1,num)], "lua")
end

function M.encode(val)
    if not val then return end
    local ok, ret = xpcall(cjson.encode, traceback, val)
    if not ok then
        assert(false, strformat("encode error. val=[%s] ret=[%s]",tostring(val), tostring(ret)))
        return
    end
    return ret
end

function M.decode(val)
    if not val then return end
    local ok, ret = xpcall(cjson.decode, traceback, val)
    if not ok then
        assert(false, strformat("decode error. val=[%s] ret=[%s]", tostring(val), tostring(ret)))
        return
    end
    return ret
end

local escape_map = {
    ['\0'] = "\\0",
    ['\b'] = "\\b",
    ['\n'] = "\\n",
    ['\r'] = "\\r",
    ['\t'] = "\\t",
    ['\26'] = "\\Z",
    ['\\'] = "\\\\",
    ["'"] = "\\'",
    ['"'] = '\\"',
}

function M.quote_sql_str( str)
    return strformat("'%s'", strgsub(str, "[\0\b\n\r\t\26\\\'\"]", escape_map))
end

function M.redis_key_field(conf, id)
    if not conf.redisd_addr then return end
    local key, field
    if id then
        key = conf.tbl .. ":" .. conf.id
        field = tostring(id)
    else
        key = conf.tbl
        field = conf.id
    end
    return key, field
end

function M.mysql_execute(addr, sqls)
    if type(sqls) == "table" then
        return skynet.call(addr, "lua", "exec", sqls)
    elseif type(sqls) == "string" then
        return skynet.call(addr, "lua", "exec_one", sqls)
    end
end

function M.redis_execute(addr, cmd, ...)
    if type(cmd) == "table" then
        return skynet.call(addr, "lua", "exec", cmd)
    elseif type(cmd) == "string" then
        return skynet.call(addr, "lua", "exec_one", cmd, ...)
    end
end

return M