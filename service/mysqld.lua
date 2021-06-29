local skynet = require "skynet"
local mysql = require "skynet.db.mysql"
local util = require "store_util"
require "skynet.manager"

local traceback = debug.traceback

local dbkey, index = ...
local db

local CMD = {}

local function success(ret)
    if not ret or ret.err or ret.badresult then
        return false
    end
    return true
end

function CMD.init(conf)
    db = mysql.connect(conf)
    db:query("set names utf8mb4")
    return true
end

function CMD.exec_one(sql)
    local ok, ret = xpcall(db.query, traceback, db, sql)
    if not ok or not success(ret) then
        assert(false, ("sql=[%s] ret=[%s]"):format(sql, util.encode(ret)))
        return
    end
    return ret
end

function CMD.exec(sqls)
    for i = 1, #sqls do
        local sql = sqls[i]
        CMD.exec_one(sql)
    end
end

skynet.start(function()
    skynet.dispatch("lua", function(_, _, cmd, ...)
        local f = CMD[cmd]
        assert(f, cmd)
        skynet.retpack(f(...)) 
    end)
end)

skynet.register(util.mysql_sname(dbkey, index))
