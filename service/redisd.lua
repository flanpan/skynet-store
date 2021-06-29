local skynet = require "skynet"
local redis = require "skynet.db.redis"
local util = require "store_util"
require "skynet.manager"

local traceback = debug.traceback
local tunpack = table.unpack
local tconcat = table.concat
local dbkey, index = ...
local db

local CMD = {}

function CMD.init(conf)
    db = redis.connect(conf)
    return true
end

function CMD.exec_one(cmd, ...)
    local ok, ret = xpcall(db[cmd], traceback, db, ...)
    if not ok then
        assert(false, ("cmd=[%s %s] ret=[%s]"):format(cmd, tconcat({...}, " "), ret))
        return
    end
    return ret
end

function CMD.exec(cmds)
    for _, cmd in pairs(cmds) do
        xpcall(CMD.exec_one, traceback, tunpack(cmd))
    end
end

skynet.start(function()
    skynet.dispatch("lua", function(_, _, cmd, ...)
        local f = CMD[cmd]
        assert(f, cmd)
        skynet.retpack(f(...)) 
    end)
end)

skynet.register(util.redis_sname(dbkey, index))
