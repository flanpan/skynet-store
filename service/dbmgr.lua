local skynet = require "skynet"
local queue = require "skynet.queue"
local util = require "store_util"
require "skynet.manager"

local dbconf
local lock = queue()
local guid_generator_addrs = {}

local CMD = {}

function CMD.init(conf)
    assert(not dbconf, "dbmgr has been initialized.")
    dbconf = conf -- init is allowed only once

    local uidconf = dbconf.guid_generator
    assert(uidconf)
    for _, worker_id in pairs(uidconf.worker_ids) do
        local addr = skynet.newservice("guid_generator", worker_id)
        table.insert(guid_generator_addrs, addr)
    end

    local redisconf = dbconf.redis
    for dbkey, conf in pairs(redisconf) do
        for index = 1, conf.service_num do
            local addr = skynet.newservice("redisd", dbkey, index)
            local ok = skynet.call(addr, "lua", "init", conf)
            if not ok then
                assert(false, ("redisd init failed. [dbkey] %s [id] %d"):format(dbkey, index))
            end
        end
    end

    local mysqlconf = dbconf.mysql
    for dbkey, conf in pairs(mysqlconf) do
        for index = 1, conf.service_num do
            local addr = skynet.newservice("mysqld", dbkey, index)
            local ok = skynet.call(addr, "lua", "init", conf)
            if not ok then
                assert(false, ("mysqld init failed. [dbkey] %s [id] %d"):format(dbkey, index))
            end
        end
    end

    return true
end

function CMD.mysql_service_num(dbkey)
    if not dbconf then return end
    local mysqlconf = dbconf.mysql
    if not mysqlconf then return end
    local conf = mysqlconf[dbkey]
    if not conf then return end
    return conf.service_num
end

function CMD.redis_service_num(dbkey)
    if not dbconf then return end
    local redisconf = dbconf.redis
    if not redisconf then return end
    local conf = redisconf[dbkey]
    if not conf then return end
    return conf.service_num
end

function CMD.guid_generators()
    return guid_generator_addrs
end

skynet.start(function()
    skynet.dispatch("lua", function(_, _, cmd, ...)
        local f = CMD[cmd]
        assert(f, cmd)
        skynet.retpack(f(...)) 
    end)
end)

skynet.register(".dbmgr")