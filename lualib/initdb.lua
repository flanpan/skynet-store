local skynet = require "skynet"

return function(dbconf)
    local addr = skynet.uniqueservice("dbmgr")
    return skynet.call(addr, "lua", "init", dbconf)
end