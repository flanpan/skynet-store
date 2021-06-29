local skynet = require "skynet"
local entity_pool = requier "entity_pool"
local _store = {}

local inited = false

local function add_pool(name, opts)
    if _store[name] then
        error(("entity pool [%d] has exist"):format(name))
    end
    _store[name] = entity_pool.new(opts)
end

local function open(flush_interval)
    if flush_interval then
        skynet.fork(function()
            while inited do
                for _, pool in pairs(_store) do
                    pool:flush()
                    skynet.yield()
                end
                skynet.sleep(flush_interval*100)
            end
        end)
    end
end

local function close()
    for _, pool in pairs(_store) do
        pool:unload_all()
    end
    _store = {}
    inited = false
end

local store = setmetatable(_store, {__index = function(self, name)
    error(("can not find entity pool [%s]"):format(name))
end})

return store, add_pool, open, close
