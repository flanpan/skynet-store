local skynet = require "skynet"
local time = skynet.time
local floor = math.floor
local ceil = math.ceil

--[[
sign | delta seconds | worker id | sequence
1bit | 30bits        | 20bits    | 13bits

sign            - 0为正数 1为负数
delta seconds   - 2020/01/01到当前时间时间差, 30bits可以用到2054-01-09
worker id       - 0~1048575 每个服务worker id都应该不一样,才能保证唯一性, worker_id需使用者自行规划
sequence        - 0~8191 当前时刻的id序列
]]

local DELTA_SECS_BITS = 30
local WORKER_ID_BITS = 20
local SEQ_BITS = 13
local TIME_LEFT_SHIFT_BITS = WORKER_ID_BITS + SEQ_BITS

local START_TIME = 1577808000 -- 2020/01/01
local MAX_WORKER_ID = (1<<WORKER_ID_BITS) - 1
local MAX_SEQ = (1<<SEQ_BITS) - 1

local worker_id = tonumber(...)
assert(worker_id >= 0 and worker_id <= MAX_WORKER_ID)

local seq = 0
local last_delta_time = 0
local gen_guid

gen_guid = function()
    local now = time()
    local floor_now = floor(now)
    local delta_time = floor_now - START_TIME
    if last_delta_time ~= delta_time then
        last_delta_time = delta_time
        seq = 0
    elseif seq < MAX_SEQ then
        seq = seq + 1
    else
        skynet.sleep(100 - ceil(now - floor_now)*100)
        return gen_guid()
    end
    return (delta_time<<TIME_LEFT_SHIFT_BITS) + (worker_id<<SEQ_BITS) + seq 
end

skynet.start(function()
    skynet.dispatch("lua", function()
        skynet.retpack(gen_guid())
    end)
end)