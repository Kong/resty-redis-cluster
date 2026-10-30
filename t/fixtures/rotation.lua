-- Create, rotate and remove an ACL user on the test cluster.
local resty_redis = require "resty.redis"

local M = {}

-- The fixture starts three masters and three replicas (ports 6371 to 6376).
local ALL_PORTS = { 6371, 6372, 6373, 6374, 6375, 6376 }

local function on_each(fn, ports)
    ports = ports or ALL_PORTS
    for i = 1, #ports do
        local red = assert(resty_redis:new())
        assert(red:connect("127.0.0.1", ports[i]))
        assert(red:auth("kong"))
        fn(red, ports[i])
        red:set_keepalive()
    end
end

function M.create(user, password, ports)
    on_each(function(red)
        assert(red:acl("setuser", user, "on", ">" .. password, "~*", "+@all"))
    end, ports)
end

function M.rotate(user, password, ports)
    on_each(function(red)
        assert(red:acl("setuser", user, "resetpass", ">" .. password))
    end, ports)
end

function M.remove(user, ports)
    on_each(function(red)
        red:acl("deluser", user)
    end, ports)
end

function M.ports()
    return ALL_PORTS
end

return M
