-- A scriptable stand-in for `resty.redis`. It mimics the real client in the ways that
-- matter here: an error reply returns `false, message`, a socket error returns
-- `nil, message`, a queued reply keeps the error, and unknown fields return a command
-- function.
local M = {
    events = {},
    nodes = {},
    topology = 1,
}

local function record(ev)
    M.events[#M.events + 1] = ev
end

function M.reset()
    M.events = {}
    M.nodes = {}
    M.topology = 1
    M.handler = nil
    M.next_id = 0
end

M.reset()

local function slots_v1()
    return {
        { 0, 5460, { "127.0.0.1", 7001, "id1" } },
        { 5461, 10922, { "127.0.0.1", 7002, "id2" } },
        { 10923, 16383, { "127.0.0.1", 7003, "id3" } },
    }
end

-- Version 2: node 7001 owns everything up to 10922, so it is a stale owner for "foo" (12182).
local function slots_v2()
    return {
        { 0, 10922, { "127.0.0.1", 7001, "id1" } },
        { 10923, 16383, { "127.0.0.1", 7003, "id3" } },
    }
end

-- Version 3: node 7001 owns every slot. A client that uses it for "foo" gets MOVED.
local function slots_v3()
    return {
        { 0, 16383, { "127.0.0.1", 7001, "id1" } },
    }
end

-- Version 4: Redis 7 that announces hostnames. The IP of the same node follows it
-- in the fourth element, the same way `CLUSTER SLOTS` reports it.
local function slots_v4()
    return {
        { 0, 5460,      { "rc-node-1", 7001, "id1", { "ip", "127.0.0.1" } } },
        { 5461, 10922,  { "rc-node-2", 7002, "id2", { "ip", "127.0.0.2" } } },
        { 10923, 16383, { "rc-node-3", 7003, "id3", { "ip", "127.0.0.3" } } },
    }
end

-- Version 5: a pure IP cluster where every node uses the same port, so the entries
-- of the flat serv_list are nodes of their own and not an IP/hostname pair.
local function slots_v5()
    return {
        { 0, 5460,      { "10.0.0.1", 6379, "id1" } },
        { 5461, 10922,  { "10.0.0.2", 6379, "id2" } },
        { 10923, 16383, { "10.0.0.3", 6379, "id3" } },
    }
end

local topologies = { slots_v1, slots_v2, slots_v3, slots_v4, slots_v5 }

local NODES = table.concat({
    "id1 127.0.0.1:7001@17001 master - 0 0 1 connected 0-5460",
    "id2 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922",
    "id3 127.0.0.1:7003@17003 master - 0 0 3 connected 10923-16383",
}, "\n") .. "\n"

local methods = {}

function methods.set_timeouts() end

function methods.connect(self, host, port, opts)
    self._node = host .. ":" .. tostring(port)
    local copy = {}
    for k, v in pairs(opts or {}) do copy[k] = v end
    record({ ev = "connect", id = self._id, node = self._node, pool = copy.pool, opts = opts })
    if M.connect then
        return M.connect(self, host, port, opts)
    end
    return 1
end

function methods.get_reused_times(self)
    return rawget(self, "_reused") or 0
end

function methods.set_keepalive(self)
    record({ ev = "keepalive", id = self._id, node = self._node })
    if rawget(self, "_closed") then
        return nil, "closed"
    end
    return 1
end

function methods.close(self)
    record({ ev = "close", id = self._id, node = self._node })
    self._closed = true
    return 1
end

function methods.cluster(self, sub)
    if sub == "info" then
        return "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
    elseif sub == "nodes" then
        return NODES
    elseif sub == "slots" then
        record({ ev = "cluster-slots", id = self._id, node = self._node, topology = M.topology })
        return topologies[M.topology]()
    end
end

function methods.info(self)
    return "redis_version:6.2.14\r\nredis_mode:cluster\r\n"
end

local function dispatch(self, cmd, ...)
    local args = { ... }
    record({ ev = "cmd", id = self._id, node = self._node, cmd = cmd, args = args })
    local res, err = "OK", nil
    if M.handler then
        res, err = M.handler(self, cmd, unpack(args))
    end
    if rawget(self, "_pipe") then
        -- Like the real client, a queued command returns nothing; the replies come at commit.
        local q = rawget(self, "_pipe")
        if res == nil and err ~= nil then
            -- A socket error stops the pipeline. commit_pipeline reports it.
            q.failed = err
            return
        end
        q[#q + 1] = res == false and { false, err } or res
        return
    end
    return res, err
end

function methods.init_pipeline(self)
    self._pipe = {}
end

function methods.commit_pipeline(self)
    local q = rawget(self, "_pipe")
    self._pipe = nil
    record({ ev = "commit", id = self._id, node = self._node, n = #q })
    if q.failed then
        return nil, q.failed
    end
    return q
end

local mt = {}
mt.__index = function(self, key)
    if methods[key] then
        return methods[key]
    end
    -- The real library returns a command function for any unknown name.
    return function(s, ...) return dispatch(s, key, ...) end
end

function M.new()
    M.next_id = M.next_id + 1
    return setmetatable({ _id = M.next_id }, mt)
end

function M.dump(filter)
    for _, e in ipairs(M.events) do
        if not filter or filter(e) then
            local parts = {}
            for _, k in ipairs({ "ev", "id", "node", "cmd", "pool", "topology", "n" }) do
                if e[k] ~= nil then parts[#parts + 1] = k .. "=" .. tostring(e[k]) end
            end
            if e.args then
                local a = {}
                for i, v in ipairs(e.args) do a[i] = tostring(v) end
                parts[#parts + 1] = "args=" .. table.concat(a, ",")
            end
            print("    " .. table.concat(parts, " "))
        end
    end
end

return M
