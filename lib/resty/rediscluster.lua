local redis = require "resty.redis"
local resty_lock = require "resty.lock"
local xmodem = require "resty.xmodem"
local setmetatable = setmetatable
local tostring = tostring
local string = string
local type = type
local table = table
local ngx = ngx
local math = math
local rawget = rawget
local pairs = pairs
local unpack = unpack
local ipairs = ipairs
local tonumber = tonumber
local match = string.match
local char = string.char
local table_insert = table.insert
local table_concat = table.concat
local string_find = string.find
local redis_crc = xmodem.redis_crc

local ngx_log = ngx.log
local ngx_err = ngx.ERR
local ngx_notice = ngx.NOTICE
local ngx_info = ngx.INFO
local ngx_debug = ngx.DEBUG


local DEFAULT_SHARED_DICT_NAME = "redis_cluster_slot_locks"
local DEFAULT_REFRESH_DICT_NAME = "refresh_lock"
local DEFAULT_MAX_REDIRECTION = 5
local DEFAULT_MAX_CONNECTION_ATTEMPTS = 3
local DEFAULT_KEEPALIVE_TIMEOUT = 55000
local DEFAULT_KEEPALIVE_CONS = 1000
local DEFAULT_CONNECTION_TIMEOUT = 1000
local DEFAULT_SEND_TIMEOUT = 1000
local DEFAULT_READ_TIMEOUT = 1000

local LEFT_BRACKET = "{"
local RIGHT_BRACKET = "}"

local _M = {}

local mt = { __index = _M }


-- According to the Redis documentation, the hash tag is found only if the '{' and '}'
-- are exist in the key, or it should use the whole key to calculate the slot.
-- See: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags
--
-- For the Redis hash slot implemetation, see: https://github.com/redis/redis/blob/8a05f0092b0e291498b8fdb8dd93355467ceab25/src/cluster.c#L30-L49
local function parse_key(key_str)
    local left_index = string_find(key_str, LEFT_BRACKET, 1, true)
    if not left_index then
        return key_str
    end

    local right_index = string_find(key_str, RIGHT_BRACKET, left_index + 1, true)
    if right_index and right_index > left_index + 1 then
        return key_str:sub(left_index + 1, right_index - 1)
    end
    return key_str
end
-- export for testing
_M.parse_key = parse_key

-- Redis 7.x and Redis 6.x use different slots format
local redis7_with_host

local slot_cache = {}
local master_nodes = {}

local cmds_for_all_master = {
    ["flushall"] = true,
    ["flushdb"] = true
}

local cluster_invalid_cmds = {
    ["config"] = true,
    ["shutdown"] = true
}


local function redis_slot(str)
    return redis_crc(parse_key(str))
end


local function check_auth(self, redis_client)
    -- redis before 6.0
    if type(self.config.auth) == "string" then
        local count, err = redis_client:get_reused_times()
        if not count then
            return nil, "failed getting reused count: " .. tostring(err)
        elseif count > 0 then
            return true, nil -- reusing the connection, so already authenticated
        end
        return redis_client:auth(self.config.auth)

    -- redis 6.x adds support for username+password combination
    elseif type(self.config.password) == "string" then
        local count, err = redis_client:get_reused_times()
        if not count then
            return nil, "failed getting reused count: " .. tostring(err)
        elseif count > 0 then
            return true, nil -- reusing the connection, so already authenticated
        end

        if type(self.config.username) == "string" then
            return redis_client:auth(self.config.username, self.config.password)
        else
            return redis_client:auth(self.config.password)
        end

    else
        return true, nil
    end
end


local function release_connection(red, config)
    local ok,err = red:set_keepalive(config.keepalive_timeout
            or DEFAULT_KEEPALIVE_TIMEOUT, config.keepalive_cons or DEFAULT_KEEPALIVE_CONS)
    if not ok then
        ngx_log(ngx_err,"set keepalive failed:", err)
    end
end


local function split(s, delimiter)
    local result = {};
    for m in (s..delimiter):gmatch("(.-)"..delimiter) do
        table_insert(result, m);
    end
    return result;
end


local function try_hosts_slots(self, serv_list)
    local start_time = ngx.now()
    local errors = {}
    local config = self.config
    if #serv_list < 1 then
        return nil, "failed to fetch slots, serv_list config is empty"
    end

    for i = 1, #serv_list do
        local ip = serv_list[i].ip
        local port = serv_list[i].port
        local redis_client = redis:new()
        local ok, err, max_connection_timeout_err
        redis_client:set_timeouts(config.connect_timeout or DEFAULT_CONNECTION_TIMEOUT,
                                  config.send_timeout or DEFAULT_SEND_TIMEOUT,
                                  config.read_timeout or DEFAULT_READ_TIMEOUT)

        -- attempt to connect DEFAULT_MAX_CONNECTION_ATTEMPTS times to redis
        -- Kong currently does not configure 'max_connection_attempts'
        for k = 1, config.max_connection_attempts or DEFAULT_MAX_CONNECTION_ATTEMPTS do
            -- for each ip:port pair, we try at least once before check total timeout
            ok, err = redis_client:connect(ip, port, self.config.connect_opts)
            if ok then break end
            if err then
                ngx_log(ngx_err, "unable to connect, attempt nr ", k, " : error: ", err)
                table_insert(errors, err)
            end

            -- Kong currently does not configure 'max_connection_timeout'
            local total_connection_time_ms = (ngx.now() - start_time) * 1000
            if (config.max_connection_timeout and total_connection_time_ms > config.max_connection_timeout) then
                max_connection_timeout_err = "max_connection_timeout of " .. config.max_connection_timeout .. "ms reached."
                ngx_log(ngx_err, max_connection_timeout_err)
                table_insert(errors, max_connection_timeout_err)
                break
            end
        end

        if ok then
            local _, autherr = check_auth(self, redis_client)
            if autherr then
                redis_client:close()
                table_insert(errors, autherr)
                return nil, errors
            end

            -- cache cluster slots
            local slots_info
            slots_info, err = redis_client:cluster("slots")
            if slots_info then
                local slots = {}
                -- while slots are updated, create a list of servers present in cluster
                -- this can differ from self.config.serv_list if a cluster is resized (added/removed nodes)
                local servers = { serv_list = {} }
                for n = 1, #slots_info do
                    -- Redis 6: { 0, 5460, { "172.26.0.21", 6379, "b050a28d860201c487891ecde77efc8a3935ea51" }, { "172.26.0.25", 6379, "20d96bc4a640fd244699e29274b2c94962def80e" } }
                    -- Redis 7: { 0, 5460, { "rc-node-1", 6379, "3a4f2814802ca25a5c92dfc167674802126557d3", { "ip", "172.26.0.11" } }, { "rc-node-5", 6379, "f14b2cf710613f517374aefe7b84d9f013102ebe", { "ip", "172.26.0.15" } } }
                    -- Redis 7: { 0, 5460, { "172.26.0.11", 6379, "64ff3bc4e8f4587c92bcfaa6c085e67417d44d5d", {} }, { "172.26.0.15", 6379, "9a63c473b928203b141d20a46be6784eb320ba0c", {} } }
                    local sub_info = slots_info[n]
                    -- slot info item 1 and 2 are the subrange start end slots
                    local start_slot, end_slot = sub_info[1], sub_info[2]

                    -- generate new list of servers, including IPs and hostnames
                    local sub_info_j
                    local sub_info_j_ip
                    for j = 3, #sub_info do
                        -- Redis 6: { "172.26.0.21", 6379, "b050a28d860201c487891ecde77efc8a3935ea51" }
                        -- Redis 7: { "rc-node-1", 6379, "3a4f2814802ca25a5c92dfc167674802126557d3", { "ip", "172.26.0.11" } }
                        -- Redis 7: { "172.26.0.11", 6379, "64ff3bc4e8f4587c92bcfaa6c085e67417d44d5d", {} }
                        sub_info_j = sub_info[j]
                        -- Redis 6: nil
                        -- Redis 7: { "ip", "172.26.0.11" }
                        -- Redis 7: {}
                        sub_info_j_ip = sub_info_j[4]
                        if sub_info_j_ip and type(sub_info_j_ip) == "table" and
                           next(sub_info_j_ip) and sub_info_j_ip[2] ~= sub_info_j[1]
                        then
                            redis7_with_host = true
                            -- prioritize IP address over hostname as IP does not suffer from stale DNS cache
                            servers.serv_list[#servers.serv_list + 1] = { ip = sub_info_j_ip[2], port = sub_info_j[2] }
                        else
                            redis7_with_host = false
                        end
                        servers.serv_list[#servers.serv_list + 1] = { ip = sub_info_j[1], port = sub_info_j[2] }
                    end

                    for slot = start_slot, end_slot do
                        -- todo: there is no need to create the 'list', as all the slots
                        -- within [start_slot, end_slot] share the same 'servers'.
                        -- just do: slots[slot] = servers
                        local list = { serv_list = {} }
                        -- from 3, here lists the host/port/nodeid of in charge nodes
                        for j = 3, #sub_info do
                            sub_info_j = sub_info[j]
                            sub_info_j_ip = sub_info_j[4]
                            if sub_info_j_ip and type(sub_info_j_ip) == "table" and
                               next(sub_info_j_ip) and sub_info_j_ip[2] ~= sub_info_j[1]
                            then
                                -- prioritize IP address over hostname as IP does not suffer from stale DNS cache
                                list.serv_list[#list.serv_list + 1] = { ip = sub_info_j_ip[2], port = sub_info_j[2] }
                            end
                            list.serv_list[#list.serv_list + 1] = { ip = sub_info_j[1], port = sub_info_j[2] }

                            slots[slot] = list
                        end
                    end
                end
                ngx_log(ngx_debug, "finished initializing slotcache ...")
                slot_cache[self.config.name] = slots
                slot_cache[self.config.name .. "serv_list"] = servers
            else
                ngx_log(ngx_debug, "failed to initialize slotcache")
                redis_client:close()
                table_insert(errors, err)
                return nil, errors
            end

            -- cache master nodes
            local nodes_res, nerr = redis_client:cluster("nodes")
            if nodes_res then
                local nodes_info = split(nodes_res, char(10))
                for _, node in ipairs(nodes_info) do
                    local node_info = split(node, " ")
                    if #node_info > 2 then
                        local is_master = match(node_info[3], "master") ~= nil
                        if is_master then
                            local ip_port = split(split(node_info[2], "@")[1], ":")
                            table_insert(master_nodes, {
                                ip = ip_port[1],
                                port = tonumber(ip_port[2])
                            })
                        end
                    end
                end
            else
                ngx_log(ngx_debug, "failed to fetch master nodes")
                redis_client:close()
                table_insert(errors, nerr)
                return nil, errors
            end

            -- only put back to connection pool when there are not errors
            release_connection(redis_client, config)

            -- refresh of slots and master nodes successful
            -- not required to connect/iterate over additional hosts
            if nodes_res and slots_info then
                return true, nil
            end
        elseif max_connection_timeout_err then
            break
        else
            table_insert(errors, err)
        end
        if #errors == 0 then
            return true, nil
        end
    end
    return nil, errors
end


function _M.fetch_slots(self)
    local serv_list = self.config.serv_list
    local serv_list_cached = slot_cache[self.config.name .. "serv_list"]

    local serv_list_combined

    -- if a cached serv_list is present, add it to combined list
    if serv_list_cached then
        serv_list_combined = {}

        for i, s in ipairs(serv_list_cached.serv_list) do
            table_insert(serv_list_combined, i, s)
        end

        -- prioritize serv_list from config over cached serv_list
        -- in the event that the entire config serv_list no longer
        -- points to anything usable, try the cached serv_list
        for i, s in ipairs(serv_list) do
            table_insert(serv_list_combined, i, s)
        end
    else
        -- otherwise we bootstrap with our serv_list from config
        serv_list_combined = serv_list
    end

    ngx_log(ngx_debug, "fetching slots from: ", #serv_list_combined, " servers")
    -- important!
    serv_list_cached = nil -- luacheck: ignore

    local _, errors = try_hosts_slots(self, serv_list_combined)
    if errors then
        local err = "failed to fetch slots: " .. table_concat(errors, ";")
        ngx_log(ngx_err, err)
        return nil, err
    end
end


function _M.refresh_slots(self)
    local worker_id = ngx.worker.id()
    local lock, err, elapsed, ok
    lock, err = resty_lock:new(self.config.dict_name or DEFAULT_SHARED_DICT_NAME, {time_out = 0})
    if not lock then
        ngx_log(ngx_err, "failed to create lock in refresh slot cache: ", err)
        return nil, err
    end

    local refresh_lock_key = (self.config.refresh_lock_key or DEFAULT_REFRESH_DICT_NAME) .. worker_id
    elapsed, err = lock:lock(refresh_lock_key)
    if not elapsed then
        return nil, 'race refresh lock fail, ' .. err
    end

    self:fetch_slots()
    ok, err = lock:unlock()
    if not ok then
        ngx_log(ngx_err, "failed to unlock in refresh slot cache:", err)
        return nil, err
    end
end


function _M.init_slots(self)
    if slot_cache[self.config.name] then
        -- already initialized
        return true
    end
    local ok, lock, elapsed, err
    lock, err = resty_lock:new(self.config.dict_name or DEFAULT_SHARED_DICT_NAME)
    if not lock then
        ngx_log(ngx_err, "failed to create lock in initialization slot cache: ", err)
        return nil, err
    end

    elapsed, err = lock:lock("redis_cluster_slot_" .. self.config.name)
    if not elapsed then
        ngx_log(ngx_err, "failed to acquire the lock in initialization slot cache: ", err)
        return nil, err
    end

    if slot_cache[self.config.name] then
        ok, err = lock:unlock()
        if not ok then
            ngx_log(ngx_err, "failed to unlock in initialization slot cache: ", err)
        end
        -- already initialized
        return true
    end

    local _, errs = self:fetch_slots()
    if errs then
        ok, err = lock:unlock()
        if not ok then
            ngx_log(ngx_err, "failed to unlock in initialization slot cache:", err)
        end
        return nil, errs
    end
    ok, err = lock:unlock()
    if not ok then
        ngx_log(ngx_err, "failed to unlock in initialization slot cache:", err)
    end
    -- initialized
    return true
end


function _M.new(_, config)
    if not config.name then
        return nil, " redis cluster config name is empty"
    end
    if not config.serv_list or #config.serv_list < 1 then
        return nil, " redis cluster config serv_list is empty"
    end

    local inst = { config = config }
    inst = setmetatable(inst, mt)
    local _, err = inst:init_slots()
    if err then
        return nil, err
    end
    return inst
end


local function pick_node(self, serv_list, magic_radom_seed, ip_or_host)
    local host
    local port
    local slave
    local index

    if #serv_list < 1 then
        return nil, nil, nil, "serv_list is empty"
    end

    if magic_radom_seed and type(magic_radom_seed) ~= "number" then
        return nil, nil, nil, "magic_radom_seed must be a number"
    end

    if ip_or_host then
        if type(ip_or_host) ~= "number" then
            return nil, nil, nil, "ip_or_host must be a number"
        end

        -- 1: pick IP first
        -- 2: then pick hostname
        ip_or_host = ip_or_host % 2
        if ip_or_host == 0 then
            ip_or_host = 2
        end
    end

    -- slave nodes can be picked; however
    -- Kong currently does not configure this option
    if self.config.enable_slave_read then
        if magic_radom_seed then
            index = magic_radom_seed % #serv_list
            if index == 0 then
                index = #serv_list
            end
        else
            index = math.random(#serv_list)
        end

        -- cluster slots will always put the master node before slave nodes
        -- serv_list: { { master_ip, master_port }, { master_host, master_port },
        --              { slave_ip, slave_port }, { slave_host, slave_port },
        --              { slave_ip, slave_port }, { slave_host, slave_port } }
        if redis7_with_host then
            -- pick IP
            if ip_or_host == 1 and index % 2 == 0 then
                index = index - 1
            end
            -- pick hostname
            if ip_or_host == 2 and index % 2 == 1 then
                index = index + 1
            end

            slave = index > 2

        else
            slave = index > 1
        end

    -- only pick the master node (IP or hostname)
    else
        if redis7_with_host then
            index = ip_or_host
        else
            index = 1
        end

        slave = false
    end

    host = serv_list[index].ip
    port = serv_list[index].port

    ngx_log(ngx_info, "index: " .. index .. ", pick node: ", host, ":", tostring(port))

    return host, port, slave
end


local ask_host_and_port = {}

local function parse_ask_signal(res)
    --ask signal sample:ASK 12191 127.0.0.1:7008, so we need to parse and get 127.0.0.1, 7008
    if res ~= ngx.null then
        if type(res) == "string" and string.sub(res, 1, 3) == "ASK" then
            local matched = ngx.re.match(res, [[^ASK [^ ]+ ([^:]+):([^ ]+)]], "jo", nil, ask_host_and_port)
            if not matched then
                return nil, nil
            end
            return matched[1], matched[2]
        end
        if type(res) == "table" then
            for i = 1, #res do
                if type(res[i]) == "string" and string.sub(res[i], 1, 3) == "ASK" then
                    local matched = ngx.re.match(res[i], [[^ASK [^ ]+ ([^:]+):([^ ]+)]], "jo", nil, ask_host_and_port)
                    if not matched then
                        return nil, nil
                    end
                    return matched[1], matched[2]
                end
            end
        end
    end
    return nil, nil
end


local function has_moved_signal(res)
    if res ~= ngx.null then
        if type(res) == "string" and string.sub(res, 1, 5) == "MOVED" then
            return true
        else
            if type(res) == "table" then
                for i = 1, #res do
                    if type(res[i]) == "string" and string.sub(res[i], 1, 5) == "MOVED" then
                        return true
                    end
                end
            end
        end
    end
    return false
end


local function generate_magic_seed(self)
    --For pipeline, We don't want request to be forwarded to all channels, eg. if we have 3*3 cluster(3 master 2 replicas) we
    --alway want pick up specific 3 nodes for pipeline requests, instead of 9.
    --Currently we simply use (num of allnode)%count as a randomly fetch. Might consider a better way in the future.
    -- use the dynamic serv_list instead of the static config serv_list
    local nodeCount = #slot_cache[self.config.name .. "serv_list"].serv_list
    return math.random(nodeCount)
end


local function handle_command_with_retry(self, target_ip, target_port, asking, cmd, key, ...)
    local config = self.config

    -- key will be passed to resty.redis
    local slot = redis_slot(tostring(key))

    local magicRandomPickupSeed = generate_magic_seed(self)
    local loop_counter = config.max_redirection or DEFAULT_MAX_REDIRECTION

    for k = 1, loop_counter do
        if k > 1 then
            ngx_log(ngx_notice, "handle retry attempts:" .. k .. " for cmd:" .. cmd .. " key:" .. tostring(key))
        end

        local slots = slot_cache[self.config.name]
        if slots == nil or slots[slot] == nil then
            self:refresh_slots()
            return nil, "not slots information present, nginx might have never successfully executed cluster(\"slots\")"
        end
        local serv_list = slots[slot].serv_list

        -- We must empty local reference to slots cache, otherwise there will be memory issue while
        -- coroutine swich happens(eg. ngx.sleep, cosocket), very important!
        slots = nil -- luacheck: ignore

        local ip, port, slave, err
        local ok, connerr

        local redis_client = redis:new()
        redis_client:set_timeouts(config.connect_timeout or DEFAULT_CONNECTION_TIMEOUT,
                                  config.send_timeout or DEFAULT_SEND_TIMEOUT,
                                  config.read_timeout or DEFAULT_READ_TIMEOUT)

        if target_ip ~= nil and target_port ~= nil then
            -- asking redirection should only happens at master nodes
            ip, port, slave = target_ip, target_port, false

            ok, connerr = redis_client:connect(ip, port, self.config.connect_opts)
            if ok then break end
        else
            -- Redis 7: try IP first and then hostname as IP does not suffer from stale DNS cache
            local tries = redis7_with_host and 2 or 1
            for i = 1, tries do
                ip, port, slave, err = pick_node(self, serv_list, magicRandomPickupSeed, i)
                if err then
                    ngx_log(ngx_err, "pickup node failed, will return failed for this request, meanwhile refereshing slotcache " .. err)
                    self:refresh_slots()
                    return nil, err
                end

                ok, connerr = redis_client:connect(ip, port, self.config.connect_opts)
                if ok then break end
            end
        end

        if ok then
            local _, autherr = check_auth(self, redis_client)
            if autherr then
                redis_client:close()
                return nil, autherr
            end
            if slave then
                --set readonly
                ok, err = redis_client:readonly()
                if not ok then
                    redis_client:close()
                    self:refresh_slots()
                    return nil, err
                end
            end

            if asking then
                --executing asking
                ok, err = redis_client:asking()
                if not ok then
                    redis_client:close()
                    self:refresh_slots()
                    return nil, err
                end
            end

            local need_to_retry = false
            local res
            if cmd == "eval" or cmd == "evalsha" then
                res, err = redis_client[cmd](redis_client, ...)
            else
                res, err = redis_client[cmd](redis_client, key, ...)
            end

            if err then
                -- todo: we can follow the moved target instead of refreshing the slots
                if has_moved_signal(res) then
                    ngx_log(ngx_debug, "find MOVED signal, trigger retry for normal commands, cmd:" .. cmd .. " key:" .. tostring(key))
                    -- if retry with moved, we will not asking to specific ip:port anymore
                    target_ip = nil
                    target_port = nil
                    local _, _, moved_ip, moved_port = string.find(err, "MOVED %d+ ([%w%.%-_]+):(%d+)")
                    if moved_ip == tostring(ip) and moved_port == tostring(port) then
                        ngx_log(ngx_debug, "nested moved redirection detected, refreshing slots ...")
                    end
                    redis_client:close()
                    self:refresh_slots()
                    need_to_retry = true

                elseif string.sub(err, 1, 3) == "ASK" then
                    ngx_log(ngx_debug, "handle asking for normal commands, cmd:" .. cmd .. " key:" .. tostring(key))
                    if asking then
                        -- Should not happen after asking target ip:port and still return ask, if so, return error.
                        redis_client:close()
                        self:refresh_slots()
                        return nil, "nested asking redirection occurred, client cannot retry "
                    else
                        local ask_host, ask_port = parse_ask_signal(err)

                        if ask_host ~= nil and ask_port ~= nil then
                            -- only put back to connection pool when there are no errors
                            release_connection(redis_client, config)
                            return handle_command_with_retry(self, ask_host, ask_port, true, cmd, key, ...)
                        else
                            redis_client:close()
                            self:refresh_slots()
                            return nil, " cannot parse ask redirection host and port: msg is " .. err
                        end
                    end

                elseif string.sub(err, 1, 11) == "CLUSTERDOWN" then
                    redis_client:close()
                    self:refresh_slots()
                    return nil, "Cannot executing command, cluster status is failed!"
                else
                    --There might be node fail, we should also refresh slot cache
                    redis_client:close()
                    self:refresh_slots()
                    return nil, err
                end
            end
            if not need_to_retry then
                -- only put back to connection pool when there are no errors
                release_connection(redis_client, config)
                return res, err
            end
        else
            --There might be node fail, we should also refresh slot cache
            self:refresh_slots()
            if k == loop_counter then
                -- only return after allowing for `loop_counter` attempts
                return nil, "tried " .. loop_counter .. " times: " .. connerr
            end
        end
    end
    return nil, "failed to execute command, reaches maximum redirection attempts"
end


local function _do_cmd_master(self, cmd, key, ...)
    local errors = {}
    for _, master in ipairs(master_nodes) do
        local redis_client = redis:new()
        redis_client:set_timeouts(self.config.connect_timeout or DEFAULT_CONNECTION_TIMEOUT,
                                  self.config.send_timeout or DEFAULT_SEND_TIMEOUT,
                                  self.config.read_timeout or DEFAULT_READ_TIMEOUT)
        local ok, err = redis_client:connect(master.ip, master.port, self.config.connect_opts)
        if ok then
            local _, autherr = check_auth(self, redis_client)
            if autherr then
                redis_client:close()
                return nil, autherr
            end

            _, err = redis_client[cmd](redis_client, key, ...)
            release_connection(redis_client, self.config)
        end
        if err then
            table_insert(errors, err)
        end
    end
    return #errors == 0, table_concat(errors, ";")
end

local function _do_cmd(self, cmd, key, ...)
    if cluster_invalid_cmds[cmd] == true then
        return nil, "command not supported"
    end

    local _reqs = rawget(self, "_reqs")
    if _reqs then
        local args = { ... }
        local t = { cmd = cmd, key = key, args = args }
        table_insert(_reqs, t)
        return
    end

    if cmds_for_all_master[cmd] then
        return _do_cmd_master(self, cmd, key, ...)
    end

    local res, err = handle_command_with_retry(self, nil, nil, false, cmd, key, ...)
    return res, err
end


local function construct_final_pipeline_resp(self, node_res_map, node_req_map)
    --construct final result with origin index
    local finalret = {}
    for k, v in pairs(node_res_map) do
        local reqs = node_req_map[k].reqs
        local res = v
        local need_to_fetch_slots = true
        for i = 1, #reqs do
            --deal with redis cluster ask redirection
            local ask_host, ask_port = parse_ask_signal(res[i])
            if ask_host ~= nil and ask_port ~= nil then
                ngx_log(ngx_debug, "handle ask signal for cmd:" .. reqs[i]["cmd"] .. " key:" .. reqs[i]["key"] .. " target host:" .. ask_host .. " target port:" .. ask_port)
                local askres, err = handle_command_with_retry(self, ask_host, ask_port, true, reqs[i]["cmd"], reqs[i]["key"], unpack(reqs[i]["args"]))
                if err then
                    return nil, err
                else
                    finalret[reqs[i].origin_index] = askres
                end
            elseif has_moved_signal(res[i]) then
                ngx_log(ngx_debug, "handle moved signal for cmd:" .. reqs[i]["cmd"] .. " key:" .. reqs[i]["key"])
                if need_to_fetch_slots then
                    -- if there is multiple signal for moved, we just need to fetch slot cache once, and do retry.
                    self:refresh_slots()
                    need_to_fetch_slots = false
                end
                local movedres, err = handle_command_with_retry(self, nil, nil, false, reqs[i]["cmd"], reqs[i]["key"], unpack(reqs[i]["args"]))
                if err then
                    return nil, err
                else
                    finalret[reqs[i].origin_index] = movedres
                end
            else
                finalret[reqs[i].origin_index] = res[i]
            end
        end
    end
    return finalret
end


local function has_cluster_fail_signal_in_pipeline(res)
    for i = 1, #res do
        if res[i] ~= ngx.null and type(res[i]) == "table" then
            for j = 1, #res[i] do
                if type(res[i][j]) == "string" and string.sub(res[i][j], 1, 11) == "CLUSTERDOWN" then
                    return true
                end
            end
        end
    end
    return false
end


function _M.init_pipeline(self)
    self._reqs = {}
end


function _M.commit_pipeline(self)
    local _reqs = rawget(self, "_reqs")

    if not _reqs or #_reqs == 0 then return nil, "no pipeline"
    end

    self._reqs = nil
    local config = self.config

    local slots = slot_cache[config.name]
    if slots == nil then
        self:refresh_slots()
        return nil, "not slots information present, nginx might have never successfully executed cluster(\"slots\")"
    end

    local node_res_map = {}
    local node_req_map = {}

    --construct req to real node mapping
    for i = 1, #_reqs do
        -- Because we will forward req to different nodes, so the result will not be the origin order,
        -- we need to record the original index and finally we can construct the result with origin order
        _reqs[i].origin_index = i
        local key = _reqs[i].key
        local slot = redis_slot(tostring(key))
        if slots[slot] == nil then
            self:refresh_slots()
            return nil, "not slots information present, nginx might have never successfully executed cluster(\"slots\")"
        end
        local slot_item = slots[slot]

        local node = slot_item.serv_list[1].ip .. tostring(slot_item.serv_list[1].port)
        if not node_req_map[node] then
            node_req_map[node] = { serv_list = slot_item.serv_list, reqs = {} }
            node_res_map[node] = {}
        end
        local ins_req = node_req_map[node].reqs
        ins_req[#ins_req + 1] = _reqs[i]
    end

    -- We must empty local reference to slots cache, otherwise there will be memory issue while
    -- coroutine swich happens(eg. ngx.sleep, cosocket), very important!
    slots = nil -- luacheck: ignore

    local magicRandomPickupSeed = generate_magic_seed(self)

    for k, v in pairs(node_req_map) do
        local reqs = v.reqs

        local ip, port
        local redis_client
        local ok, err_t = nil, {}
        -- Redis 7: try IP first and then hostname as IP does not suffer from stale DNS cache
        local tries = redis7_with_host and 2 or 1
        for i = 1, tries do
            local slave, err
            ip, port, slave, err = pick_node(self, v.serv_list, magicRandomPickupSeed, i)
            if err then
                table_insert(err_t, err)
                self:refresh_slots()
                return nil, table_concat(err_t, ",")
            end

            redis_client = redis:new()
            redis_client:set_timeouts(config.connect_timeout or DEFAULT_CONNECTION_TIMEOUT,
                                      config.send_timeout or DEFAULT_SEND_TIMEOUT,
                                      config.read_timeout or DEFAULT_READ_TIMEOUT)
            ok, err = redis_client:connect(ip, port, self.config.connect_opts)
            if ok then
                v.slave = slave
                break
            end
            table_insert(err_t, err)
        end
        if not ok then
            self:refresh_slots()
            return nil, "failed to connect, err: " .. table_concat(err_t, ",")
        end

        local _, autherr = check_auth(self, redis_client)
        if autherr then
            redis_client:close()
            return nil, autherr
        end

        if v.slave then
            --set readonly
            local ok, err = redis_client:readonly()
            if not ok then
                redis_client:close()
                self:refresh_slots()
                return nil, err
            end
        end

        redis_client:init_pipeline()
        for i = 1, #reqs do
            local req = reqs[i]
            if #req.args > 0 then
                if req.cmd == "eval" or req.cmd == "evalsha" then
                    redis_client[req.cmd](redis_client, unpack(req.args))
                else
                    redis_client[req.cmd](redis_client, req.key, unpack(req.args))
                end
            else
                redis_client[req.cmd](redis_client, req.key)
            end
        end
        local res, err = redis_client:commit_pipeline()
        if err then
            redis_client:close()
            -- There might be node fail, we should also refresh slot cache
            self:refresh_slots()
            return nil, err .. " return from " .. tostring(ip) .. ":" .. tostring(port)
        end

        if has_cluster_fail_signal_in_pipeline(res) then
            redis_client:close()
            self:refresh_slots()
            return nil, "Cannot executing pipeline command, cluster status is failed!"
        end

        -- only put back to connection pool when there are not errors
        release_connection(redis_client, config)

        node_res_map[k] = res
    end

    --construct final result with origin index
    local final_res, err = construct_final_pipeline_resp(self, node_res_map, node_req_map)
    if not err then
        return final_res
    else
        return nil, err .. " failed to construct final pipeline result "
    end
end


function _M.cancel_pipeline(self)
    self._reqs = nil
end


local function _do_eval_cmd(self, cmd, ...)
--[[
eval command usage:
eval(script, 1, key, arg1, arg2 ...)
eval(script, 0, arg1, arg2 ...)
]]
    local args = {...}
    local keys_num = args[2]
    if type(keys_num) ~= "number" then
        return nil, "Cannot execute eval without keys number"
    end
    if keys_num > 1 then
        return nil, "Cannot execute eval with more than one keys for redis cluster"
    end
    local key = args[3] or "no_key"
    return _do_cmd(self, cmd, key, ...)
end
-- dynamic cmd
setmetatable(_M, {
    __index = function(_, cmd)
        local method =
        function(self, ...)
            if cmd == "eval" or cmd == "evalsha" then
                return _do_eval_cmd(self, cmd, ...)
            else
                return _do_cmd(self, cmd, ...)
            end
        end

        -- cache the lazily generated method in our
        -- module table
        _M[cmd] = method
        return method
    end
})

return _M
