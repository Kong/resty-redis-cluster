use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => 3 * blocks();

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;$pwd/t/fixtures/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: an ASK reply sends the command to the target node
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.topology = 1
            fake.handler = function(self, cmd)
                if cmd == "get" and self._node == "127.0.0.1:7003" then
                    return false, "ASK 12182 127.0.0.1:7001"
                elseif cmd == "asking" then
                    return "OK"
                elseif cmd == "get" and self._node == "127.0.0.1:7001" then
                    return "bar"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askCluster",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:get("foo")
            ngx.say("result: ", tostring(res), " err: ", tostring(err))

            local asking, gets = 0, 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" then
                    if e.cmd == "asking" then asking = asking + 1 end
                    if e.cmd == "get" then gets = gets + 1 end
                end
            end
            ngx.say("asking: ", asking, " gets: ", gets)
        ';
    }
--- request
GET /t
--- response_body
result: bar err: nil
asking: 1 gets: 2
--- no_error_log
[error]

=== TEST 2: a MOVED reply sends the command to the new owner
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- The slot map is stale: 7001 owns every slot.
            fake.topology = 3
            fake.handler = function(self, cmd)
                if cmd == "get" and self._node == "127.0.0.1:7001" then
                    -- The next `cluster slots` reports the real map.
                    fake.topology = 1
                    return false, "MOVED 12182 127.0.0.1:7003"
                elseif cmd == "get" and self._node == "127.0.0.1:7003" then
                    return "bar"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "movedCluster",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:get("foo")
            ngx.say("result: ", tostring(res), " err: ", tostring(err))

            local gets = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "get" then gets = gets + 1 end
            end
            ngx.say("gets: ", gets)
        ';
    }
--- request
GET /t
--- response_body
result: bar err: nil
gets: 2
--- no_error_log
[error]


=== TEST 3: an ASK reply that names a hostname uses the IP of the node
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- Redis 7 that announces hostnames.
            fake.topology = 4
            -- The hostnames of the cluster cannot be resolved from this host.
            fake.connect = function(self, host, port)
                if host:match("^rc%-node") then
                    return nil, host .. " could not be resolved"
                end
                return 1
            end
            fake.handler = function(self, cmd)
                if cmd == "get" and self._node == "127.0.0.3:7003" then
                    return false, "ASK 12182 rc-node-1:7001"
                elseif cmd == "asking" then
                    return "OK"
                elseif cmd == "get" and self._node == "127.0.0.1:7001" then
                    return "bar"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askHostname",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:get("foo")
            ngx.say("result: ", tostring(res), " err: ", tostring(err))

            local hostname_connects, asks = 0, 0
            for _, e in ipairs(fake.events) do
                if e.ev == "connect" and e.node:match("^rc%-node") then
                    hostname_connects = hostname_connects + 1
                end
                if e.ev == "cmd" and e.cmd == "asking" then
                    asks = asks + 1
                end
            end
            ngx.say("connects to a hostname: ", hostname_connects, " asking: ", asks)
        ';
    }
--- request
GET /t
--- response_body
result: bar err: nil
connects to a hostname: 0 asking: 1
--- no_error_log
[error]


=== TEST 4: an ASK reply inside a pipeline is retried on the target node
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.topology = 1
            fake.handler = function(self, cmd)
                if cmd == "get" and self._node == "127.0.0.1:7003" then
                    -- The real client keeps a queued error reply as {false, message}.
                    return false, "ASK 12182 127.0.0.1:7001"
                elseif cmd == "asking" then
                    return "OK"
                elseif cmd == "get" and self._node == "127.0.0.1:7001" then
                    return "bar"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askPipelineCluster",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:init_pipeline()
            red:get("foo")
            local res, err = red:commit_pipeline()
            if not res then
                ngx.say("failed: ", err)
                return
            end
            ngx.say("result: ", tostring(res[1]), " err: ", tostring(err))

            local asking = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "asking" then asking = asking + 1 end
            end
            ngx.say("asking: ", asking)
        ';
    }
--- request
GET /t
--- response_body
result: bar err: nil
asking: 1
--- no_error_log
[error]


=== TEST 5: an ASK reply in a pure IP cluster reaches the target directly
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- Every node uses port 6379, so a pairing by position would treat the
            -- previous node as the IP of the ASK target.
            fake.topology = 5
            fake.handler = function(self, cmd)
                if cmd == "get" and self._node == "10.0.0.3:6379" then
                    return false, "ASK 12182 10.0.0.2:6379"
                elseif cmd == "asking" then
                    return "OK"
                elseif cmd == "get" then
                    return "bar"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "pureIpCluster",
                serv_list = { { ip = "10.0.0.1", port = 6379 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            assert(red:get("foo"))

            local nodes, to_seed = {}, 0
            for _, e in ipairs(fake.events) do
                if e.ev == "connect" then
                    nodes[#nodes + 1] = e.node
                    if e.node == "10.0.0.1:6379" then
                        to_seed = to_seed + 1
                    end
                end
            end
            ngx.say("third connect: ", tostring(nodes[3]))
            ngx.say("connects to the seed: ", to_seed)
        ';
    }
--- request
GET /t
--- response_body
third connect: 10.0.0.2:6379
connects to the seed: 1
--- no_error_log
[error]


=== TEST 6: a failed ASK attempt does not run the command twice
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- Hostname cluster: the IP and the hostname point at the same node.
            fake.topology = 4
            local ran = 0
            fake.handler = function(self, cmd)
                if cmd == "asking" then
                    rawset(self, "_asked", true)
                    return "OK"
                elseif cmd == "get" then
                    if not rawget(self, "_asked") then
                        return false, "ASK 12182 rc-node-3:7003"
                    end
                    ran = ran + 1
                    if ran == 1 then
                        return nil, "timeout"   -- the server ran it, the caller did not see it
                    end
                    return "bar"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askOnceCluster",
                serv_list = { { ip = "10.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:get("foo")
            ngx.say("command runs on the server: ", ran)
        ';
    }
--- request
GET /t
--- response_body
command runs on the server: 1
--- no_error_log
[error]

=== TEST 7: a pipeline ASK also tries the IP of the target
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.topology = 4
            -- The hostnames of the cluster cannot be resolved from this host.
            fake.connect = function(self, host)
                if host:match("^rc%-node") then
                    return nil, host .. " could not be resolved"
                end
                return 1
            end
            fake.handler = function(self, cmd)
                if cmd == "asking" then
                    rawset(self, "_asked", true)
                    return "OK"
                elseif cmd == "get" then
                    if rawget(self, "_asked") then
                        return "bar"
                    end
                    return false, "ASK 12182 rc-node-3:7003"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askPipeHostCluster",
                serv_list = { { ip = "10.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:init_pipeline()
            red:get("foo")
            local res, err = red:commit_pipeline()
            if not res then
                ngx.say("failed: ", err)
                return
            end
            ngx.say("result: ", tostring(res[1]))
        ';
    }
--- request
GET /t
--- response_body
result: bar
--- no_error_log
[error]


=== TEST 8: an ASK target that answers MOVED ends the request
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.topology = 1
            -- 7003 marks slot 12182 MIGRATING to 7001, but 7001 is not IMPORTING it,
            -- for example when a replica was promoted during the migration. So 7001
            -- ignores ASKING and sends the client back to the owner, and the owner
            -- answers ASK again. A second ASK must end the request.
            local gets = 0
            fake.handler = function(self, cmd)
                if cmd == "asking" then
                    return "OK"
                end
                if cmd == "get" then
                    gets = gets + 1
                    if gets > 20 then
                        -- A loop would run for ever. Stop it early.
                        return false, "ERR guard: still redirecting"
                    end
                    if self._node == "127.0.0.1:7003" then
                        return false, "ASK 12182 127.0.0.1:7001"
                    end
                    return false, "MOVED 12182 127.0.0.1:7003"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askMovedLoop",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:get("foo")
            local refreshes = -1   -- the first CLUSTER SLOTS belongs to new()
            local asking_nodes = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cluster-slots" then
                    refreshes = refreshes + 1
                elseif e.ev == "cmd" and e.cmd == "asking" then
                    asking_nodes[#asking_nodes + 1] = e.node
                end
            end
            ngx.say("result is nil: ", tostring(res == nil))
            ngx.say("nested asking: ", tostring(err ~= nil and err:find("nested asking", 1, true) ~= nil))
            ngx.say("get sent: ", gets, " slot refreshes: ", refreshes)
            ngx.say("asking sent to: ", table.concat(asking_nodes, ","))
        ';
    }
--- request
GET /t
--- response_body
result is nil: true
nested asking: true
get sent: 3 slot refreshes: 2
asking sent to: 127.0.0.1:7001
--- no_error_log
[error]

=== TEST 9: a pipeline ASK that comes back as MOVED ends the request
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.topology = 1
            local gets = 0
            fake.handler = function(self, cmd)
                if cmd == "asking" then
                    return "OK"
                end
                if cmd == "get" then
                    gets = gets + 1
                    if gets > 20 then
                        return false, "ERR guard: still redirecting"
                    end
                    if self._node == "127.0.0.1:7003" then
                        return false, "ASK 12182 127.0.0.1:7001"
                    end
                    return false, "MOVED 12182 127.0.0.1:7003"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "askMovedLoopPipe",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:init_pipeline()
            red:get("foo")
            local res, err = red:commit_pipeline()
            ngx.say("result is nil: ", tostring(res == nil))
            ngx.say("nested asking: ", tostring(err ~= nil and err:find("nested asking", 1, true) ~= nil))
            ngx.say("get sent: ", gets)
        ';
    }
--- request
GET /t
--- response_body
result is nil: true
nested asking: true
get sent: 3
--- no_error_log
[error]
