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
