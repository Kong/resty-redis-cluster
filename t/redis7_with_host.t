use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => 3 * blocks();

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: the slot format is kept for each cluster
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            -- Two clusters report different slot formats. The Redis 6 cluster has a
            -- master and a replica.
            local slots_by_host = {
                ["10.0.0.1"] = { { 0, 16383, { "redis7-node", 6379, "id7", { "ip", "10.0.0.1" } } } },
                ["10.0.0.2"] = { { 0, 16383,
                                   { "redis6-node", 6380, "id6" },
                                   { "redis6-replica", 6381, "id6r" } } },
            }

            local CRLF = string.char(13, 10)
            local connected = {}
            local fake_redis = {
                new = function()
                    return {
                        set_timeouts = function() end,
                        connect = function(self, host, port)
                            connected[#connected + 1] = host

                            -- The master of the Redis 6 cluster is down.
                            if host == "redis6-node" then
                                return nil, "connection refused"
                            end

                            self.host = host
                            return 1
                        end,
                        get_reused_times = function() return 0 end,
                        info = function() return "redis_version:7.0.0" .. CRLF end,
                        cluster = function(self, sub)
                            if sub == "nodes" then
                                return ""
                            end
                            if sub == "info" then
                                return "cluster_state:ok" .. CRLF
                            end
                            return slots_by_host[self.host]
                        end,
                        set_keepalive = function() return 1 end,
                        close = function() end,
                        get = function() return "v" end,
                    }
                end,
            }

            local previous_redis = package.loaded["resty.redis"]
            package.loaded["resty.redis"] = fake_redis
            local lib = require "resty.rediscluster"

            local cfg6 = {
                name = "cluster6",
                serv_list = { { ip = "10.0.0.2", port = 6380 } },
            }
            local cfg7 = {
                name = "cluster7",
                serv_list = { { ip = "10.0.0.1", port = 6379 } },
            }

            -- Parse the Redis 6 cluster first. Its slot cache stays warm.
            assert(lib:new(cfg6))
            -- Parse the Redis 7 cluster after it.
            assert(lib:new(cfg7))
            -- This instance finds the slot cache of cluster6 warm, so it parses nothing.
            local b = assert(lib:new(cfg6))

            local v, err = b:get("k")

            -- Restore the module table before the assertions.
            package.loaded["resty.redis"] = previous_redis

            local tried_replica = false
            for i = 1, #connected do
                if connected[i] == "redis6-replica" then
                    tried_replica = true
                end
            end

            ngx.say("value: ", tostring(v))
            ngx.say("connect failed: ", tostring(err ~= nil and string.find(err, "tried", 1, true) ~= nil))
            ngx.say("tried replica: ", tostring(tried_replica))
        ';
    }
--- request
GET /t
--- response_body
value: nil
connect failed: true
tried replica: false
--- no_error_log
[error]
