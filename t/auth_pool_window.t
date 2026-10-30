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

=== TEST 1: the pool name changes before the AUTH retry
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config
            local base_at_retry
            local bases = {}
            local closed, pooled = 0, 0
            local CRLF = string.char(13, 10)

            local fake_redis = {
                new = function()
                    return {
                        set_timeouts = function() end,
                        connect = function()
                            bases[#bases + 1] = config.base_pool
                            return 1
                        end,
                        get_reused_times = function() return 0 end,
                        info = function() return "redis_version:6.2.14" .. CRLF end,
                        cluster = function(self, sub)
                            if sub == "nodes" then
                                return ""
                            end
                            if sub == "info" then
                                return "cluster_state:ok" .. CRLF
                            end
                            return { { 0, 16383, { "127.0.0.1", 7001, "id1" } } }
                        end,
                        auth = function(self, password)
                            if password == "old" then
                                return false, "WRONGPASS invalid username-password pair"
                            end
                            base_at_retry = config.base_pool
                            return "OK"
                        end,
                        set_keepalive = function()
                            pooled = pooled + 1
                            return 1
                        end,
                        close = function()
                            closed = closed + 1
                        end,
                        get = function() return "v" end,
                    }
                end,
            }

            package.loaded["resty.redis"] = fake_redis
            local redis = require "resty.rediscluster"

            config = {
                name = "poolWindow",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                password = "old",
                auth_config_reconfigurer = function(self, err)
                    self.config.password = "new"
                    return true
                end,
            }

            local red, err = redis:new(config)
            if not red then
                ngx.say("failed: ", err)
                return
            end
            -- The pool name must move to the new credential before the AUTH retry.
            ngx.say("the first connection used the old pool: ", tostring(bases[1] ~= config.base_pool))
            ngx.say("base at retry equals final: ", tostring(base_at_retry == config.base_pool))
            ngx.say("base has a credential: ", tostring(config.base_pool:find("^nil:nil:::[0-9a-f]+$") ~= nil))
            -- The connection authenticated with the old credential and then replaced it.
            -- It must not go back to the pool under the name it was taken from.
            ngx.say("closed: ", closed, " pooled: ", pooled)
        ';
    }
--- request
GET /t
--- response_body
the first connection used the old pool: true
base at retry equals final: true
base has a credential: true
closed: 1 pooled: 0
--- no_error_log
[error]
