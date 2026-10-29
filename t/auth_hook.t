use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "yes") {
    plan tests => repeat_each() * (3 * blocks());
} else {
    plan(skip_all => "skip when REDIS_AUTH is not enabled");
}

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: the hook fixes the password while the library fetches the slots
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local calls = 0
            local config = {
                name = "hookCluster1",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                keepalive_timeout = 60000,
                keepalive_cons = 1000,
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                -- AUTH runs on every use of a pooled connection, so both runs of this
                -- block call the hook once, including the run with a warm slot cache.
                force_auth = true,
                auth_config_reconfigurer = function(self, err)
                    calls = calls + 1
                    self.config.auth = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            if not red then
                ngx.say("failed to create: ", err)
                return
            end

            local res, err = red:set("dog", "an animal")
            ngx.say("set: ", res, ", hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body
set: OK, hook calls: 1
--- no_error_log
[error]

=== TEST 2: the hook runs for a command on a warm slot cache, and the library repeats AUTH on the same connection
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster2",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "kong",
                -- AUTH runs on every use of a pooled connection, so a wrong password fails at once.
                force_auth = true,
                auth_config_reconfigurer = function(self, err)
                    self.config.auth = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))
            assert(red:set("dog", "an animal"))

            red.config.auth = "wrong-password"

            red:init_pipeline()
            red:get("dog")
            red:set("cat", "another animal")
            local res, err = red:commit_pipeline()
            if not res then
                ngx.say("failed: ", err)
                return
            end

            ngx.say("get: ", res[1], ", set: ", res[2])
        ';
    }
--- request
GET /t
--- response_body
get: an animal, set: OK
--- no_error_log
[error]

=== TEST 3: a hook that returns false leaves the error unchanged
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster3",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                auth_config_reconfigurer = function() return false end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say(red and "created" or "failed", ": ", err and err:find("WRONGPASS", 1, true) and "WRONGPASS" or tostring(err))
        ';
    }
--- request
GET /t
--- response_body_like
failed: .*WRONGPASS.*
--- error_log
failed to fetch slots

=== TEST 4: an error in the hook does not stop the request, and the library reports the AUTH error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster4",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                auth_config_reconfigurer = function() error("the hook failed") end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say(red and "created" or "failed", ": ", err and err:find("WRONGPASS", 1, true) and "WRONGPASS" or tostring(err))
        ';
    }
--- request
GET /t
--- response_body_like
failed: .*WRONGPASS.*
--- error_log
auth_config_reconfigurer failed

=== TEST 5: the hook updates the username and the password
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local calls = 0
            local config = {
                name = "hookCluster5",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                username = "kong",
                password = "wrong-password",
                force_auth = true,
                auth_config_reconfigurer = function(self, err)
                    calls = calls + 1
                    self.config.username = "kong"
                    self.config.password = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            if not red then
                ngx.say("failed to create: ", err)
                return
            end

            local res, err = red:set("dog", "an animal")
            ngx.say("set: ", res, ", hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body
set: OK, hook calls: 1
--- no_error_log
[error]

=== TEST 6: the library repeats AUTH only once per failure
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster6",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "kong",
                force_auth = true,
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))
            assert(red:set("dog", "an animal"))

            local calls = 0
            -- The hook cannot fix the password, so the retry must not loop.
            config.auth_config_reconfigurer = function(self, err)
                calls = calls + 1
                return true
            end

            red.config.auth = "wrong-password"

            local res, err = red:get("dog")
            ngx.say("get: ", tostring(res), ", hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body
get: nil, hook calls: 1
--- no_error_log
[error]

=== TEST 7: the hook runs for a single command
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local calls = 0
            local config = {
                name = "hookCluster7",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "kong",
                force_auth = true,
                auth_config_reconfigurer = function(self, err)
                    calls = calls + 1
                    self.config.auth = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))
            assert(red:set("dog", "an animal"))

            red.config.auth = "wrong-password"

            local res, err = red:get("dog")
            ngx.say("get: ", res, ", hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body
get: an animal, hook calls: 1
--- no_error_log
[error]

=== TEST 8: the hook can yield while it reads a new password
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local calls = 0
            local config = {
                name = "hookCluster8",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                -- AUTH runs on every use of a pooled connection, so both runs of this
                -- block call the hook once.
                force_auth = true,
                auth_config_reconfigurer = function(self, err)
                    calls = calls + 1
                    -- A hook that reads a secret store yields.
                    ngx.sleep(0.01)
                    self.config.auth = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            if not red then
                ngx.say("failed to create: ", err)
                return
            end

            local res, err = red:set("dog", "an animal")
            ngx.say("set: ", res, ", hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body
set: OK, hook calls: 1
--- no_error_log
[error]

=== TEST 9: the hook does not run for a non-AUTH error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local calls = 0
            -- This nginx listens on the port below. It accepts the TCP connection, but it
            -- does not speak Redis, so the AUTH step fails with an error that is not
            -- "rejected credentials".
            local config = {
                name = "hookCluster9",
                serv_list = { { ip = "127.0.0.1", port = tonumber(ngx.var.server_port) } },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "kong",
                force_auth = true,
                auth_config_reconfigurer = function(self, err)
                    calls = calls + 1
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say("new: ", red and "ok" or "failed")
            ngx.say("hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body_like
new: failed\s+hook calls: 0
--- error_log
failed to fetch slots

=== TEST 10: the hook does not run when no credential is configured
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local calls = 0
            local config = {
                name = "hookCluster10",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth_config_reconfigurer = function(self, err)
                    calls = calls + 1
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say("new: ", red and "ok" or "failed")
            ngx.say("hook calls: ", calls)
        ';
    }
--- request
GET /t
--- response_body_like
new: failed\s+hook calls: 0
--- error_log
NOAUTH

=== TEST 11: a hook that raises a table does not stop the request
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster11",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                auth_config_reconfigurer = function() error({ code = 403 }) end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say(red and "created" or "failed", ": ", err and err:find("WRONGPASS", 1, true) and "WRONGPASS" or tostring(err))
        ';
    }
--- request
GET /t
--- response_body_like
failed: .*WRONGPASS.*
--- error_log
auth_config_reconfigurer failed: table:

=== TEST 12: a hook that raises a function does not stop the request
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster12",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                auth_config_reconfigurer = function() error(function() end) end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say(red and "created" or "failed", ": ", err and err:find("WRONGPASS", 1, true) and "WRONGPASS" or tostring(err))
        ';
    }
--- request
GET /t
--- response_body_like
failed: .*WRONGPASS.*
--- error_log
auth_config_reconfigurer failed: function:

=== TEST 13: a hook that raises nil does not stop the request
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster13",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                auth_config_reconfigurer = function() error(nil) end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say(red and "created" or "failed", ": ", err and err:find("WRONGPASS", 1, true) and "WRONGPASS" or tostring(err))
        ';
    }
--- request
GET /t
--- response_body_like
failed: .*WRONGPASS.*
--- error_log
auth_config_reconfigurer failed: nil

=== TEST 14: a truthy return value that is not true does not repeat AUTH
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCluster14",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                auth = "wrong-password",
                auth_config_reconfigurer = function(self, err)
                    self.config.auth = "kong"
                    return "yes"
                end,
            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            ngx.say(red and "created" or "failed", ": ", err and err:find("WRONGPASS", 1, true) and "WRONGPASS" or tostring(err))
        ';
    }
--- request
GET /t
--- response_body_like
failed: .*WRONGPASS.*
--- error_log
failed to fetch slots

=== TEST 15: a connection with new credentials does not enter the old pool
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local serv_list = {
                { ip = "127.0.0.1", port = 6371 },
                { ip = "127.0.0.1", port = 6372 },
                { ip = "127.0.0.1", port = 6373 },
            }

            local cfg_a = {
                name = "hookPoolA",
                serv_list = serv_list,
                username = "default",
                password = "wrong-password",
                auth_config_reconfigurer = function(self, err)
                    self.config.password = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local a, aerr = redis:new(cfg_a)
            if not a then
                ngx.say("A failed: ", aerr)
                return
            end

            assert(a:set("pool-key", "value"))

            local cfg_b = {
                name = "hookPoolB",
                serv_list = serv_list,
                username = "default",
                password = "kong",
            }
            assert(redis:new(cfg_b))

            local cfg_c = {
                name = "hookPoolC",
                serv_list = serv_list,
                username = "default",
                password = "wrong-password",
            }
            local c, cerr = redis:new(cfg_c)

            ngx.say("A and B share the pool name: ", tostring(cfg_a.base_pool == cfg_b.base_pool))
            ngx.say("C created: ", c and "yes" or "no")
            ngx.say("C error is WRONGPASS: ", tostring(cerr ~= nil and cerr:find("WRONGPASS", 1, true) ~= nil))
        ';
    }
--- request
GET /t
--- response_body
A and B share the pool name: true
C created: no
C error is WRONGPASS: true
--- error_log
failed to fetch slots: WRONGPASS

=== TEST 16: an explicit pool name is not changed
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookPoolP",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = "default",
                password = "wrong-password",
                connect_opts = { pool = "user-pool" },
                auth_config_reconfigurer = function(self, err)
                    self.config.password = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local p, perr = redis:new(config)
            if not p then
                ngx.say("failed: ", perr)
                return
            end

            assert(p:set("pool-key", "value"))
            ngx.say("base_pool: ", config.base_pool)
        ';
    }
--- request
GET /t
--- response_body
base_pool: user-pool
--- no_error_log
[error]

=== TEST 17: a command returns its connection to the pool
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local resty_redis = require "resty.redis"

            -- Record the node and the pool name of the last connection.
            local connect = resty_redis.connect
            local used
            resty_redis.connect = function(self, host, port, opts)
                used = { host = host, port = port, pool = opts and opts.pool }
                return connect(self, host, port, opts)
            end

            local config = {
                name = "hookPoolReuse",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = "default",
                password = "kong",
                -- The password is correct, so the hook must not run. The connection must
                -- still go back to the pool.
                auth_config_reconfigurer = function() error("must not run") end,
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))
            assert(red:set("pool-key", "value"))

            resty_redis.connect = connect

            local probe = resty_redis:new()
            assert(probe:connect(used.host, used.port, { pool = used.pool }))
            ngx.say("reused: ", tostring(probe:get_reused_times() > 0))
        ';
    }
--- request
GET /t
--- response_body
reused: true
--- no_error_log
[error]

=== TEST 18: the library does not change the pool name of the caller
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookUserPool",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = "default",
                password = "kong",
                connect_opts = { pool = "user-pool", ssl = false },
                -- The password is correct, so the hook must not run.
                auth_config_reconfigurer = function() error("must not run") end,
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))
            assert(red:set("pool-key", "value"))

            ngx.say("caller pool: ", tostring(config.connect_opts.pool))
            ngx.say("base pool: ", tostring(config.base_pool))
        ';
    }
--- request
GET /t
--- response_body
caller pool: user-pool
base pool: user-pool
--- no_error_log
[error]

=== TEST 19: a config table works with a second instance
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookReusedConfig",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = "default",
                password = "kong",
            }

            local redis = require "resty.rediscluster"
            local first = assert(redis:new(config))
            local first_base = config.base_pool
            assert(first:set("pool-key", "value"))

            local second = assert(redis:new(config))

            ngx.say("pool in config: ", tostring(config.connect_opts and config.connect_opts.pool))
            ngx.say("base pool is stable: ", tostring(config.base_pool == first_base))
            ngx.say("base pool has no address: ", tostring(config.base_pool:find("127.0.0.1", 1, true) == nil))
        ';
    }
--- request
GET /t
--- response_body
pool in config: nil
base pool is stable: true
base pool has no address: true
--- no_error_log
[error]

=== TEST 20: the library passes a copy of the connect options
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookCopyOptions",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = "default",
                password = "kong",
            }

            local resty_redis = require "resty.redis"
            local connect = resty_redis.connect
            local seen
            resty_redis.connect = function(self, host, port, opts)
                seen = seen or {
                    same_table = opts == config.connect_opts,
                    caller_pool = config.connect_opts and config.connect_opts.pool,
                    pool = opts and opts.pool,
                    host = host,
                    port = port,
                }
                return connect(self, host, port, opts)
            end

            local redis = require "resty.rediscluster"
            local ok, err = pcall(function()
                local red = assert(redis:new(config))
                assert(red:set("pool-key", "value"))
            end)

            -- Restore the module function before the assertions.
            resty_redis.connect = connect

            if not ok then
                ngx.say("failed: ", err)
                return
            end

            ngx.say("same table: ", tostring(seen.same_table))
            ngx.say("caller pool during connect: ", tostring(seen.caller_pool))
            ngx.say("pool has the node: ", tostring(seen.pool == seen.host .. ":" .. seen.port .. ":" .. config.base_pool))
        ';
    }
--- request
GET /t
--- response_body
same table: false
caller pool during connect: nil
pool has the node: true
--- no_error_log
[error]

=== TEST 23: a connection from another credential generation is not pooled
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "hookPoolGeneration",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = "default",
                password = "kong",
            }

            local resty_redis = require "resty.redis"
            local connect = resty_redis.connect
            local used_pool
            local flipped = false
            resty_redis.connect = function(self, host, port, opts)
                if not flipped then
                    flipped = true
                    used_pool = opts.pool
                    -- Another coroutine refreshed the credentials while this connection
                    -- was waiting on the network.
                    config.base_pool = config.base_pool .. "-new"
                end
                return connect(self, host, port, opts)
            end

            local redis = require "resty.rediscluster"
            local ok, err = pcall(function()
                local red = assert(redis:new(config))
                assert(red:set("generation-key", "value"))
            end)

            -- Restore the module function before the assertions.
            resty_redis.connect = connect

            if not ok then
                ngx.say("failed: ", err)
                return
            end

            -- The connection that used the old pool name must be closed.
            local probe = resty_redis:new()
            assert(probe:connect("127.0.0.1", 6371, { pool = used_pool }))
            ngx.say("reused: ", tostring(probe:get_reused_times() > 0))
        ';
    }
--- request
GET /t
--- response_body
reused: false
--- no_error_log
[error]

=== TEST 21: the pool name contains the auth value
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local serv_list = {
                { ip = "127.0.0.1", port = 6371 },
                { ip = "127.0.0.1", port = 6372 },
                { ip = "127.0.0.1", port = 6373 },
            }

            local redis = require "resty.rediscluster"
            local good, good_err = redis:new({
                name = "hookAuthNameGood",
                serv_list = serv_list,
                auth = "kong",
            })
            if not good then
                ngx.say("good failed: ", good_err)
                return
            end

            -- The password is wrong: `new()` fails after the library computed the pool
            -- name, so the config table still shows it.
            local bad = {
                name = "hookAuthNameBad",
                serv_list = serv_list,
                auth = "wrong-password",
            }
            local bad_instance, bad_err = redis:new(bad)

            ngx.say("bad created: ", bad_instance and "yes" or "no")
            ngx.say("bad error has WRONGPASS: ", tostring(bad_err ~= nil and tostring(bad_err):find("WRONGPASS", 1, true) ~= nil))
            ngx.say("bad pool is set: ", tostring(bad.base_pool ~= nil))
            ngx.say("good pool has a credential: ", tostring(good.config.base_pool:find("^nil:nil:::[0-9a-f]+$") ~= nil))
            ngx.say("pool names differ: ", tostring(good.config.base_pool ~= bad.base_pool))
        ';
    }
--- request
GET /t
--- response_body
bad created: no
bad error has WRONGPASS: true
bad pool is set: true
good pool has a credential: true
pool names differ: true
--- error_log
failed to fetch slots: WRONGPASS

=== TEST 22: auth configurations do not share a pool
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local serv_list = {
                { ip = "127.0.0.1", port = 6371 },
                { ip = "127.0.0.1", port = 6372 },
                { ip = "127.0.0.1", port = 6373 },
            }

            local cfg_a = {
                name = "hookAuthPoolA",
                serv_list = serv_list,
                auth = "wrong-password",
                auth_config_reconfigurer = function(self, err)
                    self.config.auth = "kong"
                    return true
                end,
            }

            local redis = require "resty.rediscluster"
            local a, aerr = redis:new(cfg_a)
            if not a then
                ngx.say("A failed: ", aerr)
                return
            end
            assert(a:set("auth-pool-key", "value"))

            local cfg_b = {
                name = "hookAuthPoolB",
                serv_list = serv_list,
                auth = "kong",
            }
            assert(redis:new(cfg_b))

            ngx.say("A and B share the pool name: ", tostring(cfg_a.base_pool == cfg_b.base_pool))

            local cfg_c = {
                name = "hookAuthPoolC",
                serv_list = serv_list,
                auth = "wrong-password",
            }
            local c, cerr = redis:new(cfg_c)

            ngx.say("C created: ", c and "yes" or "no")
            ngx.say("C error has WRONGPASS: ", tostring(cerr ~= nil and tostring(cerr):find("WRONGPASS", 1, true) ~= nil))
        ';
    }
--- request
GET /t
--- response_body
A and B share the pool name: true
C created: no
C error has WRONGPASS: true
--- error_log
failed to fetch slots: WRONGPASS
