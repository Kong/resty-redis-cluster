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
