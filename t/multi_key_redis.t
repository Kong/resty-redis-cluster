use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

# The same fixture serves both modes: the auth mode adds a credential to the
# client config. "default" is the user that the fixture grants +@all.
# Require the mode explicitly: an unset variable used to run the suite with no
# credential and report failures that say nothing about the code.
our $redis_auth = $ENV{REDIS_AUTH};
if (!defined($redis_auth) || ($redis_auth ne "yes" && $redis_auth ne "no")) {
    plan(skip_all => "set REDIS_AUTH to yes or no");
}

# One extra block proves the fixture mode: a client without a credential must be
# rejected in the auth mode and accepted in the no-auth mode. Without it the auth
# run can pass against a fixture that never checks the password, because Redis
# accepts AUTH for a user with nopass.
plan tests => 3 * blocks();

my $auth_opts = "";
if ($redis_auth eq "yes") {
    $auth_opts = qq{                username = "default",\n                password = "kong",\n};
}

# The detail line differs per mode, so assert only the first line.
my $auth_mode_works = $redis_auth eq "yes" ? "false" : "true";

add_block_preprocessor(sub {
    my $block = shift;

    my $config = $block->config;
    if (defined($config)) {
        $config =~ s/__AUTH_OPTS__/$auth_opts/g;
        $block->set_value("config", $config);
    }

    my $body = $block->response_body;
    if (defined($body) && $body =~ /__AUTH_MODE_WORKS__/) {
        $body =~ s/__AUTH_MODE_WORKS__/$auth_mode_works/;
        $block->set_value("response_body", $body);
    }
});

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: MGET reads keys from every master and keeps the order
"mk:a" is on 6372, "mk:b" and "mk:c" are on 6373, "mk:d" is on 6371.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"

            local config = {
                name = "mkRedisMget",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                max_redirection = 5,
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())
            assert(red:set("mk:a", "va"))
            assert(red:set("mk:b", "vb"))
            assert(red:set("mk:c", "vc"))
            assert(red:set("mk:d", "vd"))

            local res, err = red:mget("mk:a", "mk:b", "mk:c", "mk:b", "mk:missing")
            ngx.say("err: ", tostring(err))
            ngx.say("values: ", cjson.encode(res))
        ';
    }
--- request
GET /t
--- response_body
err: nil
values: ["va","vb","vc","vb",null]
--- no_error_log
[error]

=== TEST 2: EXISTS and DEL count every key, also when the keys are on different masters
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "mkRedisDel",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                max_redirection = 5,
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())
            assert(red:set("mk:a", "va"))
            assert(red:set("mk:b", "vb"))
            assert(red:set("mk:c", "vc"))
            assert(red:set("mk:d", "vd"))

            ngx.say("exists: ", red:exists("mk:a", "mk:b", "mk:c", "mk:d", "mk:missing"))
            ngx.say("del: ", red:del("mk:a", "mk:b", "mk:c", "mk:d"))
            ngx.say("exists after: ", red:exists("mk:a", "mk:b", "mk:c", "mk:d"))
        ';
    }
--- request
GET /t
--- response_body
exists: 4
del: 4
exists after: 0
--- no_error_log
[error]

=== TEST 3: a cross-slot MSET is rejected and writes nothing
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "mkRedisMsetCross",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                max_redirection = 5,
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())

            local ok, err = red:mset("mk:a", "1", "mk:b", "2")
            local va = red:get("mk:a")
            local vb = red:get("mk:b")

            local function show(v)
                return v == ngx.null and "null" or tostring(v)
            end
            ngx.say("mset: ", tostring(ok), " ", tostring(err))
            ngx.say("values: ", show(va), ",", show(vb))
        ';
    }
--- request
GET /t
--- response_body
mset: nil command 'mset' needs all keys in one slot; use a hash tag or split the call
values: null,null
--- no_error_log
[error]

=== TEST 4: MSET with keys in one slot writes every key
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local cjson = require "cjson"

            local config = {
                name = "mkRedisMsetSame",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                max_redirection = 5,
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())

            local ok, err = red:mset("{mk}1", "1", "{mk}2", "2")
            local res = red:mget("{mk}1", "{mk}2")

            ngx.say("mset: ", tostring(ok), " ", tostring(err))
            ngx.say("values: ", cjson.encode(res))
        ';
    }
--- request
GET /t
--- response_body
mset: OK nil
values: ["1","2"]
--- no_error_log
[error]

=== TEST 5: the fixture mode matches REDIS_AUTH
A client without a credential must read a key in the no-auth mode and fail in
the auth mode. This block is what makes the auth run meaningful: Redis accepts
AUTH for a user with nopass, so a credential sent to a no-auth fixture is not
an error there, and the rest of the suite would pass without checking anything.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local config = {
                name = "mkRedisAuthMode",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                connect_timeout = 1000,
                read_timeout = 1000,
                send_timeout = 1000,
                max_redirection = 5,
            }

            local redis = require "resty.rediscluster"
            -- The client builds its slot cache in new(), so a rejected AUTH
            -- fails there and not only on the first command.
            local red, new_err = redis:new(config)
            local works = false
            if red then
                local ok = red:mget("{probe}1", "{probe}2")
                works = ok ~= nil and ok ~= false
            end

            -- One line only, so the expectation is an exact match in both modes.
            ngx.log(ngx.INFO, "mode probe: ", tostring(new_err or "client created"))
            ngx.say("unauthenticated read works: ", tostring(works))
        }
    }
--- request
GET /t
# The auth mode logs the rejection at error level, so this block cannot assert
# an empty error log. Assert that neither mode raises an alert.
--- response_body
unauthenticated read works: __AUTH_MODE_WORKS__
--- no_error_log
[alert]
