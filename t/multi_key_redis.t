use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "no") {
    plan tests => 3 * blocks();
} else {
    plan(skip_all => "skip when REDIS_AUTH is enabled");
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
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())
            assert(red:set("mk:a", "va"))
            assert(red:set("mk:b", "vb"))
            assert(red:set("mk:c", "vc"))
            assert(red:set("mk:d", "vd"))

            local res, err = red:mget("mk:a", "mk:b", "mk:c", "mk:d", "mk:missing")
            ngx.say("err: ", tostring(err))
            ngx.say("values: ", cjson.encode(res))
        ';
    }
--- request
GET /t
--- response_body
err: nil
values: ["va","vb","vc","vd",null]
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
            }

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

=== TEST 3: MSET returns an error when the keys are in different slots
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
            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())

            local ok, err = red:mset("mk:a", "1", "mk:b", "2")
            local value = red:get("mk:a")

            ngx.say("mset: ", tostring(ok), " ", tostring(err))
            ngx.say("mk:a: ", value == ngx.null and "null" or tostring(value))
        ';
    }
--- request
GET /t
--- response_body
mset: nil command 'mset' needs all keys in one slot; use a hash tag or split the call
mk:a: null
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
            }

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
