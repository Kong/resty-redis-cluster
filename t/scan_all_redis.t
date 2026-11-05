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

=== TEST 1: scan_all reads the keys of every master and returns no duplicates
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "scanRedisAll",
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
            for i = 1, 10 do
                assert(red:set("scan:" .. i, "v" .. i))
            end

            local cursor = "0"
            local seen = {}
            repeat
                local keys, err
                cursor, keys, err = red:scan_all(cursor, "MATCH", "scan:*", "COUNT", "3")
                if not cursor then
                    ngx.say("scan_all failed: ", tostring(err))
                    return
                end

                for _, key in ipairs(keys) do
                    seen[key] = (seen[key] or 0) + 1
                end
            until cursor == "0"

            local count, duplicates = 0, 0
            for _, n in pairs(seen) do
                count = count + 1
                if n > 1 then
                    duplicates = duplicates + 1
                end
            end

            ngx.say("keys: ", count)
            ngx.say("duplicates: ", duplicates)
        ';
    }
--- request
GET /t
--- response_body
keys: 10
duplicates: 0
--- no_error_log
[error]

=== TEST 2: scan_all ends with cursor 0 when nothing matches
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "scanRedisEmpty",
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

            local cursor, keys, err = red:scan_all("0", "MATCH", "nothing:*")
            ngx.say("cursor: ", tostring(cursor), " err: ", tostring(err))
            ngx.say("keys: ", keys and #keys or "nil")
        ';
    }
--- request
GET /t
--- response_body
cursor: 0 err: nil
keys: 0
--- no_error_log
[error]

=== TEST 3: scan_all with a large COUNT returns every key in one call
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                name = "scanRedisOneCall",
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
            for i = 1, 10 do
                assert(red:set("scan:" .. i, "v" .. i))
            end

            local cursor, keys = red:scan_all("0", "MATCH", "scan:*", "COUNT", "1000")
            ngx.say("cursor: ", tostring(cursor))
            ngx.say("keys: ", keys and #keys or "nil")
        ';
    }
--- request
GET /t
--- response_body
cursor: 0
keys: 10
--- no_error_log
[error]
