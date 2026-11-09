use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => 3 * blocks();

# The same fixture serves both modes: the auth mode only adds a credential to
# the client config. "default" is the user that the fixture grants +@all.
my $redis_auth = $ENV{REDIS_AUTH};
my $auth_opts = "";
if (defined($redis_auth) && $redis_auth eq "yes") {
    $auth_opts = qq{                username = "default",\n                password = "kong",\n};
}

add_block_preprocessor(sub {
    my $block = shift;
    my $config = $block->config;
    if (defined($config)) {
        $config =~ s/__AUTH_OPTS__/$auth_opts/g;
        $block->set_value("config", $config);
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
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())
            for i = 1, 10 do
                assert(red:set("scan:" .. i, "v" .. i))
            end

            local cursor = "0"
            local seen = {}
            repeat
                local page, err = red:scan_all(cursor, "MATCH", "scan:*", "COUNT", "3")
                if not page then
                    ngx.say("scan_all failed: ", tostring(err))
                    return
                end
                cursor = page[1]

                for _, key in ipairs(page[2]) do
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
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())

            local res, err = red:scan_all("0", "MATCH", "nothing:*")
            local cursor, keys = res and res[1], res and res[2]
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
__AUTH_OPTS__            }

            local redis = require "resty.rediscluster"
            local red = assert(redis:new(config))

            assert(red:flushall())
            for i = 1, 10 do
                assert(red:set("scan:" .. i, "v" .. i))
            end

            local res = red:scan_all("0", "MATCH", "scan:*", "COUNT", "1000")
            local cursor, keys = res and res[1], res and res[2]
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
