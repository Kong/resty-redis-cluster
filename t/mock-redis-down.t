# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "no") {
    plan tests => repeat_each() * (6 * blocks());
} else {
    plan(skip_all => "skip when REDIS_AUTH is enabled");
}

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;

    init_by_lua_block {
        require "resty.core"
        local redis = require "resty.redis"
        local origin_redis_connect_fn = redis.connect
        redis.connect = function(...)
            local args = {...}
            if not args[4] then
                return nil, "mock redis connect error"
            end

            local red = origin_redis_connect_fn(...)
            return red
        end
    }
};

no_long_string();
#no_diff();


run_tests();

__DATA__

=== TEST 15: fetch_slots
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local config = {
                            name = "testCluster",                   --rediscluster name
                            serv_list = {                           --redis cluster node list(host and port),
                                            { ip = "127.0.0.1", port = 6371 },
                                            { ip = "127.0.0.1", port = 6372 },
                                            { ip = "127.0.0.1", port = 6373 },
                                            { ip = "127.0.0.1", port = 6374 },
                                            { ip = "127.0.0.1", port = 6375 },
                                            { ip = "127.0.0.1", port = 6376 }
                                        },
                            keepalive_timeout = 60000,              --redis connection pool idle timeout
                            keepalive_cons = 1000,                  --redis connection pool size
                            connect_timeout = 1000,               --timeout while connecting
                            read_timeout = 1000,                    --timeout while reading
                            send_timeout = 1000,                    --timeout while sending
                            max_redirection = 5,                    --maximum retry attempts for redirection
                            connect_opts = {
                                                backlog = 30,
                                                pool_size = 30,
                                                ssl = false,
                                                ssl_verify = false,
                                            },

            }

            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)

            if not red then
                ngx.say("failed to instantiate redis: ", err)
                return
            end

            local res, err = red:set("foo", "bar")
            if not res then
                ngx.say("failed to set foo: ", err)
                return
            end

            -- mock redis connect error
            red.config.connect_opts = nil

            local res, err = red:get("foo")
            if not res then
                ngx.say("failed to get foo: ", err)
                return
            end
            ngx.say("get foo: ", res)
        ';
    }
--- request
GET /t
--- no_error_log eval
[qr/fetching slots from: 18 servers/,
qr/fetching slots from: 24 servers/,
qr/fetching slots from: 30 servers/,
qr/fetching slots from: 36 servers/]
--- response_body
failed to get foo: mock redis connect error
--- wait: 1
