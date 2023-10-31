use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
    init_by_lua '
        require("luacov")
    ';
};


no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: auth
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local config = {
                            name = "testCluster",                   --rediscluster name
                            serv_list = {                           --redis cluster node list(host and port),
                                            { ip = "127.0.0.1", port = 6381 },
                                            { ip = "127.0.0.1", port = 6382 },
                                            { ip = "127.0.0.1", port = 6383 },
                                            { ip = "127.0.0.1", port = 6384 },
                                            { ip = "127.0.0.1", port = 6385 },
                                            { ip = "127.0.0.1", port = 6386 },
                                            { ip = "127.0.0.1", port = 6387 }
                                        },
                            keepalive_timeout = 60000,              --redis connection pool idle timeout
                            keepalive_cons = 1000,                  --redis connection pool size
                            connect_timeout = 1000,                 --timeout while connecting
                            read_timeout = 1000,                    --timeout while reading
                            send_timeout = 1000,                    --timeout while sending
                            max_redirection = 5,                    --maximum retry attempts for redirection
                            auth = "kong",

            }
            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)

            if err then
                ngx.say("failed to create: ", err)
                return
            end


            local res, err = red:set("dog", "an animal")
            if not res then
                ngx.say("failed to set dog: ", err)
                return
            end

            ngx.say("set dog: ", res)

            for i = 1, 2 do
                local res, err = red:get("dog")
                if err then
                    ngx.say("failed to get dog: ", err)
                    return
                end

                if not res then
                    ngx.say("dog not found.")
                    return
                end

                ngx.say("dog: ", res)
            end
        ';
    }
--- request
GET /t
--- response_body
set dog: OK
dog: an animal
dog: an animal
--- no_error_log
[error]

=== TEST 2: username and password
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local config = {
                            name = "testCluster",                   --rediscluster name
                            serv_list = {                           --redis cluster node list(host and port),
                                            { ip = "127.0.0.1", port = 6381 },
                                            { ip = "127.0.0.1", port = 6382 },
                                            { ip = "127.0.0.1", port = 6383 },
                                            { ip = "127.0.0.1", port = 6384 },
                                            { ip = "127.0.0.1", port = 6385 },
                                            { ip = "127.0.0.1", port = 6386 },
                                            { ip = "127.0.0.1", port = 6387 }
                                        },
                            keepalive_timeout = 60000,              --redis connection pool idle timeout
                            keepalive_cons = 1000,                  --redis connection pool size
                            connect_timeout = 1000,                 --timeout while connecting
                            read_timeout = 1000,                    --timeout while reading
                            send_timeout = 1000,                    --timeout while sending
                            max_redirection = 5,                    --maximum retry attempts for redirection
                            username = "default",
                            password = "kong",

            }
            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)

            if err then
                ngx.say("failed to create: ", err)
                return
            end


            local res, err = red:set("dog", "an animal")
            if not res then
                ngx.say("failed to set dog: ", err)
                return
            end

            ngx.say("set dog: ", res)

            for i = 1, 2 do
                local res, err = red:get("dog")
                if err then
                    ngx.say("failed to get dog: ", err)
                    return
                end

                if not res then
                    ngx.say("dog not found.")
                    return
                end

                ngx.say("dog: ", res)
            end
        ';
    }
--- request
GET /t
--- response_body
set dog: OK
dog: an animal
dog: an animal
--- no_error_log
[error]
