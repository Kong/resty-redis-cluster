# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "no") {
    plan tests => repeat_each() * (5 * blocks());
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

=== TEST 1: reusing a config without connect_opts does not modify it
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local config = {
                name = "testCluster",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                keepalive_timeout = 60000,
                keepalive_cons = 200,
            }

            local redis = require "resty.rediscluster"
            for i = 1, 5 do
                local red, err = redis:new(config)
                if not red then
                    ngx.say("failed to create: ", err)
                    return
                end

                local res, err = red:set("dog", "an animal")
                if not res then
                    ngx.say("failed to set dog: ", err)
                    return
                end
            end

            ngx.say("connect_opts: ", tostring(config.connect_opts))
            ngx.say("base_pool: ", tostring(config.base_pool))
        }
    }
--- request
GET /t
--- response_body
connect_opts: nil
base_pool: nil
--- log_level: debug
--- error_log eval
qr/set pool name: [\d.]+:637[1-6]:nil:nil:::(?:,|$)/
--- no_error_log eval
["[error]", qr/set pool name: [\d.]+:\d+:[\d.]+:\d+:/]



=== TEST 2: reusing a config with a user-provided pool keeps the pool name stable
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local config = {
                name = "testCluster",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                    { ip = "127.0.0.1", port = 6374 },
                    { ip = "127.0.0.1", port = 6375 },
                    { ip = "127.0.0.1", port = 6376 },
                },
                connect_opts = {
                    pool = "my-pool",
                    backlog = 30,
                },
                keepalive_timeout = 60000,
                keepalive_cons = 200,
            }

            local redis = require "resty.rediscluster"
            for i = 1, 5 do
                local red, err = redis:new(config)
                if not red then
                    ngx.say("failed to create: ", err)
                    return
                end

                local res, err = red:set("dog", "an animal")
                if not res then
                    ngx.say("failed to set dog: ", err)
                    return
                end

                ngx.say(i, ": ", config.connect_opts.pool)
            end
        }
    }
--- request
GET /t
--- response_body
1: my-pool
2: my-pool
3: my-pool
4: my-pool
5: my-pool
--- log_level: debug
--- error_log eval
qr/set pool name: [\d.]+:637[1-6]:my-pool(?:,|$)/
--- no_error_log eval
["[error]", qr/set pool name: [\d.]+:\d+:[\d.]+:\d+:/]
