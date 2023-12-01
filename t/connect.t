# vim:set ft= ts=4 sw=4 et:

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
    lua_shared_dict redis_cluster_slot_locks 100k;
};


no_long_string();

run_tests();

__DATA__

=== TEST 1: concurrent connections exceed the backlog limit(with auth)
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
                            connect_opts = {
                                                backlog = 1,
                                                pool_size = 1,
                                            },
                            username = "kong",
                            password = "kong",

            }

            local t = {}
            for i = 1, 6 do
                local th = assert(ngx.thread.spawn(function(i)
                    local redis = require "resty.rediscluster"
                    local red, err = redis:new(config)

                    if err then
                        ngx.say("failed to create redis cluster client: ", err)
                        return
                    end

                    red:init_pipeline()
                    red:hgetall("animals")

                    local res, err = red:commit_pipeline()
                    if err then
                        ngx.say(err)
                        return
                    end
                end, i))
                table.insert(t, th)
            end
            for i, th in ipairs(t) do
                ngx.thread.wait(th)
            end
        ';
    }
--- request
GET /t
--- response_body eval
qr/failed to connect, err: too many waiting connect operations/
--- no_error_log
[error]



=== TEST 2: connections in the backlog queue have reached timeout(with auth)
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
                            connect_timeout = 5,
                            connect_opts = {
                                                backlog = 100,
                                                pool_size = 1,
                                            },
                            username = "kong",
                            password = "kong",

            }

            local t = {}
            for i = 1, 100 do
                local th = assert(ngx.thread.spawn(function(i)
                    local redis = require "resty.rediscluster"
                    local red, err = redis:new(config)

                    if err then
                        ngx.say("failed to create redis cluster client: ", err)
                        return
                    end

                    red:init_pipeline()
                    red:hmset("animals", { dog = "bark", cat = "meow", cow = "moo" })

                    local res, err = red:commit_pipeline()
                    if err then
                        ngx.say(err)
                        return
                    end
                end, i))
                table.insert(t, th)
            end
            for i, th in ipairs(t) do
                ngx.thread.wait(th)
            end
        ';
    }
--- request
GET /t
--- response_body eval
qr/failed to connect, err: timeout/
--- error_log eval
qr/lua tcp socket queued connect timed out/
