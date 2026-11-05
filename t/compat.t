use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => 3 * blocks();

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;$pwd/t/fixtures/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: set_keepalive and close do not send a command
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "compatKeepalive",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:init_pipeline()
            local keepalive = red:set_keepalive(10000, 100)
            local closed = red:close()
            local reused = red:get_reused_times()

            local sent = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" then
                    sent = sent + 1
                end
            end

            ngx.say("keepalive: ", tostring(keepalive))
            ngx.say("close: ", tostring(closed), " reused: ", tostring(reused))
            ngx.say("commands: ", sent)
        ';
    }
--- request
GET /t
--- response_body
keepalive: 1
close: 1 reused: 0
commands: 0
--- no_error_log
[error]

=== TEST 2: array_to_hash converts a flat array
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "compatArrayToHash",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local h = red:array_to_hash({ "a", "1", "b", "2" })
            ngx.say("a=", tostring(h.a), " b=", tostring(h.b))
        ';
    }
--- request
GET /t
--- response_body
a=1 b=2
--- no_error_log
[error]
