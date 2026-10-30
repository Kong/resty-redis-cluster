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

=== TEST 1: one failed master does not stop the command on the others
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "auth" then
                    if self._node == "127.0.0.1:7002" then
                        return false, "WRONGPASS invalid username-password pair"
                    end
                    return "OK"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "flushCluster",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                password = "pw",
            }))

            local res, err = red:flushall()
            ngx.say("result: ", tostring(res))
            ngx.say("error: ", tostring(err))

            local flushed = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "flushall" then
                    flushed[#flushed + 1] = e.node
                end
            end
            table.sort(flushed)
            ngx.say("flushall nodes: ", table.concat(flushed, ","))
        ';
    }
--- request
GET /t
--- response_body
result: nil
error: failed on 1 master(s): 127.0.0.1:7002 WRONGPASS invalid username-password pair
flushall nodes: 127.0.0.1:7001,127.0.0.1:7003
--- no_error_log
[error]

=== TEST 2: every master runs the command
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
                name = "flushClusterOk",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                password = "pw",
            }))

            local res, err = red:flushall()
            ngx.say("result: ", tostring(res), " error: ", tostring(err))

            local flushed = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "flushall" then
                    flushed[#flushed + 1] = e.node
                end
            end
            table.sort(flushed)
            ngx.say("flushall nodes: ", table.concat(flushed, ","))
        ';
    }
--- request
GET /t
--- response_body
result: true error: nil
flushall nodes: 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
--- no_error_log
[error]
