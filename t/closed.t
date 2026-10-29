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

=== TEST 1: a closed pooled connection is retried when the option is on
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.reused = true
            fake.connect = function(self)
                self._reused = fake.reused and 1 or 0
                return 1
            end

            local gets = 0
            fake.handler = function(self, cmd)
                if cmd == "get" then
                    gets = gets + 1
                    if gets == 1 then
                        return false, "closed"
                    end
                    return "v"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "closedCluster",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                retry_on_closed_connection = true,
            }))

            local res, err = red:get("k")
            ngx.say("result: ", tostring(res), " err: ", tostring(err))
            ngx.say("gets: ", gets)
        ';
    }
--- request
GET /t
--- response_body
result: v err: nil
gets: 2
--- no_error_log
[error]

=== TEST 2: the retry is off by default
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.reused = true
            fake.connect = function(self)
                self._reused = fake.reused and 1 or 0
                return 1
            end

            local gets = 0
            fake.handler = function(self, cmd)
                if cmd == "get" then
                    gets = gets + 1
                    if gets == 1 then
                        return false, "closed"
                    end
                    return "v"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "closedClusterOff",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:get("k")
            ngx.say("result: ", tostring(res), " err: ", tostring(err))
            ngx.say("gets: ", gets)
        ';
    }
--- request
GET /t
--- response_body
result: nil err: closed
gets: 1
--- no_error_log
[error]

=== TEST 3: a new connection is not retried
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.reused = false
            fake.connect = function(self)
                self._reused = fake.reused and 1 or 0
                return 1
            end

            local gets = 0
            fake.handler = function(self, cmd)
                if cmd == "get" then
                    gets = gets + 1
                    if gets == 1 then
                        return false, "closed"
                    end
                    return "v"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "closedClusterNew",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                retry_on_closed_connection = true,
            }))

            local res, err = red:get("k")
            ngx.say("result: ", tostring(res), " err: ", tostring(err))
            ngx.say("gets: ", gets)
        ';
    }
--- request
GET /t
--- response_body
result: nil err: closed
gets: 1
--- no_error_log
[error]
