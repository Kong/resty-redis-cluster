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

=== TEST 1: a stored value that starts with MOVED, read by a cross-slot MGET
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    -- "z" lives on 7002. Its value is plain user data.
                    if keys[1] == "z" then out[1] = "MOVED to Paris" end
                    return out
                end
                return "OK"
            end
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "probeMoved",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            local res, err = red:mget("b", "z", "a")
            local mgets, refreshes = 0, -1
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then mgets = mgets + 1 end
                if e.ev == "cluster-slots" then refreshes = refreshes + 1 end
            end
            ngx.say("values: ", table.concat(res or {}, " | "), " err: ", tostring(err))
            ngx.say("mget commands sent: ", mgets, " slot refreshes: ", refreshes)
        }
    }
--- request
GET /t
--- response_body
values: v:b | MOVED to Paris | v:a err: nil
mget commands sent: 3 slot refreshes: 0
--- no_error_log
[error]


=== TEST 2: a stored value that looks like an ASK reply, read by a cross-slot MGET
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    if self._node == "evil.example:6379" then
                        return { "ATTACKER-CONTROLLED" }
                    end
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    if keys[1] == "z" then out[1] = "ASK 0 evil.example:6379" end
                    return out
                end
                return "OK"
            end
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "probeAsk",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                password = "s3cret",
            }))
            local res, err = red:mget("b", "z", "a")
            local evil, auth_sent = 0, false
            for _, e in ipairs(fake.events) do
                if e.node == "evil.example:6379" then
                    evil = evil + 1
                    if e.ev == "cmd" and e.cmd == "auth" then auth_sent = true end
                end
            end
            ngx.say("values: ", table.concat(res or {}, " | "), " err: ", tostring(err))
            ngx.say("events on evil.example:6379: ", evil, " auth sent: ", tostring(auth_sent))
        }
    }
--- request
GET /t
--- response_body
values: v:b | ASK 0 evil.example:6379 | v:a err: nil
events on evil.example:6379: 0 auth sent: false
--- no_error_log
[error]


=== TEST 3: a stored value that starts with CLUSTERDOWN, read by a cross-slot MGET
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    if keys[1] == "z" then out[1] = "CLUSTERDOWN is a fine band name" end
                    return out
                end
                return "OK"
            end
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "probeDown",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            local res, err = red:mget("b", "z", "a")
            ngx.say("values: ", table.concat(res or {}, " | "), " err: ", tostring(err))
        }
    }
--- request
GET /t
--- response_body
values: v:b | CLUSTERDOWN is a fine band name | v:a err: nil
--- no_error_log
[error]


=== TEST 4: the same ASK-looking value in a plain pipeline of GET
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()
            fake.handler = function(self, cmd, ...)
                if self._node == "evil.example:6379" then
                    return "ATTACKER-CONTROLLED"
                end
                if cmd == "get" then
                    return "ASK 0 evil.example:6379"
                end
                return "OK"
            end
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "probePipe",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
                password = "s3cret",
            }))
            red:init_pipeline()
            red:get("z")
            local res, err = red:commit_pipeline()
            local evil = 0
            for _, e in ipairs(fake.events) do
                if e.node == "evil.example:6379" then evil = evil + 1 end
            end
            ngx.say("result: ", tostring(res and res[1]), " err: ", tostring(err))
            ngx.say("events on evil.example:6379: ", evil)
        }
    }
--- request
GET /t
--- response_body
result: ASK 0 evil.example:6379 err: nil
events on evil.example:6379: 0
--- no_error_log
[error]
