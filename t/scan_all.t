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

=== TEST 1: one call visits every master and ends with cursor 0
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, cursor)
                if cmd ~= "scan" then
                    return "OK"
                end
                if self._node == "127.0.0.1:7001" then
                    return { "0", { "k1", "k2" } }
                elseif self._node == "127.0.0.1:7002" then
                    return { "0", { "k3" } }
                elseif self._node == "127.0.0.1:7003" then
                    return { "0", { "k4" } }
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllOneCall",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor, keys, err = red:scan_all("0")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("cursor: ", tostring(cursor), " err: ", tostring(err))
            ngx.say("keys: ", table.concat(keys or {}, ","))
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
cursor: 0 err: nil
keys: k1,k2,k3,k4
scans: 3
--- no_error_log
[error]

=== TEST 2: COUNT stops the call and the next call continues on the same master
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, cursor)
                if cmd ~= "scan" then
                    return "OK"
                end
                if self._node == "127.0.0.1:7001" then
                    if cursor == "0" then
                        return { "7", { "k1" } }
                    end
                    return { "0", { "k2" } }
                elseif self._node == "127.0.0.1:7002" then
                    return { "0", { "k3" } }
                end
                return { "0", { "k4" } }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllCount",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor = "0"
            local out = {}
            for i = 1, 6 do
                local keys
                cursor, keys = red:scan_all(cursor, "COUNT", "1")
                out[#out + 1] = cursor .. ":" .. table.concat(keys or {}, "+")
                if cursor == "0" then
                    break
                end
            end
            ngx.say(table.concat(out, " "))
        ';
    }
--- request
GET /t
--- response_body
1:3:7:k1 2:3:0:k2 3:3:0:k3 0:k4
--- no_error_log
[error]

=== TEST 3: an empty page with a non-zero cursor keeps the iteration moving
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, cursor)
                if cmd ~= "scan" then
                    return "OK"
                end
                if self._node == "127.0.0.1:7001" then
                    if cursor == "0" then
                        return { "5", {} }
                    end
                    return { "0", { "k1" } }
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllEmptyPage",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor, keys = red:scan_all("0")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("cursor: ", tostring(cursor))
            ngx.say("keys: ", table.concat(keys or {}, ","))
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
cursor: 0
keys: k1
scans: 4
--- no_error_log
[error]

=== TEST 4: the SCAN options reach every master
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd ~= "scan" then
                    return "OK"
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllOptions",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            assert(red:scan_all("0", "MATCH", "p:*", "COUNT", "10"))

            local seen = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    local args = {}
                    for i = 1, #e.args do
                        args[i] = tostring(e.args[i])
                    end
                    seen[#seen + 1] = table.concat(args, ",")
                end
            end
            table.sort(seen)
            ngx.say("args: ", table.concat(seen, " "))
        ';
    }
--- request
GET /t
--- response_body
args: 0,MATCH,p:*,COUNT,10 0,MATCH,p:*,COUNT,10 0,MATCH,p:*,COUNT,10
--- no_error_log
[error]

=== TEST 5: an invalid cursor returns an error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd ~= "scan" then
                    return "OK"
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllBadCursor",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor, err = red:scan_all("abc")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("cursor: ", tostring(cursor))
            ngx.say("err: ", tostring(err))
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
cursor: nil
err: invalid scan cursor: abc
scans: 0
--- no_error_log
[error]

=== TEST 6: a cursor from a cluster with another master count is rejected
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd ~= "scan" then
                    return "OK"
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllStaleCursor",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor, err = red:scan_all("1:5:0")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("cursor: ", tostring(cursor))
            ngx.say("err: ", tostring(err))
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
cursor: nil
err: scan cursor is stale: the cluster has 3 masters now and the cursor was made with 5; restart with cursor 0
scans: 0
--- no_error_log
[error]
