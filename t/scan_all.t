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
                local index = cursor == "0" and "done" or cursor:match("^(%d+):")
                out[#out + 1] = index .. ":" .. table.concat(keys or {}, "+")
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
1:k1 2:k2 3:k3 done:k4
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

            local cursor, keys, err = red:scan_all("abc")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("cursor: ", tostring(cursor), " keys: ", tostring(keys))
            ngx.say("err: ", tostring(err))
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
cursor: nil keys: nil
err: invalid scan cursor: abc
scans: 0
--- no_error_log
[error]

=== TEST 6: a cursor with another master fingerprint is rejected
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

            local cursor, keys, err = red:scan_all("1:3:0-0:0")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("cursor: ", tostring(cursor), " keys: ", tostring(keys))
            ngx.say("err: ", tostring(err))
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
cursor: nil keys: nil
err: scan cursor is stale: the set of masters changed; restart with cursor 0
scans: 0
--- no_error_log
[error]

=== TEST 7: without COUNT one call stops at the default key budget
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- Every SCAN returns ten keys and a non-zero cursor: without a cap,
            -- this would walk the whole keyspace in one call.
            fake.handler = function(self, cmd, cursor)
                if cmd ~= "scan" then
                    return "OK"
                end
                local next_cursor = tostring((tonumber(cursor) or 0) + 1)
                local page = {}
                for i = 1, 10 do
                    page[i] = self._node .. ":" .. next_cursor .. ":" .. i
                end
                return { next_cursor, page }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllBudget",
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
            ngx.say("keys: ", #keys, " scans: ", scans, " done: ", tostring(cursor == "0"))
        ';
    }
--- request
GET /t
--- response_body
keys: 100 scans: 10 done: false
--- no_error_log
[error]

=== TEST 8: a sparse MATCH stops at the round-trip cap
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- No key ever matches, and the cursor never returns to 0.
            fake.handler = function(self, cmd, cursor)
                if cmd ~= "scan" then
                    return "OK"
                end
                return { "42", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllSparse",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor, keys = red:scan_all("0", "MATCH", "nothing:*")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("keys: ", #keys, " scans: ", scans, " done: ", tostring(cursor == "0"))
            ngx.say("index: ", cursor and cursor:match("^(%d+):") or "nil")
        ';
    }
--- request
GET /t
--- response_body
keys: 0 scans: 16 done: false
index: 1
--- no_error_log
[error]

=== TEST 9: a reordered master list keeps the cursor valid
Version 6 lists the same three masters as version 1, in the opposite order.
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
                if cursor == "0" then
                    return { "7", { self._node .. ":first" } }
                end
                return { "0", { self._node .. ":last" } }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllReorder",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor, keys = red:scan_all("0", "COUNT", "1")
            ngx.say("first: ", table.concat(keys or {}, ","))
            ngx.say("index: ", cursor and cursor:match("^(%d+):") or "nil")

            -- The slot map now reports the same masters in another order.
            fake.topology = 6
            red:refresh_slots()

            local cursor2, keys2, err2 = red:scan_all(cursor, "COUNT", "1")
            ngx.say("err: ", tostring(err2))
            ngx.say("second: ", table.concat(keys2 or {}, ","))
        ';
    }
--- request
GET /t
--- response_body
first: 127.0.0.1:7001:first
index: 1
err: nil
second: 127.0.0.1:7001:last
--- no_error_log
[error]

=== TEST 10: a changed master with the same count is rejected
Version 7 has three masters, but 7013 replaces 7003.
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
                if cursor == "0" then
                    return { "7", { self._node .. ":first" } }
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllReplace",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local cursor = red:scan_all("0", "COUNT", "1")

            -- Same count, but one master is replaced.
            fake.topology = 7
            red:refresh_slots()

            local cursor2, keys2, err2 = red:scan_all(cursor, "COUNT", "1")
            ngx.say("cursor: ", tostring(cursor2), " keys: ", tostring(keys2))
            ngx.say("err: ", tostring(err2))
        ';
    }
--- request
GET /t
--- response_body
cursor: nil keys: nil
err: scan cursor is stale: the set of masters changed; restart with cursor 0
--- no_error_log
[error]

=== TEST 11: a master that cannot be reached refreshes the slot cache
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- 7001 owns slots 0-5460 and is down; the configured seed node 7002 works.
            fake.connect = function(self, host, port)
                if host .. ":" .. tostring(port) == "127.0.0.1:7001" then
                    return nil, "connection refused"
                end
                return 1
            end
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return { "0", {} }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllDeadMaster",
                serv_list = { { ip = "127.0.0.1", port = 7002 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local function slot_fetches()
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == "cluster-slots" then
                        n = n + 1
                    end
                end
                return n
            end

            red:init_slots()
            local before = slot_fetches()

            local c, keys, err = red:scan_all("0", "COUNT", "10")
            local after = slot_fetches()

            ngx.say("cursor: ", tostring(c))
            ngx.say("err starts with failed to connect: ", tostring(tostring(err):find("failed to connect", 1, true) ~= nil))
            ngx.say("slot cache refreshed: ", tostring(after > before))
        ';
    }
--- request
GET /t
--- response_body
cursor: nil
err starts with failed to connect: true
slot cache refreshed: true
--- no_error_log
[error]

=== TEST 12: one failing call refreshes the slot cache once
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.connect = function(self, host, port)
                if host .. ":" .. tostring(port) == "127.0.0.1:7001" then
                    return nil, "connection refused"
                end
                return 1
            end
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return { "0", {} }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllOneRefresh",
                serv_list = { { ip = "127.0.0.1", port = 7002 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local function slot_fetches()
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == "cluster-slots" then
                        n = n + 1
                    end
                end
                return n
            end

            red:init_slots()
            local before = slot_fetches()

            -- Three masters fail on the first page, but the call returns on the
            -- first failure, so the cache is fetched once.
            local c = red:scan_all("0", "COUNT", "10")
            local after = slot_fetches()

            ngx.say("cursor: ", tostring(c))
            ngx.say("slot fetches in one call: ", after - before)
        ';
    }
--- request
GET /t
--- response_body
cursor: nil
slot fetches in one call: 1
--- no_error_log
[error]

=== TEST 13: a later call succeeds after the failed one refreshed the cache
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            local dead = true
            fake.connect = function(self, host, port)
                if dead and host .. ":" .. tostring(port) == "127.0.0.1:7001" then
                    return nil, "connection refused"
                end
                return 1
            end
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return { "0", { self._node .. ":k" } }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllRecover",
                serv_list = { { ip = "127.0.0.1", port = 7002 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local c1, k1, e1 = red:scan_all("0", "COUNT", "100")

            dead = false
            local c2, k2, e2 = red:scan_all("0", "COUNT", "100")

            ngx.say("first err: ", tostring(e1) ~= "nil")
            ngx.say("second err: ", tostring(e2))
            ngx.say("second keys: ", k2 and #k2 or "nil")
        ';
    }
--- request
GET /t
--- response_body
first err: true
second err: nil
second keys: 3
--- no_error_log
[error]

