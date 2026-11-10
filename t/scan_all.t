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

            local res, err = red:scan_all("0")
            local cursor, keys = res and res[1], res and res[2]
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
            local err
            for i = 1, 6 do
                local keys
                local page
                page, err = red:scan_all(cursor, "COUNT", "1")
                cursor, keys = page and page[1], page and page[2]
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

            local res = red:scan_all("0")
            local cursor, keys = res and res[1], res and res[2]
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

            local res, err = red:scan_all("abc")
            local cursor, keys = res and res[1], res and res[2]
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

            local res, err = red:scan_all("1:3:0:0")
            local cursor, keys = res and res[1], res and res[2]
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

            local res = red:scan_all("0")
            local cursor, keys = res and res[1], res and res[2]
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

            local res = red:scan_all("0", "MATCH", "nothing:*")
            local cursor, keys = res and res[1], res and res[2]
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

=== TEST 9: a MATCH value that reads COUNT is a value, not the option
The options are pairs of a keyword and a value. "COUNT" in the value slot of
MATCH must not be read as the keyword.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- Ten keys on every page and a cursor that never ends: the budget
            -- alone decides how many pages one call reads.
            fake.handler = function(self, cmd, cursor)
                if cmd ~= "scan" then
                    return "OK"
                end
                local next_cursor = tostring((tonumber(cursor) or 0) + 1)
                local page = {}
                for i = 1, 10 do
                    page[i] = "k" .. next_cursor .. ":" .. i
                end
                return { next_cursor, page }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllMatchCount",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local function scans()
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == "cmd" and e.cmd == "scan" then
                        n = n + 1
                    end
                end
                return n
            end

            local res = red:scan_all("0", "MATCH", "COUNT", "COUNT", "2")
            ngx.say("keys: ", #res[2], " scans: ", scans())

            fake.events = {}
            local num = red:scan_all("0", "MATCH", "COUNT", "COUNT", 2)
            ngx.say("numeric budget keys: ", #num[2], " scans: ", scans())
        ';
    }
--- request
GET /t
--- response_body
keys: 10 scans: 1
numeric budget keys: 10 scans: 1
--- no_error_log
[error]

=== TEST 10: a MATCH pattern that starts with COUNT is still a pattern
"count*" is not the keyword COUNT, so the budget after it must be used. This
is the control for TEST 9: the fix must not change a pattern that only looks
similar.
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
                local next_cursor = tostring((tonumber(cursor) or 0) + 1)
                local page = {}
                for i = 1, 10 do
                    page[i] = "count" .. next_cursor .. ":" .. i
                end
                return { next_cursor, page }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllCountPattern",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res = red:scan_all("0", "MATCH", "count*", "COUNT", "5")
            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("keys: ", #res[2], " scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
keys: 10 scans: 1
--- no_error_log
[error]

=== TEST 11: an unusable COUNT returns an error
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
                return { "0", { "k" } }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllBadCount",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:scan_all("0", "MATCH", "k*", "COUNT", "abc")
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))

            local scans = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scans = scans + 1
                end
            end
            ngx.say("scans: ", scans)
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: COUNT needs a number, got abc
scans: 0
--- no_error_log
[error]

=== TEST 12: a COUNT without a value returns an error
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
                return { "0", { "k" } }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllCountNoValue",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:scan_all("0", "COUNT", "abc")
            ngx.say("res is nil: ", tostring(res == nil))
            ngx.say("err: ", tostring(err))
        ';
    }
--- request
GET /t
--- response_body
res is nil: true
err: COUNT needs a number, got abc
--- no_error_log
[error]

=== TEST 13: a reordered master list keeps the cursor valid
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

            local res = red:scan_all("0", "COUNT", "1")
            local cursor, keys = res and res[1], res and res[2]
            ngx.say("first: ", table.concat(keys or {}, ","))
            ngx.say("index: ", cursor and cursor:match("^(%d+):") or "nil")

            -- The slot map now reports the same masters in another order.
            fake.topology = 6
            red:refresh_slots()

            local res2, err2 = red:scan_all(cursor, "COUNT", "1")
            local cursor2, keys2 = res2 and res2[1], res2 and res2[2]
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

=== TEST 14: a changed master with the same count is rejected
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

            local cursor = red:scan_all("0", "COUNT", "1")[1]

            -- Same count, but one master is replaced.
            fake.topology = 7
            red:refresh_slots()

            local res2, err2 = red:scan_all(cursor, "COUNT", "1")
            local cursor2, keys2 = res2 and res2[1], res2 and res2[2]
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

=== TEST 15: a master that cannot be reached refreshes the slot cache
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

            local res, err = red:scan_all("0", "COUNT", "10")
            local c = res and res[1]
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

=== TEST 16: one failing call refreshes the slot cache once
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
            local res = red:scan_all("0", "COUNT", "10")
            local c = res and res[1]
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

=== TEST 17: a later call succeeds after the failed one refreshed the cache
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

            local r1, e1 = red:scan_all("0", "COUNT", "100")

            dead = false
            local r2, e2 = red:scan_all("0", "COUNT", "100")
            local k2 = r2 and r2[2]

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

=== TEST 18: the result has the same shape as the single-node scan
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
                return { "0", { "k1" } }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllShape",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            -- Exactly what a caller does today with resty.redis scan.
            local res, err = red:scan_all("0", "COUNT", "10")
            local cursor = res and res[1]
            local keys = res and res[2] or {}

            ngx.say("result is a table: ", tostring(type(res) == "table"))
            ngx.say("cursor type: ", type(cursor))
            ngx.say("keys: ", #keys, " err: ", tostring(err))
            ngx.say("keys are not a string: ", tostring(type(keys) == "table"))
        ';
    }
--- request
GET /t
--- response_body
result is a table: true
cursor type: string
keys: 3 err: nil
keys are not a string: true
--- no_error_log
[error]


=== TEST 19: the cursor carries one crc32 of the sorted master list
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
                    return { "7", { "k1" } }
                end
                return { "0", {} }
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllFingerprint",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res = red:scan_all("0", "COUNT", "1")
            local cursor = res and res[1]

            local _, total, fingerprint = string.match(cursor or "", "^(%d+):(%d+):(%d+):")
            local expected = ngx.crc32_long(
                "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003")

            ngx.say("shape: ", tostring(cursor ~= nil and cursor:match("^%d+:%d+:%d+:%d+$") ~= nil))
            ngx.say("total: ", tostring(total))
            ngx.say("fingerprint: ", tostring(fingerprint))
            ngx.say("matches crc32: ", tostring(fingerprint == tostring(expected)))
        ';
    }
--- request
GET /t
--- response_body
shape: true
total: 3
fingerprint: 866892679
matches crc32: true
--- no_error_log
[error]

=== TEST 20: a command that Redis rejects does not refresh the slot cache
resty.redis reports a "-ERR" reply as `false, err` and a socket error as
`nil, err`. A rejected command says nothing about the topology, so the call
must return the error without a CLUSTER SLOTS walk on every retry.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- A real node answers -ERR for an odd option tail such as "MATCH"
            -- with no value.
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return false, "ERR syntax error"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllBadCommand",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            red:init_slots()

            local function fetches()
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == "cluster-slots" then
                        n = n + 1
                    end
                end
                return n
            end
            local function count_event(name)
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == name then
                        n = n + 1
                    end
                end
                return n
            end

            -- Count only what the bad calls do, not the first slot fetch.
            fake.events = {}
            local errs = {}
            for i = 1, 5 do
                local _, err = red:scan_all("0", "MATCH")
                errs[i] = tostring(err)
            end

            ngx.say("err: ", errs[1])
            ngx.say("slot fetches for 5 bad calls: ", fetches())
            -- The connection answered, so it belongs back in the pool.
            ngx.say("closed: ", count_event("close"), " pooled: ", count_event("keepalive"))
        ';
    }
--- request
GET /t
--- response_body
err: ERR syntax error
slot fetches for 5 bad calls: 0
closed: 0 pooled: 5
--- no_error_log
[error]

=== TEST 21: a cluster level error still refreshes the slot cache
CLUSTERDOWN is a "-ERR" reply too, so resty.redis reports it as `false`. The
cluster view may be stale, so this one must refresh. This is the guard against
treating every `false` reply as a bad command.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return false, "CLUSTERDOWN The cluster is down"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllClusterDown",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            red:init_slots()

            local function fetches()
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == "cluster-slots" then
                        n = n + 1
                    end
                end
                return n
            end

            local before = fetches()
            local res, err = red:scan_all("0", "COUNT", "10")
            local after = fetches()

            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
            ngx.say("slot cache refreshed: ", tostring(after > before))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: CLUSTERDOWN The cluster is down
slot cache refreshed: true
--- no_error_log
[error]

