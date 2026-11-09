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

=== TEST 1: a failed call refreshes the master list, so the next call uses the new node
The fake cluster starts with 7001, 7002 and 7003. When 7001 cannot be
reached, the topology switches to the one that has 7013 in place of 7003.
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
                    -- The node failed over while it was away.
                    fake.topology = 7
                    return nil, "connection refused"
                end
                return 1
            end
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return { "0", { self._node } }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllFailover",
                serv_list = { { ip = "127.0.0.1", port = 7002 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local first, first_err = red:scan_all("0", "COUNT", "100")
            dead = false
            local second, second_err = red:scan_all("0", "COUNT", "100")

            local scanned = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "scan" then
                    scanned[e.node] = true
                end
            end
            local nodes = {}
            for node in pairs(scanned) do
                nodes[#nodes + 1] = node
            end
            table.sort(nodes)

            ngx.say("first ok: ", tostring(first ~= nil))
            ngx.say("first err starts with failed to connect: ",
                    tostring(tostring(first_err):find("failed to connect", 1, true) ~= nil))
            ngx.say("second err: ", tostring(second_err))
            ngx.say("second keys: ", second and #second[2] or "nil")
            ngx.say("scanned: ", table.concat(nodes, " "))
        ';
    }
--- request
GET /t
--- response_body
first ok: false
first err starts with failed to connect: true
second err: nil
second keys: 3
scanned: 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7013
--- no_error_log
[error]

=== TEST 2: a SCAN error also refreshes the master list
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return nil, "timeout"
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllScanError",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
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
            local after = slot_fetches()

            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
            ngx.say("slot cache refreshed: ", tostring(after > before))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: timeout
slot cache refreshed: true
--- no_error_log
[error]

=== TEST 3: each failed call refreshes the master list once
Two calls fail in a row, so the cache is fetched twice in total and not
once for each master that the first call tries.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.connect = function(self, host, port)
                if host .. ":" .. tostring(port) == "127.0.0.1:7002" then
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
                name = "scanAllTwoFailures",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
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

            local r1, e1 = red:scan_all("0", "COUNT", "10")
            local middle = slot_fetches()
            local r2, e2 = red:scan_all("0", "COUNT", "10")
            local after = slot_fetches()

            ngx.say("calls failed: ", tostring(r1 == nil and r2 == nil))
            ngx.say("errs equal: ", tostring(e1 == e2))
            ngx.say("fetches in first call: ", middle - before)
            ngx.say("fetches in second call: ", after - middle)
        ';
    }
--- request
GET /t
--- response_body
calls failed: true
errs equal: true
fetches in first call: 1
fetches in second call: 1
--- no_error_log
[error]

=== TEST 4: a failed refresh stops at the time bound, not after every address
The fake client refuses at once, so this cannot measure seconds. It measures
the behavior that makes the bound work: the walk over the address list stops
early, and the error names the bound.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return { "0", {} }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scanAllRefreshBound",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            red:init_slots()

            -- Fill the cached address list with nodes that do not answer, so a
            -- refresh has to walk all of them. The elapsed time crosses the
            -- bound after the first attempt.
            fake.connect = function(self, host, port)
                self._node = host .. ":" .. tostring(port)
                -- Move the clock past the bound on the first attempt.
                ngx.sleep(0.02)
                return nil, "timeout"
            end

            local function connects()
                local n = 0
                for _, e in ipairs(fake.events) do
                    if e.ev == "connect" then
                        n = n + 1
                    end
                end
                return n
            end

            -- Without a bound the walk tries every address three times.
            red.config.max_connection_timeout = nil
            fake.events = {}
            red:refresh_slots()
            local unbounded = connects()

            -- With a bound of 1 ms it stops after the first attempt.
            red.config.max_connection_timeout = 1
            fake.events = {}
            local ok, err = red:refresh_slots()
            local bounded = connects()

            ngx.say("the unbounded walk tried more than one address: ",
                    tostring(unbounded > 1))
            ngx.say("the bounded walk stopped earlier: ",
                    tostring(bounded < unbounded))
            ngx.say("refresh ok: ", tostring(ok ~= nil))
            ngx.say("err names the bound: ",
                    tostring(tostring(err):find("max_connection_timeout", 1, true) ~= nil))
        ';
    }
--- request
GET /t
# This block exercises the failure path, which logs at error level by design.
--- response_body
the unbounded walk tried more than one address: true
the bounded walk stopped earlier: true
refresh ok: false
err names the bound: true
--- no_error_log
[alert]

=== TEST 5: a refresh passes a bound even when the caller sets none
The default is what protects a caller that never sets max_connection_timeout,
which is every caller today. Assert the value reaches fetch_slots, so a change
that drops the default fails here. The first fetch must stay unbounded: a
client that cannot read the slots once cannot start at all.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "scan" then
                    return { "0", {} }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"

            -- new() runs the first fetch, so record what it passes there.
            local init_opts, init_called = nil, false
            local real_fetch = redis.fetch_slots
            redis.fetch_slots = function(self, opts)
                init_called = true
                init_opts = opts and opts.max_connection_timeout
                return real_fetch(self, opts)
            end

            local red = assert(redis:new({
                name = "scanAllRefreshDefault",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            redis.fetch_slots = real_fetch

            -- Now record what a refresh passes down.
            local refresh_opts
            red.fetch_slots = function(self, opts)
                refresh_opts = opts and opts.max_connection_timeout
                return nil, "stopped by the test"
            end
            red:refresh_slots()

            ngx.say("the first fetch ran: ", tostring(init_called))
            ngx.say("the first fetch passes no bound: ", tostring(init_opts == nil))
            ngx.say("a refresh passes a bound: ", tostring(type(refresh_opts) == "number"))
        ';
    }
--- request
GET /t
# This block exercises the failure path, which logs at error level by design.
--- response_body
the first fetch ran: true
the first fetch passes no bound: true
a refresh passes a bound: true
--- no_error_log
[alert]
