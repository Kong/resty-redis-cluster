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
