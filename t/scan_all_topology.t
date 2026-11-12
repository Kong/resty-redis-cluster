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

=== TEST 1: scan_all does not notice a new master on its own
This records the current behavior. The slot cache changes only when something calls refresh_slots.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "scan" then
                    return { "0", { "key@" .. self._node } }
                end
                return "OK"
            end
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "staleScan",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            -- The cluster now serves slots 10923-16383 from 7013. Nothing in this
            -- worker calls refresh_slots: it only scans.
            fake.topology = 7
            local seen = {}
            for round = 1, 3 do
                local cursor, all = "0", {}
                repeat
                    local res, err = red:scan_all(cursor, "COUNT", 10)
                    if not res then ngx.say("error: ", err) return end
                    cursor = res[1]
                    for _, k in ipairs(res[2]) do all[#all + 1] = k end
                until cursor == "0"
                table.sort(all)
                ngx.say("round ", round, ": ", table.concat(all, " "))
            end
            local refreshes = -1
            for _, e in ipairs(fake.events) do
                if e.ev == "cluster-slots" then refreshes = refreshes + 1 end
            end
            ngx.say("slot refreshes: ", refreshes)
        }
    }
--- request
GET /t
--- response_body
round 1: key@127.0.0.1:7001 key@127.0.0.1:7002 key@127.0.0.1:7003
round 2: key@127.0.0.1:7001 key@127.0.0.1:7002 key@127.0.0.1:7003
round 3: key@127.0.0.1:7001 key@127.0.0.1:7002 key@127.0.0.1:7003
slot refreshes: 0
--- no_error_log
[error]
