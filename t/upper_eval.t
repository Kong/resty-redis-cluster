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

=== TEST 1: upper case EVAL reaches the eval path
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()
            fake.handler = function(self, cmd, ...)
                return "OK"
            end
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "probeEval",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))
            for _, name in ipairs({ "eval", "EVAL" }) do
                local before = #fake.events
                local res, err = red[name](red, "return redis.call('get', KEYS[1])", 1, "foo")
                for i = before + 1, #fake.events do
                    local e = fake.events[i]
                    if e.ev == "cmd" then
                        local a = {}
                        for j, v in ipairs(e.args or {}) do a[j] = tostring(v) end
                        ngx.say(name, " -> sent: ", e.cmd, "(", table.concat(a, " , "), ") on ", e.node)
                    end
                end
            end
        }
    }
--- request
GET /t
--- response_body
eval -> sent: eval(return redis.call('get', KEYS[1]) , 1 , foo) on 127.0.0.1:7003
EVAL -> sent: eval(return redis.call('get', KEYS[1]) , 1 , foo) on 127.0.0.1:7003
--- no_error_log
[error]
