use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "yes") {
    plan tests => 3 * blocks();
} else {
    plan(skip_all => "skip when REDIS_AUTH is not enabled");
}

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: a real password rotation recovers the connection
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local MASTERS = { 6371, 6372, 6373 }
            local OLD = "rotation-old"
            local NEW = "rotation-new"
            local user = "rot-" .. tostring(ngx.now()):gsub("%.", "")

            local resty_redis = require "resty.redis"

            local function on_each_master(cmd, ...)
                for i = 1, #MASTERS do
                    local admin = assert(resty_redis:new())
                    assert(admin:connect("127.0.0.1", MASTERS[i]))
                    assert(admin:auth("kong"))
                    local ok, err = admin[cmd](admin, ...)
                    assert(ok, err)
                    admin:set_keepalive()
                end
            end

            -- Create the same ACL user on every master.
            on_each_master("acl", "setuser", user, "on", ">" .. OLD, "~*", "+@all")

            local calls = 0
            local old_base

            local cfg = {
                name = "rotationTest",
                serv_list = {
                    { ip = "127.0.0.1", port = 6371 },
                    { ip = "127.0.0.1", port = 6372 },
                    { ip = "127.0.0.1", port = 6373 },
                },
                username = user,
                password = OLD,
            }

            -- Default force_auth: a new connection sends AUTH once.
            cfg.auth_config_reconfigurer = function(self, err)
                calls = calls + 1
                old_base = self.config.base_pool
                self.config.password = NEW
                cfg.password = NEW
                return true
            end

            -- Warm up with the default user. A different credential means a different
            -- pool name, so the tested user starts with an empty pool.
            local warm = assert(require("resty.rediscluster"):new({
                name = "rotationWarm",
                serv_list = cfg.serv_list,
                username = "default",
                password = "kong",
            }))
            assert(warm:set("rotation-key", "value"))

            -- Rotate the password on every master.
            on_each_master("acl", "setuser", user, "resetpass", ">" .. NEW)

            local red = assert(require("resty.rediscluster"):new(cfg))
            ngx.say("hook calls after new: ", calls)
            ngx.say("pool changed: ", tostring(old_base ~= nil and cfg.base_pool ~= old_base))

            local v = red:get("rotation-key")
            ngx.say("value: ", tostring(v))
            ngx.say("hook calls after get: ", calls)

            on_each_master("acl", "deluser", user)
        ';
    }
--- request
GET /t
--- response_body
hook calls after new: 1
pool changed: true
value: value
hook calls after get: 1
--- no_error_log
[error]
