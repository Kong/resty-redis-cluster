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

=== TEST 1: the library pool name masks the credential
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "logMasked",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                username = "default",
                password = "kong",
            }))
            ngx.say("ok")
        ';
    }
--- request
GET /t
--- response_body
ok
--- error_log
set pool name: 127.0.0.1:7001:nil:nil::default:<credential>

=== TEST 2: a caller pool name is not masked
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake
            fake.reset()

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "logUserPool",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                connect_opts = { pool = "my-user-pool" },
            }))
            ngx.say("ok")
        ';
    }
--- request
GET /t
--- response_body
ok
--- error_log
set pool name: 127.0.0.1:7001:my-user-pool
