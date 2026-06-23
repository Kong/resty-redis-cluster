# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

no_long_string();

run_tests();

__DATA__

=== TEST 1: parses x.y.z version (standard redis)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local rediscluster = require "resty.rediscluster"
            local CRLF = string.char(13, 10)
            local server_info = "redis_version:7.0.5" .. CRLF .. "redis_mode:cluster" .. CRLF
            local v, err = rediscluster.parse_redis_version(server_info)
            if not v then
                ngx.say("err: ", err)
                return
            end
            ngx.say("version: ", v)
            ngx.say("ge_9: ", tostring(tonumber(v) >= 9.0))
        ';
    }
--- request
GET /t
--- response_body
version: 7.0
ge_9: false
--- no_error_log
[error]


=== TEST 2: parses x.y version (AWS ElastiCache Serverless Valkey, FTI-7672)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local rediscluster = require "resty.rediscluster"
            local CRLF = string.char(13, 10)
            local server_info = "redis_version:7.2" .. CRLF .. "redis_mode:cluster" .. CRLF
            local v, err = rediscluster.parse_redis_version(server_info)
            if not v then
                ngx.say("err: ", err)
                return
            end
            ngx.say("version: ", v)
            ngx.say("ge_9: ", tostring(tonumber(v) >= 9.0))
        ';
    }
--- request
GET /t
--- response_body
version: 7.2
ge_9: false
--- no_error_log
[error]


=== TEST 3: redis 8.x is accepted (not >= 9.0)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local rediscluster = require "resty.rediscluster"
            local CRLF = string.char(13, 10)
            local server_info = "redis_version:8.0.1" .. CRLF .. "redis_mode:cluster" .. CRLF
            local v, err = rediscluster.parse_redis_version(server_info)
            if not v then
                ngx.say("err: ", err)
                return
            end
            ngx.say("version: ", v)
            ngx.say("ge_9: ", tostring(tonumber(v) >= 9.0))
        ';
    }
--- request
GET /t
--- response_body
version: 8.0
ge_9: false
--- no_error_log
[error]


=== TEST 4: redis 9.x triggers compatibility warning (>= 9.0)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local rediscluster = require "resty.rediscluster"
            local CRLF = string.char(13, 10)
            local server_info = "redis_version:9.0" .. CRLF .. "redis_mode:cluster" .. CRLF
            local v, err = rediscluster.parse_redis_version(server_info)
            if not v then
                ngx.say("err: ", err)
                return
            end
            ngx.say("version: ", v)
            ngx.say("ge_9: ", tostring(tonumber(v) >= 9.0))
        ';
    }
--- request
GET /t
--- response_body
version: 9.0
ge_9: true
--- no_error_log
[error]
