=begin comment
REDIS_AUTH=no PATH="/usr/local/openresty/nginx/sbin:$PATH" prove -v t/z-recover.t
=end comment
=cut

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "no") {
    plan tests => repeat_each() * (4 * blocks());
} else {
    plan(skip_all => "skip when REDIS_AUTH is enabled");
}

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;$pwd/t/fixtures/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();
no_shuffle();
run_tests();

__DATA__

=== TEST 1: recover after cluster up again
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {

            local compose = require "compose"

            -- ensure Redis 6 up
            local cok, sig, code = compose.updown("up")
            if not cok then
                ngx.say("failed to start cluster: ", sig, " ", code)
                return
            end
            ngx.say("up: ", cok)

            local config = {
                            name      = "testCluster",                           --rediscluster name
                            serv_list = {                                        --redis cluster node list(host and port),
                                            { ip = "127.0.0.1", port = 6371 },
                                            { ip = "127.0.0.1", port = 6372 },
                                            { ip = "127.0.0.1", port = 6373 },
                                            { ip = "127.0.0.1", port = 6374 },
                                            { ip = "127.0.0.1", port = 6375 },
                                            { ip = "127.0.0.1", port = 6376 }
                                        },
                            keepalive_timeout = 60000,              --redis connection pool idle timeout
                            keepalive_cons    = 1000,               --redis connection pool size
                            connect_timeout   = 1000,               --timeout while connecting
                            read_timeout      = 1000,               --timeout while reading
                            send_timeout      = 1000,               --timeout while sending
                            max_redirection   = 5                   --maximum retry attempts for redirection

            }
            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            if err then
                ngx.say("failed to create redis cluster client: ", err)
                return
            end

            local ok
            ok, err = red:flushall()
            if not ok then
                ngx.say("failed to flush all: ", err)
                return
            end

            local res
            res, err = red:hmset("fruits", { apple = "small", banana = "long", watermelon = "large" })
            if not res then
                ngx.say("failed to set hm fruits: ", err)
                return
            end
            ngx.say("hmset fruits: ", res)

            red:init_pipeline()
            red:hgetall("fruits")
            res, err = red:commit_pipeline()
            if err then
                ngx.say("failed to get hm fruits: ", err)
                return
            end
            if not res then
                ngx.say("hm fruits not found.")
                return
            end
            if next(res) ~= nil then
                res = red:array_to_hash(res[1])
            end
            ngx.say("apple: ", res.apple)
            ngx.say("banana: ", res.banana)
            ngx.say("watermelon: ", res.watermelon)

            -- ensure Redis 6 down
            cok, sig, code = compose.updown("down")
            if not cok then
                ngx.say("failed to stop cluster: ", sig, " ", code)
                return
            end
            ngx.say("down: ", cok)

            red:init_pipeline()
            red:hgetall("fruits")
            res, err = red:commit_pipeline()
            if err then
                ngx.say("failed to get hm fruits: ", err)
            end

            -- ensure Redis 7 up
            cok, sig, code = compose.updown("up", true)
            if not cok then
                ngx.say("failed to stop cluster: ", sig, " ", code)
                return
            end
            ngx.say("retry up: ", cok)

            res, err = red:hmset("fruits", { apple = "small", banana = "long", watermelon = "large" })
            if not res then
                ngx.say("failed to set hm fruits: ", err)
                return
            end
            ngx.say("hmset fruits: ", res)

            red:init_pipeline()
            red:hgetall("fruits")
            res, err = red:commit_pipeline()
            if err then
                ngx.say("failed to get hm fruits: ", err)
                return
            end
            if not res then
                ngx.say("hm fruits not found.")
                return
            end
            if next(res) ~= nil then
                res = red:array_to_hash(res[1])
            end
            ngx.say("apple: ", res.apple)
            ngx.say("banana: ", res.banana)
            ngx.say("watermelon: ", res.watermelon)

            -- ensure Redis 7 down
            cok, sig, code = compose.updown("down", true)
            if not cok then
                ngx.say("failed to stop cluster: ", sig, " ", code)
                return
            end
            ngx.say("retry down: ", cok)

            red:init_pipeline()
            red:hgetall("fruits")
            res, err = red:commit_pipeline()
            if err then
                ngx.say("failed to get hm fruits: ", err)
            end

            -- ensure Redis 6 up
            cok, sig, code = compose.updown("up")
            if not cok then
                ngx.say("failed to stop cluster: ", sig, " ", code)
                return
            end
            ngx.say("restore up: ", cok)
        }
    }
--- request
GET /t
--- error_code: 200
--- response_body_like
^up: true
hmset fruits: OK
apple: small
banana: long
watermelon: large
down: true
failed to get hm fruits: failed to connect, err: [1-9][0-9.:]+ timeout
retry up: true
hmset fruits: OK
apple: small
banana: long
watermelon: large
retry down: true
failed to get hm fruits: failed to connect, err: [1-9][0-9.:]+ timeout,[[:alnum:].:-]+ no resolver defined to resolve\040"redis-\d"
restore up: true$
--- error_log eval
["tcp socket connect timed out", qr/unable to connect ip:port [[:alnum:].:-]+, attempt \d, error: timeout/]
--- wait: 1
--- timeout: 240s
