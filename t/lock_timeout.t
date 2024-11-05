# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

my $redis_auth = $ENV{REDIS_AUTH};
if (defined($redis_auth) && $redis_auth eq "no") {
    plan tests => repeat_each() * (5 * blocks());
} else {
    plan(skip_all => "skip when REDIS_AUTH is enabled");
}

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};


no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: return detailed error message
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '

            local config = {
                            name = "testCluster",                   -- rediscluster name
                            serv_list = {                           -- redis cluster node list(host and port), invalid address
                                            { ip = "10.255.255.100", port = 6371 },
                                        },
                            keepalive_timeout = 60000,              -- redis connection pool idle timeout
                            keepalive_cons = 1000,                  -- redis connection pool size
                            connect_timeout = 1000,                 -- timeout while connecting
                            read_timeout = 1000,                    -- timeout while reading
                            send_timeout = 1000,                    -- timeout while sending
                            lock_timeout = 0,                       -- timeout while acquiring resty.lock
                            max_redirection = 5                     -- maximum retry attempts for redirection

            }

            local rc = require "resty.rediscluster"
            local spawn = ngx.thread.spawn
            local wait = ngx.thread.wait
            local say = ngx.say

            local function initialize(i)
              local red, err = rc:new(config)

              if err then
                return i .. ":" .. err
              end

              return "ok"
            end

            local num = 3
            local threads = {}
            for i = 1, num do
              table.insert(threads, spawn(initialize, i))
            end
            for i = 1, num do
              local tok, res = wait(threads[i])

              if not tok then
                say("failed to run thread ", i)

              else
                say(res)
              end
            end
        ';
    }
--- request
GET /t
--- response_body
1:failed to fetch slots: timeout;timeout;timeout
2:failed to acquire the lock in initializing slot cache: timeout
3:failed to acquire the lock in initializing slot cache: timeout
--- error_log
unable to connect ip:port 10.255.255.100:6371, attempt 1
unable to connect ip:port 10.255.255.100:6371, attempt 2
unable to connect ip:port 10.255.255.100:6371, attempt 3
