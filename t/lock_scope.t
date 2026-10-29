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

=== TEST 1: the slot locks belong to one worker
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            -- White-box test: it pins the lock key contract. A cross-worker test needs
            -- `worker_processes 2` and a slow hook; run it manually.
            local keys = {}
            local fake_lock = {
                new = function()
                    return {
                        lock = function(self, key)
                            keys[#keys + 1] = key
                            return 0
                        end,
                        unlock = function(self)
                            return 1
                        end,
                    }
                end,
            }

            local fake_redis = require "fake_redis"
            package.loaded["resty.redis"] = fake_redis
            package.loaded["resty.lock"] = fake_lock

            local worker_id = ngx.worker.id()
            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "scopeCluster",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:refresh_slots()

            ngx.say("init lock has worker: ",
                    tostring(keys[1] == "redis_cluster_slot_scopeCluster_" .. worker_id))
            ngx.say("refresh lock has worker: ",
                    tostring(keys[2] == "refresh_lock" .. worker_id))
        ';
    }
--- request
GET /t
--- response_body
init lock has worker: true
refresh lock has worker: true
--- no_error_log
[error]


=== TEST 2: a process without a worker id still gets a lock key
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local keys = {}
            local fake_lock = {
                new = function()
                    return {
                        lock = function(self, key)
                            keys[#keys + 1] = key
                            return 0
                        end,
                        unlock = function(self)
                            return 1
                        end,
                    }
                end,
            }

            local fake_redis = require "fake_redis"
            package.loaded["resty.redis"] = fake_redis
            package.loaded["resty.lock"] = fake_lock

            -- The master process and a privileged agent have no worker id.
            ngx.worker.id = function() return nil end
            local pid = ngx.worker.pid()

            local redis = require "resty.rediscluster"
            local ok, red = pcall(function()
                return assert(redis:new({
                    name = "noWorkerCluster",
                    serv_list = { { ip = "127.0.0.1", port = 7001 } },
                    dict_name = "redis_cluster_slot_locks",
                    lock_timeout = 1,
                }))
            end)

            ngx.say("new ok: ", tostring(ok))
            if not ok then
                return
            end

            red:refresh_slots()
            ngx.say("init lock has the pid: ",
                    tostring(keys[1] == "redis_cluster_slot_noWorkerCluster_p" .. pid))
            ngx.say("refresh lock has the pid: ",
                    tostring(keys[2] == "refresh_lockp" .. pid))
        ';
    }
--- request
GET /t
--- response_body
new ok: true
init lock has the pid: true
refresh lock has the pid: true
--- no_error_log
[error]
