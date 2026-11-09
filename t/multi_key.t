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

=== TEST 1: MGET splits the keys by slot and keeps the order of the reply
The fake cluster has one master for each third of the slot space.
"b" is in 7001, "z" is in 7002 and "a" is in 7003.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    return out
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMgetSplit",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("b", "z", "a")
            ngx.say("keys: ", table.concat(res or {}, ","), " err: ", tostring(err))

            local cmds = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    local args = {}
                    for i = 1, #e.args do
                        args[i] = tostring(e.args[i])
                    end
                    cmds[#cmds + 1] = e.node .. "=" .. table.concat(args, "+")
                end
            end
            table.sort(cmds)
            ngx.say("cmds: ", table.concat(cmds, " "))
        ';
    }
--- request
GET /t
--- response_body
keys: v:b,v:z,v:a err: nil
cmds: 127.0.0.1:7001=b 127.0.0.1:7002=z 127.0.0.1:7003=a
--- no_error_log
[error]

=== TEST 2: MGET with keys in one slot sends one command
All keys with the hash tag {s} share slot 3828.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    return out
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMgetOneSlot",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("{s}one", "{s}two", "{s}three")
            ngx.say("keys: ", table.concat(res or {}, ","), " err: ", tostring(err))

            local count, args = 0, {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    count = count + 1
                    args = e.args
                end
            end
            ngx.say("count: ", count, " args: ", table.concat(args, "+"), " node: ", "127.0.0.1:7001")
        ';
    }
--- request
GET /t
--- response_body
keys: v:{s}one,v:{s}two,v:{s}three err: nil
count: 1 args: {s}one+{s}two+{s}three node: 127.0.0.1:7001
--- no_error_log
[error]

=== TEST 3: MGET with one key sends one command
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    return { "v:" .. tostring(keys[1]) }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMgetSingle",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("b")
            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    count = count + 1
                end
            end
            ngx.say("keys: ", table.concat(res or {}, ","), " err: ", tostring(err), " count: ", count)
        ';
    }
--- request
GET /t
--- response_body
keys: v:b err: nil count: 1
--- no_error_log
[error]

=== TEST 4: MGET keeps a null value in its place
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        if keys[i] == "z" then
                            out[i] = ngx.null
                        else
                            out[i] = "v:" .. tostring(keys[i])
                        end
                    end
                    return out
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMgetNull",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res = assert(red:mget("b", "z", "a"))
            local second = res[2] == ngx.null and "null" or tostring(res[2])
            ngx.say("values: ", tostring(res[1]), ",", second, ",", tostring(res[3]), " count: ", #res)
        ';
    }
--- request
GET /t
--- response_body
values: v:b,null,v:a count: 3
--- no_error_log
[error]

=== TEST 5: DEL and EXISTS split by slot and add the counts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "del" or cmd == "exists" then
                    return 1
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkDelExists",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local deleted, derr = red:del("b", "z", "a")
            local present, eerr = red:exists("b", "z", "a")
            ngx.say("del: ", tostring(deleted), " err: ", tostring(derr))
            ngx.say("exists: ", tostring(present), " err: ", tostring(eerr))

            local del_count, exists_count = 0, 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "del" then
                    del_count = del_count + 1
                elseif e.ev == "cmd" and e.cmd == "exists" then
                    exists_count = exists_count + 1
                end
            end
            ngx.say("commands: del=", del_count, " exists=", exists_count)
        ';
    }
--- request
GET /t
--- response_body
del: 3 err: nil
exists: 3 err: nil
commands: del=3 exists=3
--- no_error_log
[error]

=== TEST 6: a cross-slot multi-key command inside a pipeline returns an error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkPipelineMget",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:init_pipeline()
            local res, err = red:mget("b", "z")
            red:cancel_pipeline()

            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    count = count + 1
                end
            end
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
            ngx.say("sent: ", count)
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: command 'mget' needs one slot inside a pipeline; call it directly or use a hash tag
sent: 0
--- no_error_log
[error]

=== TEST 7: a multi-key command in one slot can still use a pipeline
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    return out
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkPipelineOneSlot",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            red:init_pipeline()
            local queued, qerr = red:mget("{s}one", "{s}two")
            local res, err = red:commit_pipeline()

            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    count = count + 1
                end
            end
            ngx.say("queued: ", tostring(queued), " ", tostring(qerr))
            ngx.say("err: ", tostring(err))
            ngx.say("values: ", res and table.concat(res[1], ",") or "nil")
            ngx.say("sent: ", count)
        ';
    }
--- request
GET /t
--- response_body
queued: nil nil
err: nil
values: v:{s}one,v:{s}two
sent: 1
--- no_error_log
[error]

=== TEST 8: a command that cannot be split rejects keys in different slots
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return 1
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkSinter",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local ok1, err1 = red:sinter("b", "z")
            local ok2, err2 = red:sinter("{s}a", "{s}b")

            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "sinter" then
                    count = count + 1
                end
            end
            ngx.say("cross: ", tostring(ok1), " ", tostring(err1))
            ngx.say("same: ", tostring(ok2), " ", tostring(err2))
            ngx.say("sent: ", count)
        ';
    }
--- request
GET /t
--- response_body
cross: nil command 'sinter' needs all keys in one slot; use a hash tag or split the call
same: 1 nil
sent: 1
--- no_error_log
[error]

=== TEST 9: MSET with keys in one slot sends one command
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMset",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local same_ok, same_err = red:mset("{s}a", "1", "{s}b", "2")

            local count, args = 0, {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mset" then
                    count = count + 1
                    args = e.args
                end
            end
            ngx.say("same: ", tostring(same_ok), " ", tostring(same_err))
            ngx.say("count: ", count, " args: ", table.concat(args, "+"))
        ';
    }
--- request
GET /t
--- response_body
same: OK nil
count: 1 args: {s}a+1+{s}b+2
--- no_error_log
[error]

=== TEST 10: a numeric key is routed by the text that the client sends
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    return out
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkNumeric",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget(42, "b")

            local cmds = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    local args = {}
                    for i = 1, #e.args do
                        args[i] = tostring(e.args[i])
                    end
                    cmds[#cmds + 1] = args[1]
                end
            end
            table.sort(cmds)
            ngx.say("keys: ", table.concat(res or {}, ","), " err: ", tostring(err))
            ngx.say("cmds: ", table.concat(cmds, ","))
        ';
    }
--- request
GET /t
--- response_body
keys: v:42,v:b err: nil
cmds: 42,b
--- no_error_log
[error]

=== TEST 11: a part that Redis rejects returns the Redis error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "del" then
                    if self._node == "127.0.0.1:7002" then
                        return false, "ERR del failed"
                    end
                    return 1
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkPartError",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:del("b", "z", "a")
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: del failed for slot 8157: ERR del failed
--- no_error_log
[error]

=== TEST 12: SMOVE checks its two keys and ignores the member
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return 1
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkSmove",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local ok1, err1 = red:smove("b", "z", "member")
            local ok2, err2 = red:smove("{s}a", "{s}b", "z")

            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "smove" then
                    count = count + 1
                end
            end
            ngx.say("cross: ", tostring(ok1), " ", tostring(err1))
            ngx.say("same: ", tostring(ok2), " ", tostring(err2))
            ngx.say("sent: ", count)
        ';
    }
--- request
GET /t
--- response_body
cross: nil command 'smove' needs all keys in one slot; use a hash tag or split the call
same: 1 nil
sent: 1
--- no_error_log
[error]

=== TEST 13: MSET with an odd number of arguments returns an error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMsetOdd",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mset("b", "1", "z")

            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mset" then
                    count = count + 1
                end
            end
            ngx.say("res: ", tostring(res), " err: ", tostring(err))
            ngx.say("sent: ", count)
        ';
    }
--- request
GET /t
--- response_body
res: nil err: wrong number of arguments for 'mset' command
sent: 0
--- no_error_log
[error]

=== TEST 14: a cross-slot MSET is rejected and writes nothing
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMsetGuard",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mset("b", "1", "z", "2")
            local same, same_err = red:mset("{s}a", "1", "{s}b", "2")

            local sent = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mset" then
                    sent = sent + 1
                end
            end
            ngx.say("cross: ", tostring(res), " ", tostring(err))
            ngx.say("same: ", tostring(same), " ", tostring(same_err))
            ngx.say("sent: ", sent)
        ';
    }
--- request
GET /t
--- response_body
cross: nil command 'mset' needs all keys in one slot; use a hash tag or split the call
same: OK nil
sent: 1
--- no_error_log
[error]

=== TEST 15: a nil argument returns an error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkNilArg",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("b", nil, "z")

            local count = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    count = count + 1
                end
            end
            ngx.say("res: ", tostring(res), " err: ", tostring(err))
            ngx.say("sent: ", count)
        ';
    }
--- request
GET /t
--- response_body
res: nil err: nil argument for 'mget' command
sent: 0
--- no_error_log
[error]

=== TEST 16: a repeated key keeps its value and its position
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "mget" then
                    local keys = { ... }
                    local out = {}
                    for i = 1, #keys do
                        out[i] = "v:" .. tostring(keys[i])
                    end
                    return out
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkRepeat",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("b", "z", "b")

            local cmds = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    local args = {}
                    for i = 1, #e.args do
                        args[i] = tostring(e.args[i])
                    end
                    cmds[#cmds + 1] = e.node .. "=" .. table.concat(args, "+")
                end
            end
            table.sort(cmds)

            ngx.say("values: ", table.concat(res or {}, ","), " err: ", tostring(err))
            ngx.say("cmds: ", table.concat(cmds, " "))
        ';
    }
--- request
GET /t
--- response_body
values: v:b,v:z,v:b err: nil
cmds: 127.0.0.1:7001=b+b 127.0.0.1:7002=z
--- no_error_log
[error]

=== TEST 17: SMOVE and RENAME reject a wrong argument count
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return 1
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkArgCount",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local _, smove2 = red:smove("{s}a", "{s}b")
            local _, smove4 = red:smove("{s}a", "{s}b", "m", "extra")
            local _, rename1 = red:rename("{s}a")
            local _, rename3 = red:rename("{s}a", "{s}b", "{s}c")
            local ok, smove_ok = red:smove("{s}a", "{s}b", "m")

            local sent = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and (e.cmd == "smove" or e.cmd == "rename") then
                    sent = sent + 1
                end
            end
            ngx.say(tostring(smove2))
            ngx.say(tostring(smove4))
            ngx.say(tostring(rename1))
            ngx.say(tostring(rename3))
            ngx.say("ok: ", tostring(ok == 1 or ok), " sent: ", sent)
        ';
    }
--- request
GET /t
--- response_body
wrong number of arguments for 'smove' command
wrong number of arguments for 'smove' command
wrong number of arguments for 'rename' command
wrong number of arguments for 'rename' command
ok: true sent: 1
--- no_error_log
[error]

=== TEST 18: a MOVED reply on one split part is retried
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            -- The slot map is stale: 7001 owns every slot.
            fake.topology = 3
            fake.handler = function(self, cmd, ...)
                if cmd ~= "mget" then
                    return "OK"
                end
                if self._node == "127.0.0.1:7001" then
                    -- The next CLUSTER SLOTS reports the real map.
                    fake.topology = 1
                    return false, "MOVED 12182 127.0.0.1:7003"
                end
                local keys = { ... }
                local out = {}
                for i = 1, #keys do
                    out[i] = "v:" .. tostring(keys[i])
                end
                return out
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMovedSplit",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("z", "a")

            local mgets = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mget" then
                    mgets = mgets + 1
                end
            end
            ngx.say("values: ", table.concat(res or {}, ","), " err: ", tostring(err))
            ngx.say("mgets: ", mgets)
        ';
    }
--- request
GET /t
--- response_body
values: v:z,v:a err: nil
mgets: 4
--- no_error_log
[error]

=== TEST 19: an ASK reply on one split part is retried on the target node
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd, ...)
                if cmd == "asking" then
                    return "OK"
                end
                if cmd ~= "mget" then
                    return "OK"
                end
                if self._node == "127.0.0.1:7002" then
                    return false, "ASK 8157 127.0.0.1:7001"
                end
                local keys = { ... }
                local out = {}
                for i = 1, #keys do
                    out[i] = "v:" .. tostring(keys[i])
                end
                return out
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkAskSplit",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("z", "a")

            local asking = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "asking" then
                    asking = asking + 1
                end
            end
            ngx.say("values: ", table.concat(res or {}, ","), " err: ", tostring(err))
            ngx.say("asking: ", asking)
        ';
    }
--- request
GET /t
--- response_body
values: v:z,v:a err: nil
asking: 1
--- no_error_log
[error]


=== TEST 20: MGET returns no values when a middle part fails
"b", "z" and "a" are in 7001, 7002 and 7003, so the reply has three parts.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "mget" and self._node == "127.0.0.1:7002" then
                    return false, "ERR boom"
                end
                if cmd == "mget" then
                    return { "v1" }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMgetMiddleError",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("b", "z", "a")
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: mget failed for slot 8157: ERR boom
--- no_error_log
[error]

=== TEST 21: MGET returns no values when the first part fails
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "mget" and self._node == "127.0.0.1:7001" then
                    return false, "ERR boom"
                end
                if cmd == "mget" then
                    return { "v1" }
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkMgetFirstError",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mget("b", "z", "a")
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: mget failed for slot 3300: ERR boom
--- no_error_log
[error]

=== TEST 22: UNLINK returns no count when a middle part fails
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "unlink" and self._node == "127.0.0.1:7002" then
                    return false, "ERR boom"
                end
                if cmd == "unlink" then
                    return 1
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkUnlinkMiddleError",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:unlink("b", "z", "a")
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: unlink failed for slot 8157: ERR boom
--- no_error_log
[error]

=== TEST 23: TOUCH returns no count when a middle part fails
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "touch" and self._node == "127.0.0.1:7002" then
                    return false, "ERR boom"
                end
                if cmd == "touch" then
                    return 1
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkTouchMiddleError",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:touch("b", "z", "a")
            ngx.say("res: ", tostring(res))
            ngx.say("err: ", tostring(err))
        ';
    }
--- request
GET /t
--- response_body
res: nil
err: touch failed for slot 8157: ERR boom
--- no_error_log
[error]

=== TEST 24: an upper case command name takes the same path
Redis takes a command name in any case, and the client builds a method for any
name. Without normalization an upper case call skips the slot check and the
split, so it reaches Redis and fails with CROSSSLOT.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                if cmd == "del" or cmd == "exists" then
                    return 1
                end
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkUpperSplit",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            -- "b", "z" and "a" are in three different slots.
            local lower = red:del("b", "z", "a")
            local upper = red:DEL("b", "z", "a")
            local mixed = red:Exists("b", "z", "a")

            ngx.say("lower: ", tostring(lower))
            ngx.say("upper: ", tostring(upper))
            ngx.say("mixed: ", tostring(mixed))

            local sent = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and (e.cmd == "del" or e.cmd == "exists") then
                    sent = sent + 1
                end
            end
            ngx.say("commands: ", sent)
        ';
    }
--- request
GET /t
--- response_body
lower: 3
upper: 3
mixed: 3
commands: 9
--- no_error_log
[error]

=== TEST 25: an upper case guarded command returns the same error
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()
            fake.handler = function(self, cmd)
                return "OK"
            end

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkUpperGuard",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:MSET("b", 1, "z", 2)
            local same, same_err = red:MSET("{s}a", 1, "{s}b", 2)

            ngx.say("cross: ", tostring(res), " ", tostring(err))
            ngx.say("same: ", tostring(same), " ", tostring(same_err))
        ';
    }
--- request
GET /t
--- response_body
cross: nil command 'mset' needs all keys in one slot; use a hash tag or split the call
same: OK nil
--- no_error_log
[error]

=== TEST 26: an upper case command cannot reach a blocked command
"shutdown" is in the blocklist. The instance holds no "shutdown" field, so the
metatable builds a method for it, and an upper case call must reach the same
blocklist entry.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local fake = require "fake_redis"
            package.loaded["resty.redis"] = fake

            fake.reset()

            local redis = require "resty.rediscluster"
            local red = assert(redis:new({
                name = "mkUpperBlocked",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local lower, lower_err = red:shutdown()
            local upper, upper_err = red:SHUTDOWN()

            ngx.say("lower: ", tostring(lower), " ", tostring(lower_err))
            ngx.say("upper: ", tostring(upper), " ", tostring(upper_err))

            local sent = 0
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "shutdown" then
                    sent = sent + 1
                end
            end
            ngx.say("shutdown commands sent: ", sent)
        ';
    }
--- request
GET /t
--- response_body
lower: nil command not supported
upper: nil command not supported
shutdown commands sent: 0
--- no_error_log
[error]
