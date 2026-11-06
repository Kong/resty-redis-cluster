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

=== TEST 13: MSET splits the pairs by slot and sends them to the right nodes
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
                name = "mkMsetSplit",
                serv_list = { { ip = "127.0.0.1", port = 7001 } },
                dict_name = "redis_cluster_slot_locks",
                lock_timeout = 1,
            }))

            local res, err = red:mset("b", "1", "z", "2", "a", "3")

            local cmds = {}
            for _, e in ipairs(fake.events) do
                if e.ev == "cmd" and e.cmd == "mset" then
                    local args = {}
                    for i = 1, #e.args do
                        args[i] = tostring(e.args[i])
                    end
                    cmds[#cmds + 1] = e.node .. "=" .. table.concat(args, "+")
                end
            end
            table.sort(cmds)

            ngx.say("res: ", tostring(res), " err: ", tostring(err))
            ngx.say("cmds: ", table.concat(cmds, " "))
        ';
    }
--- request
GET /t
--- response_body
res: OK err: nil
cmds: 127.0.0.1:7001=b+1 127.0.0.1:7002=z+2 127.0.0.1:7003=a+3
--- no_error_log
[error]

=== TEST 14: MSET with an odd number of arguments returns an error
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

