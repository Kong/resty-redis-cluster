=begin comment
This test stops a master container and rebuilds the fixture afterwards, so it
runs only when the caller asks for it. Run it by hand:

REDIS_AUTH=no Z_FAILOVER=1 PATH="/usr/local/openresty/nginx/sbin:$PATH" \
    prove -v t/z-failover.t

Set FAILOVER_COMPOSE when the fixture is not the repository one. The fixture
listens on 127.0.0.1:6371-6376 and needs a replica for every master.
=end comment
=cut

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

my $redis_auth = $ENV{REDIS_AUTH};
my $run_failover = $ENV{Z_FAILOVER};
if (defined($redis_auth) && $redis_auth eq "no"
    && defined($run_failover) && $run_failover eq "1") {
    plan tests => repeat_each() * (3 * blocks());
} else {
    plan(skip_all => "set Z_FAILOVER=1 and REDIS_AUTH=no to run this test");
}

my $pwd = cwd();
my $compose = $ENV{FAILOVER_COMPOSE} || "t/redis_cluster/7.2/no-auth/docker-compose.yaml";
my $compose_cmd = "docker compose -f '$compose'";

add_block_preprocessor(sub {
    my $block = shift;
    my $config = $block->config;
    if (defined($config)) {
        $config =~ s/__COMPOSE_CMD__/$compose_cmd/g;
        $block->set_value("config", $config);
    }
});

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_shared_dict redis_cluster_slot_locks 32k;
};

no_long_string();
no_shuffle();
run_tests();

__DATA__

=== TEST 1: scan_all recovers after a master fails over
The test stops a master that has a replica. The cluster needs about ten seconds
to promote the replica, so the first scan_all calls fail. Once the promotion is
complete, a call succeeds and reads every key again. The test rebuilds the
fixture at the end, because a restarted node has lost its cluster config.
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {

            local function shell(cmd)
                local fh = io.popen(cmd .. " 2>/dev/null")
                local out = fh:read("*a")
                fh:close()
                return out
            end

            -- Find a master and the replica that replaces it. A node line is
            -- "<id> <ip:port@cport> <flags> <master-id|-> ... <slots>".
            local nodes = shell("redis-cli -p 6371 cluster nodes")
            local masters, replicas = {}, {}
            for line in nodes:gmatch("[^\n]+") do
                local id, addr, flags, owner = line:match("^(%x+) ([%d%.]+:%d+)@%d+ ([%w_,-]+) ([%x-]+)")
                local port = addr and tonumber(addr:match(":(%d+)$"))
                if port then
                    if flags:find("master", 1, true) and not flags:find("fail", 1, true) then
                        masters[id] = port
                    elseif flags:find("slave", 1, true) then
                        replicas[owner] = port
                    end
                end
            end

            local master_port
            for id, port in pairs(masters) do
                if replicas[id] then
                    master_port = port
                    break
                end
            end

            if not master_port then
                ngx.say("no master with a replica")
                return
            end

            local config = {
                            name      = "zFailover",
                            serv_list = {
                                            { ip = "127.0.0.1", port = 6371 },
                                            { ip = "127.0.0.1", port = 6372 },
                                            { ip = "127.0.0.1", port = 6373 },
                                            { ip = "127.0.0.1", port = 6374 },
                                            { ip = "127.0.0.1", port = 6375 },
                                            { ip = "127.0.0.1", port = 6376 }
                                        },
                            connect_timeout   = 1000,
                            read_timeout      = 1000,
                            send_timeout      = 1000,
                            max_redirection   = 5
            }
            local redis = require "resty.rediscluster"
            local red, err = redis:new(config)
            if not red then
                ngx.say("failed to create the client: ", err)
                return
            end

            assert(red:flushall())
            for i = 1, 100 do
                assert(red:set("zfo:" .. i, i))
            end

            -- One full iteration before the failure, to fill the slot cache.
            local function count_keys()
                local cursor, total = "0", 0
                repeat
                    local res, scan_err = red:scan_all(cursor, "MATCH", "zfo:*", "COUNT", "1000")
                    if not res then
                        return nil, scan_err
                    end
                    cursor = res[1]
                    total = total + #res[2]
                until cursor == "0"
                return total
            end

            local before = count_keys()

            local container = "redis_" .. (master_port - 6370)
            ngx.log(ngx.WARN, "z-failover: stop ", container)
            shell("docker stop " .. container)

            local recovered_at, last_err, failed = nil, nil, 0
            local started = ngx.now()
            while ngx.now() - started < 60 do
                local res, scan_err = red:scan_all("0", "MATCH", "zfo:*", "COUNT", "1000")
                if res then
                    recovered_at = ngx.now() - started
                    break
                end
                failed = failed + 1
                last_err = scan_err
                ngx.sleep(1)
            end

            local after
            if recovered_at then
                after = count_keys()
            end

            -- A restarted node has lost nodes.conf and its data, so rebuild the
            -- whole fixture instead of starting that one container again.
            shell("__COMPOSE_CMD__ down -v")
            shell("__COMPOSE_CMD__ up -d")
            local restored = false
            for _ = 1, 60 do
                if shell("redis-cli -p 6371 cluster info"):find("cluster_state:ok", 1, true) then
                    restored = true
                    break
                end
                ngx.sleep(2)
            end

            ngx.say("keys before: ", tostring(before))
            ngx.say("failed calls: ", tostring(failed > 0))
            ngx.say("last error is set: ", tostring(last_err ~= nil))
            ngx.say("recovered: ", tostring(recovered_at ~= nil))
            ngx.say("keys after: ", tostring(after))
            ngx.say("fixture restored: ", tostring(restored))
        }
    }
--- request
GET /t
--- response_body
keys before: 100
failed calls: true
last error is set: true
recovered: true
keys after: 100
fixture restored: true
--- error_log eval
# The stopped master refuses the connection. The connect error is the point of
# the test, not a defect.
[qr/(connection refused|connect timed out)/]
--- wait: 1
--- timeout: 300s
