package = "kong-redis-cluster"
version = "redistls-1"
source = {
    url = "git://github.com/Kong/resty-redis-cluster",
    tag = "feat/redis-cluster-tls"
}
description = {
    summary = "Openresty lua client for redis cluster",
    detailed = [[
        Openresty environment lua client with redis cluster support.
        This is a wrapper around the 'resty.redis' library with cluster discovery 
        and failover recovery support.
    ]],
    homepage = "https://github.com/Kong/resty-redis-cluster",
    license = "Apache License 2.0"
}
dependencies = {
  "lua >= 5.1",
  "lua-resty-redis-client == redistls-1",
}
build = {
    type = "builtin",
    modules = {
        ["resty.rediscluster"] = "lib/resty/rediscluster.lua",
        ["resty.xmodem"] = "lib/resty/xmodem.lua"
    }
}