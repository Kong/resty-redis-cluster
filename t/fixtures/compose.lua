local _M = {}

local io_popen    = io.popen
local str_fmt     = string.format
local str_find    = string.find
local ngx_sleep   = ngx.sleep
local update_time = ngx.update_time

local function updown(up_down, retry)
    if type(up_down) ~= "string" then
        return nil, "up_down must be string"
    end

    if up_down ~= "up" and up_down ~= "down" then
        return nil, "up_down must be either 'up' or 'down'"
    end

    local red_dir = "6.2.14"
    local docker_compose_yml = str_fmt("t/redis_cluster/%s/no-auth/%s", red_dir, retry and "docker-compose-retry.yaml" or "docker-compose.yaml")
    local cmd = str_fmt("docker compose -f %s ", docker_compose_yml)

    if up_down == "up" then
        cmd = cmd .. "up -d --quiet-pull "
    end
    if up_down == "down" then
        cmd = cmd .. "down -v "
    end
    cmd = cmd .. "2>&1 1>/dev/null"

    local cok, sig, code = os.execute(cmd)

    local container_name = "redis-cluster-init"
    local inspect_cmd = str_fmt('docker inspect --format "{{.State.Status}}:{{.State.ExitCode}}" %s', container_name)
    local counter, max_count = 0, 6
    local inspect_stat = up_down == "up" and "exited:" or "\n"
    local fh, out
    update_time()
    while true do
        fh = io_popen(inspect_cmd, "r")
        out = fh:read(50)
        fh:close()
        if str_find(out, inspect_stat, 1, true) then
            counter = counter + 1
            if counter >= max_count then
                break
            end
        end
        ngx_sleep(1)
    end

    return cok, sig, code
end

_M.updown = updown

return _M
