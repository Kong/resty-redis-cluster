LUA_VERSION      ?= 5.1
OPENRESTY_PREFIX ?= /usr/local/openresty
NGINX_SBIN       ?= $(OPENRESTY_PREFIX)/nginx/sbin
PREFIX           ?= /usr/local
LUA_INCLUDE_DIR  ?= $(PREFIX)/include
LUA_LIB_DIR      ?= $(PREFIX)/lib/lua/$(LUA_VERSION)
INSTALL          ?= install

# Test::Nginx takes `nginx` from PATH, or from TEST_NGINX_BINARY. A distribution
# nginx has no ngx_lua, so pin the OpenResty build instead of trusting PATH.
TEST_ENV = PATH=$(NGINX_SBIN):$$PATH TEST_NGINX_BINARY=$(NGINX_SBIN)/nginx

.PHONY: all test test-redis7 install

all: ;

install: all
	$(INSTALL) -d $(DESTDIR)$(LUA_LIB_DIR)/resty
	$(INSTALL) lib/resty/*.lua $(DESTDIR)$(LUA_LIB_DIR)/resty

test: all
	$(TEST_ENV) prove -r t

# z-recover.t starts the 6.2 fixture itself, so it is not in this list.
REDIS7_TESTS ?= t/auth_hook.t t/auth.t t/auth_rotation.t t/auth_pool_window.t \
                t/redis7_with_host.t t/redirects.t t/closed.t t/cmd_master.t \
                t/lock_scope.t t/sanity.t t/mock-redis-down.t \
                t/version.t t/lock_timeout.t

test-redis7: all
	$(TEST_ENV) prove $(REDIS7_TESTS)
