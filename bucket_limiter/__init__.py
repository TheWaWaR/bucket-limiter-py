# coding: utf-8

import time

import redis

LUA_SCRIPT = '''
local key = KEYS[1]
local interval = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local nTokens = tonumber(ARGV[3])
local timeNow = tonumber(ARGV[4])
local expire = tonumber(ARGV[5])

local currentTokens

if redis.call('exists', key) == 0 then
  currentTokens = capacity
  redis.call('hset', key, 'lastFillAt', timeNow)
else
  local tokens = tonumber(redis.call('hget', key, 'tokens'))
  local lastFillAt = tonumber(redis.call('hget', key, 'lastFillAt'))
  if timeNow - lastFillAt > interval then
    currentTokens = capacity
    redis.call('hset', key, 'lastFillAt', timeNow)
  else
    currentTokens = tokens
  end
end

assert(currentTokens >= 0)

if expire > 0 then
  redis.call('expire', key, expire)
end

if nTokens > currentTokens then
  redis.call('hset', key, 'tokens', currentTokens)
  return 0
else
  redis.call('hset', key, 'tokens', currentTokens - nTokens)
  return 1
end
'''


def now_ms():
    return int(time.time() * 1000)


class Limiter(object):

    def get_token_count(self, key):
        raise NotImplemented()

    def consume(self, key, interval, capacity, n=1):
        raise NotImplemented()


class RedisLimiter(Limiter):

    def __init__(self,
                 redis_cli=None,
                 host='localhost', port=6379, db=0):
        if not redis_cli:
            redis_cli = redis.StrictRedis(host=host, port=port, db=db)
        self.redis_cli = redis_cli

    def get_redis_key(self, key, interval):
        return 'limiter:{}:{}'.format(key, interval)

    def get_token_count(self, key, interval):
        redis_key = self.get_redis_key(key, interval)
        return self.redis_cli.hget(redis_key, 'tokens')

    def consume(self, key, interval, capacity, n=1, expire=0):
        redis_key = self.get_redis_key(key, interval)
        if expire == 0:
            expire = interval * 3 + 60
        interval_ms = interval * 1000
        return 1 == self.redis_cli.eval(
            LUA_SCRIPT, 1,
            redis_key,
            interval_ms, capacity, n, now_ms(), expire
        )
