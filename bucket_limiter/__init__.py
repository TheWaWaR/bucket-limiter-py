# coding: utf-8

import time
import hashlib

import redis
from redis.exceptions import RedisError

LUA_SCRIPT = '''
local effects = {}
for idx, key in ipairs(KEYS) do
  local idxBase = (idx - 1) * 5
  local interval = tonumber(ARGV[idxBase + 1])
  local capacity = tonumber(ARGV[idxBase + 2])
  local nTokens = tonumber(ARGV[idxBase + 3])
  local timeNow = tonumber(ARGV[idxBase + 4])
  local expire = tonumber(ARGV[idxBase + 5])
  local currentTokens = -1
  local lastFillAt = timeNow

  if redis.call('exists', key) == 0 then
    currentTokens = capacity
    redis.call('hset', key, 'lastFillAt', timeNow)
  else
    lastFillAt = tonumber(redis.call('hget', key, 'lastFillAt'))
    if timeNow - lastFillAt > interval then
      currentTokens = capacity
      redis.call('hset', key, 'lastFillAt', timeNow)
    else
      currentTokens = tonumber(redis.call('hget', key, 'tokens'))
      if currentTokens > capacity then
        currentTokens = capacity
      end
    end
  end

  assert(currentTokens >= 0)

  if expire > 0 then
    redis.call('expire', key, expire)
  end

  if nTokens > currentTokens then
    redis.call('hset', key, 'tokens', currentTokens)
    for i, effect in ipairs(effects) do
      redis.call('hset', effect[1], 'tokens', effect[2])
    end
    return {key, interval, capacity, currentTokens, lastFillAt}
  else
    table.insert(effects, {key, currentTokens, nTokens})
  end
end

for i, effect in ipairs(effects) do
  redis.call('hset', effect[1], 'tokens', effect[2] - effect[3])
end

return {'', 0, 0, 0, 0}
'''
LUA_SCRIPT_SHA1 = hashlib.sha1(LUA_SCRIPT).hexdigest()


def now_ms():
    return int(time.time() * 1000)


class RedisConsumeDenied(object):
    def __init__(self, redis_rv):
        self.redis_key = redis_rv[0]
        self.interval = redis_rv[1] / 1000
        self.capacity = redis_rv[2]
        self.current_tokens = redis_rv[3]
        self.last_fill_at = redis_rv[4]

    def __repr__(self):
        return '<RedisConsumeDenied([{}] interval={}, capacity={}, tokens={})>'.format(
            self.redis_key, self.interval, self.capacity, self.current_tokens,
        )


class RedisLimiter(object):

    def __init__(self,
                 redis_cli=None,
                 host='localhost', port=6379, db=0,
                 key_prefix='limiter'):
        if not redis_cli:
            redis_cli = redis.StrictRedis(host=host, port=port, db=db)
        self.redis_cli = redis_cli
        self.key_prefix = key_prefix

    def get_redis_key(self, key, interval):
        return '{}:{}:{}'.format(self.key_prefix, key, interval)

    def get_token_count(self, key, interval):
        redis_key = self.get_redis_key(key, interval)
        return self.redis_cli.hget(redis_key, 'tokens')

    def consume(self, args):
        script_keys = []
        script_args = []
        the_now_ms = now_ms()
        for (key, interval, capacity, n) in args:
            redis_key = self.get_redis_key(key, interval)
            expire = interval * 2 + 15
            interval_ms = interval * 1000
            script_keys.append(redis_key)
            script_args.extend([interval_ms, capacity, n, the_now_ms, expire])

        for i in range(3):
            try:
                rv = self.redis_cli.evalsha(
                    LUA_SCRIPT_SHA1, len(script_keys), *(script_keys + script_args)
                )
                if rv == ['', 0, 0, 0, 0]:
                    return True, None
                else:
                    return False, RedisConsumeDenied(rv)
            except RedisError:
                sha1 = self.redis_cli.script_load(LUA_SCRIPT)
                assert sha1 == LUA_SCRIPT_SHA1

    def consume_one(self, key, interval, capacity, n=1):
        return self.consume([(key, interval, capacity, n)])
