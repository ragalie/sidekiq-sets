module Sidekiq
  module Sets
    module Scripting
      LUA_SCRIPTS = {
        zaddnx: <<-LUA,
          local to_add = {}
          for i = 1, #ARGV, 2 do
            local message = ARGV[i+1]
            local value = redis.call('ZSCORE', KEYS[1], message)
            if not value then
              local score = ARGV[i]
              table.insert(to_add, score)
              table.insert(to_add, message)
            end
          end

          if #to_add == 0 then
            return 0
          end

          return redis.call('ZADD', KEYS[1], unpack(to_add))
        LUA
        zpop: <<-LUA
          local element = redis.call('ZRANGE', KEYS[1], 0, 0)[1]
          if element ~= nil then
            redis.call('ZREM', KEYS[1], element)
          end

          return element
        LUA
      }

      def self.call(name, keys, args)
        Sidekiq.redis { |conn| conn.eval(LUA_SCRIPTS[name], keys, args) }
      end
    end
  end
end
