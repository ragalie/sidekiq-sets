module Sidekiq
  module Sets
    class Queue < Sidekiq::Queue
      def self.retrieve_work(redis_queue_names)
        Sidekiq.redis do |conn|
          queue = conn.brpop(*redis_queue_names.map { |name| "#{name}:active"})
          Sidekiq::Sets::Scripting.call(:zpop, [queue], [])
        end
      end

      def push(payloads)
        with_conn do |conn|
          if payloads.first['at']
            conn.zadd('schedule', payloads.map do |hash|
              at = hash.delete('at').to_s
              [at, Sidekiq.dump_json(hash)]
            end)
          else
            to_push = payloads.map { |entry| [Time.now.to_i, Sidekiq.dump_json(entry)] }
            conn.sadd('queues', @name)

            conn.multi do
              added = Sidekiq::Sets::Scripting.call(:zaddnx, [@rname], to_push)

              if added
                conn.lpush("#{@rname}:active", @rname)
              end
            end
          end
        end
      end

      def requeue(messages)
        with_conn do |conn|
          conn.multi do
            conn.zadd(@rname, messages.map { |message| [0, message] })
            conn.rpush("#{@rname}:active", @rname)
          end
        end
      end

      def size
        with_conn { |conn| conn.zcard(@rname) }
      end

      def latency
        entry = with_conn do |conn|
          conn.zrange(@rname, -1, -1)
        end.first
        return 0 unless entry
        Time.now.to_f - Sidekiq.load_json(entry)['enqueued_at']
      end

      def each(&block)
        initial_size = size
        deleted_size = 0
        page = 0
        page_size = 50

        loop do
          range_start = page * page_size - deleted_size
          range_end   = page * page_size - deleted_size + (page_size - 1)
          entries = with_conn do |conn|
            conn.zrange @rname, range_start, range_end
          end
          break if entries.empty?
          page += 1
          entries.each do |entry|
            block.call Job.new(entry, @name)
          end
          deleted_size = initial_size - size
        end
      end
    end
  end
end

Sidekiq.options[:queue_strategy] = Sidekiq::Sets::Queue
