module Airtable
  # Thread-safe sliding window rate limiter.
  #
  # Airtable enforces a limit of 5 requests per second per base. This limiter
  # delays requests that would exceed the limit, preventing 429 responses.
  #
  # The limiter is process-global (singleton) and keyed by base ID (app_token),
  # so all Table instances in the same Sidekiq process share it.
  #
  # Usage is automatic — Table calls RateLimiter.wait! before each request.
  class RateLimiter
    DEFAULT_MAX_REQUESTS = 5
    DEFAULT_WINDOW_SECONDS = 1.0

    def initialize(max_requests: DEFAULT_MAX_REQUESTS, window_seconds: DEFAULT_WINDOW_SECONDS, clock: nil)
      @max_requests = max_requests
      @window_seconds = window_seconds
      @clock = clock || -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
      @buckets = {}
      @buckets_mutex = Mutex.new
    end

    # Block until a request slot is available for the given base.
    def wait!(base_id)
      bucket = bucket_for(base_id)
      bucket.wait!
    end

    class << self
      def instance
        @instance_mutex.synchronize do
          @instance ||= new
        end
      end

      def instance=(limiter)
        @instance_mutex.synchronize do
          @instance = limiter
        end
      end

      def reset!
        @instance_mutex.synchronize do
          @instance = nil
        end
      end
    end

    @instance_mutex = Mutex.new
    @instance = nil

    private

    def bucket_for(base_id)
      @buckets_mutex.synchronize do
        @buckets[base_id] ||= Bucket.new(@max_requests, @window_seconds, @clock)
      end
    end

    # Per-base sliding window bucket. Tracks timestamps of recent requests
    # and sleeps when the window is full.
    #
    # Thread-safety: the mutex is released during sleep so other threads
    # targeting the same base can proceed once a slot opens.
    class Bucket
      def initialize(max_requests, window_seconds, clock)
        @max_requests = max_requests
        @window_seconds = window_seconds
        @clock = clock
        @timestamps = []
        @mutex = Mutex.new
      end

      def wait!
        loop do
          sleep_time = nil

          @mutex.synchronize do
            now = @clock.call
            evict_expired!(now)

            if @timestamps.length >= @max_requests
              sleep_time = @window_seconds - (now - @timestamps.first)
              sleep_time = 0.001 if sleep_time <= 0
            else
              @timestamps << now
              return
            end
          end

          # Sleep WITHOUT holding the mutex so other threads aren't blocked
          Kernel.sleep(sleep_time)
        end
      end

      private

      def evict_expired!(now)
        @timestamps.reject! { |t| now - t >= @window_seconds }
      end
    end
  end
end
