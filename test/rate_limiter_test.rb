require 'test_helper'

describe Airtable::RateLimiter do
  before do
    Airtable::RateLimiter.reset!
  end

  describe "within limit" do
    it "should allow requests up to the max without delay" do
      limiter = Airtable::RateLimiter.new(max_requests: 5, window_seconds: 1.0)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      5.times { limiter.wait!("appBase1") }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
      assert elapsed < 0.1, "5 requests within limit should not delay, took #{elapsed}s"
    end
  end

  describe "exceeding limit" do
    it "should delay the request that exceeds the limit" do
      limiter = Airtable::RateLimiter.new(max_requests: 3, window_seconds: 0.5)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      4.times { limiter.wait!("appBase1") }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
      assert elapsed >= 0.4, "4th request with max_requests=3 should wait, took #{elapsed}s"
    end
  end

  describe "independent bases" do
    it "should not throttle different bases against each other" do
      limiter = Airtable::RateLimiter.new(max_requests: 3, window_seconds: 1.0)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      3.times { limiter.wait!("appBase1") }
      3.times { limiter.wait!("appBase2") }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
      assert elapsed < 0.1, "Different bases should not interfere, took #{elapsed}s"
    end
  end

  describe "window sliding" do
    it "should allow requests after the window slides" do
      time = 0.0
      clock = -> { time }
      limiter = Airtable::RateLimiter.new(max_requests: 3, window_seconds: 1.0, clock: clock)

      # Fill the window
      3.times { limiter.wait!("appBase1") }

      # Advance time past the window
      time = 1.1

      # These should succeed immediately (window has slid)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      3.times { limiter.wait!("appBase1") }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
      assert elapsed < 0.1, "Requests after window should not delay, took #{elapsed}s"
    end
  end

  describe "singleton" do
    it "should return the same instance" do
      a = Airtable::RateLimiter.instance
      b = Airtable::RateLimiter.instance
      assert_same a, b
    end

    it "should allow replacing the instance" do
      custom = Airtable::RateLimiter.new(max_requests: 10)
      Airtable::RateLimiter.instance = custom
      assert_same custom, Airtable::RateLimiter.instance
    end

    it "should reset to a new instance" do
      original = Airtable::RateLimiter.instance
      Airtable::RateLimiter.reset!
      replacement = Airtable::RateLimiter.instance
      refute_same original, replacement
    end
  end

  describe "thread safety" do
    it "should handle concurrent access without errors" do
      limiter = Airtable::RateLimiter.new(max_requests: 5, window_seconds: 0.5)
      errors = []
      threads = 5.times.map do
        Thread.new do
          3.times { limiter.wait!("appBase1") }
        rescue => e
          errors << e
        end
      end
      threads.each(&:join)
      assert_empty errors, "Expected no errors, got: #{errors.map(&:message)}"
    end

    it "should enforce the rate limit across threads" do
      limiter = Airtable::RateLimiter.new(max_requests: 5, window_seconds: 0.5)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      threads = 3.times.map do
        Thread.new do
          5.times { limiter.wait!("appBase1") }
        end
      end
      threads.each(&:join)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
      # 15 requests at 5/0.5s = need at least 1 second (3 windows)
      assert elapsed >= 0.9, "15 requests at 5/0.5s should take >= 0.9s, took #{elapsed}s"
    end
  end
end
