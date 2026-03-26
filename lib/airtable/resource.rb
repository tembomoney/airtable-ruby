module Airtable
  # Base class for authorised resources sending network requests.
  #
  # Each Table instance holds its own persistent connection to the Airtable
  # API. Table instances must not be shared across threads.
  class Resource
    BASE_URI = 'https://api.airtable.com'
    BASE_PATH = '/v0'
    DEFAULT_TIMEOUT = 30

    attr_reader :api_key, :app_token, :worksheet_name

    def initialize(api_key, app_token, worksheet_name, timeout: DEFAULT_TIMEOUT)
      @api_key = api_key
      @app_token = app_token
      @worksheet_name = worksheet_name
      @timeout = timeout
    end

    private

    def connection
      if @connection.nil? || !connection_active?
        @connection = build_connection
      end
      @connection
    end

    def build_connection
      reused = !@connection.nil?
      uri = URI(BASE_URI)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      http.open_timeout = @timeout
      http.read_timeout = @timeout
      http.write_timeout = @timeout
      http.keep_alive_timeout = 30
      http.start
      log_connection(reused ? :reconnected : :opened)
      http
    end

    def connection_active?
      @connection&.started?
    rescue IOError
      false
    end

    def close_connection
      @connection&.finish if connection_active?
      log_connection(:closed)
    rescue IOError
      # already closed
    ensure
      @connection = nil
    end

    MAX_API_RETRIES = 3
    RETRYABLE_STATUSES = [429, 503].freeze

    def perform_request(request)
      api_attempts = 0

      loop do
        Airtable::RateLimiter.instance.wait!(app_token)

        response = perform_request_with_connection_retry(request)
        api_attempts += 1
        status = response.code.to_i

        if RETRYABLE_STATUSES.include?(status) && api_attempts < MAX_API_RETRIES
          delay = backoff_delay(api_attempts)
          log_api_retry(request, status, api_attempts, delay)
          sleep_for_retry(delay)
          next
        end

        return response
      end
    end

    def perform_request_with_connection_retry(request)
      retries = 0
      begin
        start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        response = connection.request(request)
        duration_ms = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time) * 1000).round
        @last_request_duration_ms = duration_ms
        @last_request_body_size = request.body&.bytesize || 0
        @last_response_body_size = response.body&.bytesize || 0
        response
      rescue IOError, Errno::ECONNRESET, Errno::EPIPE, OpenSSL::SSL::SSLError => e
        close_connection
        retries += 1
        if retries <= 1
          log_retry(request, e)
          retry
        end
        raise e
      end
    end

    def sleep_for_retry(delay)
      Kernel.sleep(delay)
    end

    def backoff_delay(attempt)
      base = 2**(attempt - 1)   # attempt 1 => 1s, attempt 2 => 2s
      base + (rand * base)      # full jitter: [base, 2*base)
    end

    def log_api_retry(request, status, attempt, delay)
      message = "[Airtable] HTTP #{status} on #{request.method} #{worksheet_name}, " \
                "retry #{attempt}/#{MAX_API_RETRIES - 1} in #{'%.2f' % delay}s"
      if defined?(Rails)
        Rails.logger.warn(message)
      else
        $stderr.puts(message)
      end
    end

    def log_retry(request, error)
      message = "[Airtable] Connection reset (#{error.class}: #{error.message}), retrying #{request.method} #{worksheet_name}"
      if defined?(Rails)
        Rails.logger.warn(message)
      else
        $stderr.puts(message)
      end
    end

    def log_connection(event)
      message = "[Airtable] Connection #{event} for #{worksheet_name}"
      if defined?(Rails)
        Rails.logger.debug(message)
      end
    end

    def default_headers
      {
        'Authorization' => "Bearer #{@api_key}",
        'Content-Type' => 'application/json'
      }
    end

    def build_get_request(path, query: nil)
      full_path = query ? "#{path}?#{encode_query(query)}" : path
      request = Net::HTTP::Get.new(full_path)
      default_headers.each { |key, value| request[key] = value }
      request
    end

    def build_post_request(path, body:)
      request = Net::HTTP::Post.new(path)
      default_headers.each { |key, value| request[key] = value }
      request.body = body.to_json
      request
    end

    def build_put_request(path, body:)
      request = Net::HTTP::Put.new(path)
      default_headers.each { |key, value| request[key] = value }
      request.body = body.to_json
      request
    end

    def build_patch_request(path, body:)
      request = Net::HTTP::Patch.new(path)
      default_headers.each { |key, value| request[key] = value }
      request.body = body.to_json
      request
    end

    def build_delete_request(path)
      request = Net::HTTP::Delete.new(path)
      default_headers.each { |key, value| request[key] = value }
      request
    end

    def encode_query(params)
      params.map { |key, value| "#{URI.encode_www_form_component(key.to_s)}=#{URI.encode_www_form_component(value.to_s)}" }.join('&')
    end
  end
end
