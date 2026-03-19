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
      uri = URI(BASE_URI)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      http.open_timeout = @timeout
      http.read_timeout = @timeout
      http.write_timeout = @timeout
      http.keep_alive_timeout = 30
      http.start
      http
    end

    def connection_active?
      @connection&.started?
    rescue IOError
      false
    end

    def close_connection
      @connection&.finish if connection_active?
    rescue IOError
      # already closed
    ensure
      @connection = nil
    end

    def perform_request(request)
      retries = 0
      begin
        connection.request(request)
      rescue IOError, Errno::ECONNRESET, Errno::EPIPE, OpenSSL::SSL::SSLError => e
        close_connection
        retries += 1
        retry if retries <= 1
        raise e
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
