
module Airtable
  class Error < StandardError

    attr_reader :message, :type, :status_code

    # Maps HTTP status codes to named error types, matching the official airtable.js error handler.
    # Used as a fallback when the response body doesn't include a type.
    STATUS_CODE_ERROR_TYPES = {
      401 => 'AUTHENTICATION_REQUIRED',
      403 => 'NOT_AUTHORIZED',
      404 => 'NOT_FOUND',
      422 => 'INVALID_REQUEST',
      429 => 'TOO_MANY_REQUESTS',
      500 => 'SERVER_ERROR',
      503 => 'SERVICE_UNAVAILABLE'
    }.freeze

    # {"error"=>{"type"=>"UNKNOWN_COLUMN_NAME", "message"=>"Could not find fields foo"}}
    def initialize(error_hash, status_code: nil)
      @message = error_hash['message']
      @type = error_hash['type']
      @status_code = status_code
      super(@message)
    end

    # Build an Error from a raw HTTP status code and response body.
    # Attempts JSON parse first; falls back to classifying by status code.
    # The type from the JSON body takes precedence over the status code mapping,
    # because Airtable returns specific types (e.g. UNKNOWN_COLUMN_NAME) that
    # are more informative than the generic status-code-based type.
    def self.from_response(status_code, body)
      error_hash = parse_error_body(body)
      error_hash['type'] ||= STATUS_CODE_ERROR_TYPES.fetch(status_code, 'UNKNOWN_ERROR')
      error_hash['message'] ||= default_message_for(status_code, body)
      new(error_hash, status_code: status_code)
    end

    class << self
      private

      def parse_error_body(body)
        return {} if body.nil? || body.to_s.strip.empty?

        parsed = JSON.parse(body)
        parsed.is_a?(Hash) && parsed['error'].is_a?(Hash) ? parsed['error'] : {}
      rescue JSON::ParserError
        {}
      end

      def default_message_for(status_code, body)
        type = STATUS_CODE_ERROR_TYPES.fetch(status_code, 'UNKNOWN_ERROR')
        truncated = body.to_s[0..200]
        "#{type} (HTTP #{status_code}): #{truncated}"
      end
    end

  end
end
