module Airtable

  class Table < Resource
    LIMIT_MAX = 100

    # Fetch all records iterating through offsets until retrieving the entire collection
    # all(:sort => ["Name", :desc])
    def all(options={})
      offset = nil
      results = []
      begin
        options.merge!(:limit => LIMIT_MAX, :offset => offset)
        response = records(options)
        results += response.records
        offset = response.offset
      end until offset.nil? || offset.empty? || results.empty?
      results
    end

    # Fetch records from the sheet given the list options
    # Options: limit = 100, offset = "as345g", sort = ["Name", "asc"]
    # records(:sort => ["Name", :desc], :limit => 50, :offset => "as345g")
    def records(options={})
      options["sortField"], options["sortDirection"] = options.delete(:sort) if options[:sort]
      request = build_get_request(worksheet_url, query: options)
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'GET', parsed_result: result)
      check_and_raise_error(result, status_code: response.code.to_i)
      RecordSet.new(result)
    end

    # Query for records using a string formula
    # Options: limit = 100, offset = "as345g", sort = ["Name", "asc"],
    #          fields = [Name, Email], formula = "Count > 5", view = "Main View"
    #
    # select(limit: 10, sort: ["Name", "asc"], formula: "Order < 2")
    def select(options={})
      options['sortField'], options['sortDirection'] = options.delete(:sort) if options[:sort]
      options['maxRecords'] = options.delete(:limit) if options[:limit]

      if options[:formula]
        raise_bad_formula_error unless options[:formula].is_a? String
        options['filterByFormula'] = options.delete(:formula)
      end

      request = build_get_request(worksheet_url, query: options)
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'GET', parsed_result: result)
      check_and_raise_error(result, status_code: response.code.to_i)
      RecordSet.new(result)
    end

    def raise_bad_formula_error
      raise ArgumentError.new("The value for filter should be a String.")
    end

    # Returns record based given row id
    def find(id)
      request = build_get_request("#{worksheet_url}/#{id}")
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'GET', parsed_result: result)
      check_and_raise_error(result, status_code: response.code.to_i)
      Record.new(result_attributes(result)) if result.present? && result["id"]
    end

    # Creates a record by posting to airtable
    def create(record)
      request = build_post_request(worksheet_url, body: { "fields" => record.fields })
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'POST', parsed_result: result)

      check_and_raise_error(result, status_code: response.code.to_i)

      record.override_attributes!(result_attributes(result))
      record
    end

    # Replaces record in airtable based on id
    def update(record)
      request = build_put_request("#{worksheet_url}/#{record.id}", body: { "fields" => record.fields_for_update })
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'PUT', parsed_result: result)

      check_and_raise_error(result, status_code: response.code.to_i)

      record.override_attributes!(result_attributes(result))
      record
    end

    def update_record_fields(record_id, fields_for_update)
      request = build_patch_request("#{worksheet_url}/#{record_id}", body: { "fields" => fields_for_update })
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'PATCH', parsed_result: result)

      check_and_raise_error(result, status_code: response.code.to_i)

      Record.new(result_attributes(result))
    end

    # Deletes record in table based on id
    def destroy(id)
      request = build_delete_request("#{worksheet_url}/#{id}")
      response = perform_request(request)
      result = parse_response(response)
      log_response(response, 'DELETE', parsed_result: result)
      check_and_raise_error(result, status_code: response.code.to_i)
      result
    end

    protected

    def check_and_raise_error(result, status_code: nil)
      if result.is_a?(Hash) && result['error'].is_a?(Hash)
        error_hash = result['error']
        # Preserve Airtable's specific type if present, otherwise classify by status code
        error_hash['type'] ||= Error::STATUS_CODE_ERROR_TYPES.fetch(status_code, 'UNKNOWN_ERROR')
        raise Error.new(error_hash, status_code: status_code)
      end

      # Raise on non-2xx status codes even when the parsed result has no error hash
      # (or has a non-Hash error value like a string).
      if status_code && status_code >= 400
        raise Error.from_response(status_code, nil)
      end
    end

    def log_response(response, http_method, parsed_result: nil)
      status_code = response.code.to_i
      duration_ms = @last_request_duration_ms || 0
      request_body_size = @last_request_body_size || 0
      response_body_size = @last_response_body_size || 0
      error_hash = parsed_result.is_a?(Hash) && parsed_result['error'].is_a?(Hash) ? parsed_result['error'] : nil
      error_type = error_hash&.dig('type')
      error_message = error_hash&.dig('message')

      log_line = "[Airtable] #{status_code} #{http_method} #{worksheet_name} #{duration_ms}ms request=#{request_body_size}b response=#{response_body_size}b"
      log_line += " error_type=#{error_type} error_message=#{error_message}" if error_type

      if defined?(Rails)
        Rails.logger.info(log_line)
      else
        $stderr.puts(log_line)
      end

      if defined?(NewRelic::Agent)
        attributes = {
          airtable_status_code: status_code,
          airtable_table: worksheet_name,
          airtable_method: http_method,
          airtable_duration_ms: duration_ms,
          airtable_request_body_size: request_body_size,
          airtable_response_body_size: response_body_size
        }
        attributes[:airtable_error_type] = error_type if error_type
        attributes[:airtable_error_message] = error_message if error_message

        NewRelic::Agent.add_custom_attributes(attributes)
        NewRelic::Agent.record_custom_event('AirtableRequest', {
          status_code: status_code,
          table: worksheet_name,
          http_method: http_method,
          duration_ms: duration_ms,
          request_body_size: request_body_size,
          response_body_size: response_body_size,
          error_type: error_type,
          error_message: error_message
        })
      end
    end

    def parse_response(response)
      body = response.body
      return {} if body.nil? || body.empty?

      JSON.parse(body)
    rescue JSON::ParserError
      raise Error.from_response(response.code.to_i, body)
    end

    def result_attributes(res)
      res["fields"].merge("id" => res["id"]) if res.present? && res["id"]
    end

    def worksheet_url
      "#{BASE_PATH}/#{app_token}/#{url_encode(worksheet_name)}"
    end

    # From http://apidock.com/ruby/ERB/Util/url_encode
    def url_encode(s)
      s.to_s.dup.force_encoding("ASCII-8BIT").gsub(/[^a-zA-Z0-9_\-.]/) {
        sprintf("%%%02X", $&.unpack("C")[0])
      }
    end
  end

end
