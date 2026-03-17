module Airtable

  class Table < Resource
    # Maximum results per request
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
      response = self.class.get(worksheet_url, query: options)
      log_request(response, 'GET')
      results = response.parsed_response
      check_and_raise_error(results, status_code: response.code)
      RecordSet.new(results)
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

      response = self.class.get(worksheet_url, query: options)
      log_request(response, 'GET')
      results = response.parsed_response
      check_and_raise_error(results, status_code: response.code)
      RecordSet.new(results)
    end

    def raise_bad_formula_error
      raise ArgumentError.new("The value for filter should be a String.")
    end

    # Returns record based given row id
    def find(id)
      response = self.class.get(worksheet_url + "/" + id)
      log_request(response, 'GET')
      result = response.parsed_response
      check_and_raise_error(result, status_code: response.code)
      Record.new(result_attributes(result)) if result.present? && result["id"]
    end

    # Creates a record by posting to airtable
    def create(record)
      response = self.class.post(worksheet_url,
        :body => { "fields" => record.fields }.to_json,
        :headers => { "Content-type" => "application/json" })
      log_request(response, 'POST')
      result = response.parsed_response

      check_and_raise_error(result, status_code: response.code)

      record.override_attributes!(result_attributes(result))
      record
    end

    # Replaces record in airtable based on id
    def update(record)
      response = self.class.put(worksheet_url + "/" + record.id,
        :body => { "fields" => record.fields_for_update }.to_json,
        :headers => { "Content-type" => "application/json" })
      log_request(response, 'PUT')
      result = response.parsed_response

      check_and_raise_error(result, status_code: response.code)

      record.override_attributes!(result_attributes(result))
      record

    end

    def update_record_fields(record_id, fields_for_update)
      response = self.class.patch(worksheet_url + "/" + record_id,
        :body => { "fields" => fields_for_update }.to_json,
        :headers => { "Content-type" => "application/json" })
      log_request(response, 'PATCH')
      result = response.parsed_response

      check_and_raise_error(result, status_code: response.code)

      Record.new(result_attributes(result))
    end

    # Deletes record in table based on id
    def destroy(id)
      response = self.class.delete(worksheet_url + "/" + id)
      log_request(response, 'DELETE')
      response.parsed_response
    end

    protected

    def check_and_raise_error(response, status_code: nil)
      response['error'] ? raise(Error.new(response['error'], status_code: status_code)) : false
    end

    def log_request(response, http_method)
      if defined?(Rails)
        Rails.logger.info("[Airtable] #{response.code} #{http_method} #{worksheet_name}")
      end

      if defined?(NewRelic::Agent)
        NewRelic::Agent.add_custom_attributes(
          airtable_status_code: response.code,
          airtable_table: worksheet_name,
          airtable_method: http_method
        )
        NewRelic::Agent.record_custom_event('AirtableRequest', {
          status_code: response.code,
          table: worksheet_name,
          http_method: http_method
        })
      end
    end

    def result_attributes(res)
      res["fields"].merge("id" => res["id"]) if res.present? && res["id"]
    end

    def worksheet_url
      "/#{app_token}/#{url_encode(worksheet_name)}"
    end

    # From http://apidock.com/ruby/ERB/Util/url_encode
    def url_encode(s)
      s.to_s.dup.force_encoding("ASCII-8BIT").gsub(/[^a-zA-Z0-9_\-.]/) {
        sprintf("%%%02X", $&.unpack("C")[0])
      }
    end
  end # Table

end # Airtable
