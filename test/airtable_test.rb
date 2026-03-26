require 'test_helper'

describe Airtable do
  before do
    @client_key = "12345"
    @app_key = "appXXV84Qu"
    @sheet_name = "Test"
  end

  describe "client" do
    it "should allow client to be created" do
      @client = Airtable::Client.new(@client_key)
      assert_kind_of Airtable::Client, @client
      @table = @client.table(@app_key, @sheet_name)
      assert_kind_of Airtable::Table, @table
    end

    it "should accept a custom timeout" do
      @client = Airtable::Client.new(@client_key, timeout: 10)
      @table = @client.table(@app_key, @sheet_name)
      assert_kind_of Airtable::Table, @table
    end

    it "should use the default timeout when none is specified" do
      @table = Airtable::Table.new(@client_key, @app_key, @sheet_name)
      assert_kind_of Airtable::Table, @table
    end
  end

  describe "records" do
    it "should fetch record set" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}", { "records" => [], "offset" => "abcde" })
      @table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      @records = @table.records
      assert_equal "abcde", @records.offset
    end

    it "should fetch records with sort options" do
      stub_request(:get, /https:\/\/api\.airtable\.com\/v0\/#{@app_key}\/#{@sheet_name}/)
        .with(query: hash_including({ 'sortField' => 'Name', 'sortDirection' => 'desc' }))
        .to_return(
          body: { "records" => [{ "fields" => { "Name" => "Alice" }, "id" => "rec1" }] }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )
      @table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      @records = @table.records(sort: ["Name", "desc"])
      assert_equal 1, @records.records.length
    end
  end

  describe "select" do
    it "should select records based on a formula" do
      query_str = "OR(RECORD_ID() = 'recXYZ1', RECORD_ID() = 'recXYZ2')"
      stub_request(:get, /https:\/\/api\.airtable\.com\/v0\/#{@app_key}\/#{@sheet_name}/)
        .with(query: hash_including({ 'filterByFormula' => query_str }))
        .to_return(
          body: { "records" => [] }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )
      @table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      @select_records = @table.select(formula: query_str)
      assert_equal @select_records.records, []
    end

    it "should raise an ArgumentError if a formula is not a string" do
      @table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      assert_raises ArgumentError do
        @table.select(formula: {foo: 'bar'})
      end
    end
  end

  describe "find" do
    it "should find a record by id" do
      record_id = "rec456"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "fields" => { "name" => "Found Record" }, "id" => record_id })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = table.find(record_id)
      assert_equal record_id, record["id"]
    end
  end

  describe "create" do
    it "should allow creating records" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}",
        { "fields" => { "name" => "Sarah Jaine", "email" => "sarah@jaine.com", "foo" => "bar" }, "id" => "12345" }, :post)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:name => "Sarah Jaine", :email => "sarah@jaine.com")
      table.create(record)
      assert_equal "12345", record["id"]
      assert_equal "bar", record["foo"]
    end
  end

  describe "update" do
    it "should allow updating records with PUT" do
      record_id = "12345"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "fields" => { "name" => "Sarah Jaine", "email" => "sarah@jaine.com", "foo" => "bar" }, "id" => record_id }, :put)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:name => "Sarah Jaine", :email => "sarah@jaine.com", :id => record_id)
      table.update(record)
      assert_equal "12345", record["id"]
      assert_equal "bar", record["foo"]
    end

    it "should allow patching record fields" do
      record_id = "rec123"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "fields" => { "name" => "Updated Name" }, "id" => record_id }, :patch)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.update_record_fields(record_id, { "name" => "Updated Name" })
      assert_equal record_id, result["id"]
    end
  end

  describe "destroy" do
    it "should allow deleting a record" do
      record_id = "rec789"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "deleted" => true, "id" => record_id }, :delete)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.destroy(record_id)
      assert_equal true, result["deleted"]
    end

    it "should raise an error when delete returns an error" do
      record_id = "rec789"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "error" => { "type" => "NOT_FOUND", "message" => "Record not found" } }, :delete, 404)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      assert_raises Airtable::Error do
        table.destroy(record_id)
      end
    end
  end

  describe "error handling" do
    it "should raise an error when the API returns an error" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}",
        {"error"=>{"type"=>"UNKNOWN_COLUMN_NAME", "message"=>"Could not find fields foo"}}, :post, 422)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:foo => "bar")
      error = assert_raises Airtable::Error do
        table.create(record)
      end
      assert_equal "UNKNOWN_COLUMN_NAME", error.type
      assert_equal 422, error.status_code
    end

    it "should include the status code in errors" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}",
        {"error"=>{"type"=>"INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND", "message"=>"Invalid permissions"}}, :post, 403)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:foo => "bar")
      error = assert_raises Airtable::Error do
        table.create(record)
      end
      assert_equal 403, error.status_code
      assert_equal "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND", error.type
    end

    it "should raise a classified error for non-JSON response bodies" do
      stub_request(:post, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}")
        .to_return(
          body: '<html>502 Bad Gateway</html>',
          status: 502,
          headers: { 'Content-Type' => 'text/html' }
        )
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:foo => "bar")
      error = assert_raises Airtable::Error do
        table.create(record)
      end
      assert_equal "UNKNOWN_ERROR", error.type
      assert_equal 502, error.status_code
      assert_includes error.message, "502 Bad Gateway"
    end

    it "should return nil for empty response bodies on find" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(
          body: '',
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.find("rec123")
      assert_nil result
    end

    it "should return nil for nil response bodies on find" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(
          body: nil,
          status: 204,
          headers: {}
        )
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.find("rec123")
      assert_nil result
    end
  end

  describe "authorisation" do
    it "should set authorisation header on requests" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .with(headers: { 'Authorization' => "Bearer #{@client_key}" })
        .to_return(
          body: { "fields" => { "name" => "Test" }, "id" => "rec123" }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = table.find("rec123")
      assert_equal "rec123", record["id"]
    end
  end

  describe "timeouts" do
    it "should raise Net::OpenTimeout on connection timeout" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_timeout
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      assert_raises Net::OpenTimeout do
        table.find("rec123")
      end
    end

    it "should raise Net::ReadTimeout on read timeout" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_raise(Net::ReadTimeout)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      assert_raises Net::ReadTimeout do
        table.find("rec123")
      end
    end
  end

  describe "connection resilience" do
    it "should recover from a stale connection" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)

      record = table.find("rec123")
      assert_equal "rec123", record["id"]

      record = table.find("rec123")
      assert_equal "rec123", record["id"]
    end

    it "should retry once on connection reset" do
      call_count = 0
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return do |_request|
          call_count += 1
          if call_count == 1
            raise Errno::ECONNRESET
          else
            { body: { "fields" => { "name" => "Test" }, "id" => "rec123" }.to_json,
              status: 200,
              headers: { 'Content-Type' => 'application/json' } }
          end
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = table.find("rec123")
      assert_equal "rec123", record["id"]
      assert_equal 2, call_count
    end

    it "should raise after retrying on persistent connection failure" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_raise(Errno::ECONNRESET)

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      assert_raises Errno::ECONNRESET do
        table.find("rec123")
      end
    end
  end

  describe "instrumentation" do
    it "should track request duration in milliseconds" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.find("rec123")
      duration_ms = table.instance_variable_get(:@last_request_duration_ms)
      assert_kind_of Integer, duration_ms
      assert duration_ms >= 0, "duration_ms should be non-negative, got #{duration_ms}"
    end

    it "should track request body size for POST requests" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}",
        { "fields" => { "name" => "Sarah" }, "id" => "rec1" }, :post)
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:name => "Sarah")
      table.create(record)
      request_body_size = table.instance_variable_get(:@last_request_body_size)
      assert request_body_size > 0, "request body size should be positive for POST, got #{request_body_size}"
    end

    it "should track response body size" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.find("rec123")
      response_body_size = table.instance_variable_get(:@last_response_body_size)
      assert response_body_size > 0, "response body size should be positive, got #{response_body_size}"
    end

    it "should track zero request body size for GET requests" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.find("rec123")
      request_body_size = table.instance_variable_get(:@last_request_body_size)
      assert_equal 0, request_body_size
    end

    it "should log connection retry with error details" do
      call_count = 0
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return do |_request|
          call_count += 1
          if call_count == 1
            raise Errno::ECONNRESET
          else
            { body: { "fields" => { "name" => "Test" }, "id" => "rec123" }.to_json,
              status: 200,
              headers: { 'Content-Type' => 'application/json' } }
          end
        end

      stderr_output = StringIO.new
      original_stderr = $stderr
      $stderr = stderr_output

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.find("rec123")

      $stderr = original_stderr
      assert_includes stderr_output.string, '[Airtable] Connection reset'
      assert_includes stderr_output.string, 'Errno::ECONNRESET'
      assert_includes stderr_output.string, 'retrying GET'
    end
  end

  describe "error logging in responses" do
    it "should include error_type and error_message in log output when Airtable returns an error" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}",
        { "error" => { "type" => "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND", "message" => "Invalid permissions, or the requested model was not found" } }, :post, 403)

      stderr_output = StringIO.new
      original_stderr = $stderr
      $stderr = stderr_output

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:name => "Test")

      assert_raises Airtable::Error do
        table.create(record)
      end

      $stderr = original_stderr
      assert_includes stderr_output.string, 'error_type=INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND'
      assert_includes stderr_output.string, 'error_message=Invalid permissions, or the requested model was not found'
    end

    it "should not include error fields in log output for successful responses" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })

      stderr_output = StringIO.new
      original_stderr = $stderr
      $stderr = stderr_output

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.find("rec123")

      $stderr = original_stderr
      refute_includes stderr_output.string, 'error_type='
      refute_includes stderr_output.string, 'error_message='
    end

    it "should include error details for GET requests returning 403" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123",
        { "error" => { "type" => "MODEL_ID_NOT_FOUND", "message" => "Could not find record" } }, :get, 403)

      stderr_output = StringIO.new
      original_stderr = $stderr
      $stderr = stderr_output

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)

      assert_raises Airtable::Error do
        table.find("rec123")
      end

      $stderr = original_stderr
      assert_includes stderr_output.string, 'error_type=MODEL_ID_NOT_FOUND'
    end

    it "should include error details for PATCH requests with unknown columns" do
      record_id = "rec123"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "error" => { "type" => "UNKNOWN_COLUMN_NAME", "message" => "Could not find fields Premium Type (protection)" } }, :patch, 422)

      stderr_output = StringIO.new
      original_stderr = $stderr
      $stderr = stderr_output

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)

      assert_raises Airtable::Error do
        table.update_record_fields(record_id, { "Premium Type (protection)" => "Life" })
      end

      $stderr = original_stderr
      assert_includes stderr_output.string, 'error_type=UNKNOWN_COLUMN_NAME'
      assert_includes stderr_output.string, 'error_message=Could not find fields Premium Type (protection)'
    end

    it "should include error details for DELETE requests on missing records" do
      record_id = "rec789"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/#{record_id}",
        { "error" => { "type" => "NOT_FOUND", "message" => "Record not found" } }, :delete, 404)

      stderr_output = StringIO.new
      original_stderr = $stderr
      $stderr = stderr_output

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)

      assert_raises Airtable::Error do
        table.destroy(record_id)
      end

      $stderr = original_stderr
      assert_includes stderr_output.string, 'error_type=NOT_FOUND'
    end

    it "should preserve the error type and message on the raised exception" do
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}",
        { "error" => { "type" => "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND", "message" => "Invalid permissions, or the requested model was not found" } }, :post, 403)

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(:name => "Test")

      error = assert_raises Airtable::Error do
        table.create(record)
      end

      assert_equal "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND", error.type
      assert_equal "Invalid permissions, or the requested model was not found", error.message
      assert_equal 403, error.status_code
    end
  end

  describe "worksheet names with special characters" do
    it "should encode spaces in worksheet names" do
      sheet_name = "My Table"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/My%20Table/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })
      table = Airtable::Client.new(@client_key).table(@app_key, sheet_name)
      record = table.find("rec123")
      assert_equal "rec123", record["id"]
    end

    it "should encode ampersands in worksheet names" do
      sheet_name = "Plans & Users"
      stub_airtable_response!("https://api.airtable.com/v0/#{@app_key}/Plans%20%26%20Users/rec123",
        { "fields" => { "name" => "Test" }, "id" => "rec123" })
      table = Airtable::Client.new(@client_key).table(@app_key, sheet_name)
      record = table.find("rec123")
      assert_equal "rec123", record["id"]
    end
  end

  describe "error classification by HTTP status code" do
    it "should classify 401 as AUTHENTICATION_REQUIRED" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '{"error":{"message":"Unauthorized"}}', status: 401, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'AUTHENTICATION_REQUIRED', error.type
      assert_equal 401, error.status_code
    end

    it "should classify 403 as NOT_AUTHORIZED when no type in body" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '{"error":{"message":"Forbidden"}}', status: 403, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'NOT_AUTHORIZED', error.type
      assert_equal 403, error.status_code
    end

    it "should classify 404 as NOT_FOUND" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '{"error":{"message":"Not found"}}', status: 404, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'NOT_FOUND', error.type
      assert_equal 404, error.status_code
    end

    it "should classify 422 as INVALID_REQUEST" do
      stub_request(:post, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}")
        .to_return(body: '{"error":{"message":"Invalid request"}}', status: 422, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(name: "Test")
      error = assert_raises(Airtable::Error) { table.create(record) }
      assert_equal 'INVALID_REQUEST', error.type
      assert_equal 422, error.status_code
    end

    it "should classify 429 as TOO_MANY_REQUESTS" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '{"error":{"message":"Rate limited"}}', status: 429, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'TOO_MANY_REQUESTS', error.type
      assert_equal 429, error.status_code
    end

    it "should classify 500 as SERVER_ERROR" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '{"error":{"message":"Internal server error"}}', status: 500, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'SERVER_ERROR', error.type
      assert_equal 500, error.status_code
    end

    it "should classify 503 as SERVICE_UNAVAILABLE" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '<html>503 Service Unavailable</html>', status: 503, headers: { 'Content-Type' => 'text/html' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'SERVICE_UNAVAILABLE', error.type
      assert_equal 503, error.status_code
    end

    it "should preserve Airtable-specific type from JSON body over status code mapping" do
      stub_request(:post, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}")
        .to_return(body: '{"error":{"type":"UNKNOWN_COLUMN_NAME","message":"Could not find fields foo"}}', status: 422, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(foo: "bar")
      error = assert_raises(Airtable::Error) { table.create(record) }
      assert_equal 'UNKNOWN_COLUMN_NAME', error.type
      assert_equal "Could not find fields foo", error.message
    end

    it "should classify HTML 502 as UNKNOWN_ERROR (unmapped status code)" do
      stub_request(:post, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}")
        .to_return(body: '<html>502 Bad Gateway</html>', status: 502, headers: { 'Content-Type' => 'text/html' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      record = Airtable::Record.new(foo: "bar")
      error = assert_raises(Airtable::Error) { table.create(record) }
      assert_equal 'UNKNOWN_ERROR', error.type
      assert_equal 502, error.status_code
    end

    it "should classify empty body with error status code" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '', status: 401, headers: {})
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'AUTHENTICATION_REQUIRED', error.type
      assert_equal 401, error.status_code
    end

    it "should classify nil body with error status code" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: nil, status: 500, headers: {})
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'SERVER_ERROR', error.type
      assert_equal 500, error.status_code
    end

    it "should classify unknown status code as UNKNOWN_ERROR" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: 'I am a teapot', status: 418, headers: {})
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'UNKNOWN_ERROR', error.type
      assert_equal 418, error.status_code
    end

    it "should raise on JSON without error hash but with error status code" do
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: '{"ok":false}', status: 500, headers: { 'Content-Type' => 'application/json' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert_equal 'SERVER_ERROR', error.type
      assert_equal 500, error.status_code
    end

    it "should truncate long HTML bodies in error messages" do
      long_html = '<html>' + ('x' * 500) + '</html>'
      stub_request(:get, "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}/rec123")
        .to_return(body: long_html, status: 503, headers: { 'Content-Type' => 'text/html' })
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      error = assert_raises(Airtable::Error) { table.find("rec123") }
      assert error.message.length <= 250, "Error message should be truncated, got #{error.message.length} chars"
    end
  end

  describe "Error.from_response" do
    it "should build error from status code and HTML body" do
      error = Airtable::Error.from_response(503, '<html>outage</html>')
      assert_equal 'SERVICE_UNAVAILABLE', error.type
      assert_equal 503, error.status_code
      assert_includes error.message, 'outage'
    end

    it "should build error from status code and nil body" do
      error = Airtable::Error.from_response(401, nil)
      assert_equal 'AUTHENTICATION_REQUIRED', error.type
      assert_equal 401, error.status_code
    end

    it "should build error from status code and empty body" do
      error = Airtable::Error.from_response(401, '')
      assert_equal 'AUTHENTICATION_REQUIRED', error.type
      assert_equal 401, error.status_code
    end

    it "should preserve JSON body type over status code mapping" do
      error = Airtable::Error.from_response(422, '{"error":{"type":"CUSTOM_TYPE","message":"custom msg"}}')
      assert_equal 'CUSTOM_TYPE', error.type
      assert_equal 'custom msg', error.message
    end

    it "should fall back to UNKNOWN_ERROR for unmapped status codes" do
      error = Airtable::Error.from_response(999, 'garbage')
      assert_equal 'UNKNOWN_ERROR', error.type
      assert_equal 999, error.status_code
    end
  end
end
