require 'test_helper'

describe "batch operations" do
  before do
    @client_key = "12345"
    @app_key = "appXXV84Qu"
    @sheet_name = "Test"
    @base_url = "https://api.airtable.com/v0/#{@app_key}/#{@sheet_name}"
    Airtable::RateLimiter.instance = Airtable::RateLimiter.new(max_requests: 1000, window_seconds: 1.0)
  end

  after do
    Airtable::RateLimiter.reset!
  end

  describe "create_batch" do
    it "should create records in a single request when <= 10" do
      records = 3.times.map { |i| Airtable::Record.new(name: "Record #{i}") }
      stub_request(:post, @base_url)
        .to_return(
          body: { "records" => 3.times.map { |i| { "id" => "rec#{i}", "fields" => { "name" => "Record #{i}" } } } }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.create_batch(records)

      assert result.all_succeeded?
      assert_equal 3, result.successes.length
      assert_empty result.failures
      assert_equal "rec0", result.successes[0]["id"]
    end

    it "should auto-chunk into groups of 10" do
      records = 15.times.map { |i| Airtable::Record.new(name: "Record #{i}") }
      call_count = 0
      stub_request(:post, @base_url)
        .to_return do |request|
          call_count += 1
          body = JSON.parse(request.body)
          count = body["records"].length
          {
            body: { "records" => count.times.map { |i| { "id" => "rec#{call_count}_#{i}", "fields" => { "name" => "R" } } } }.to_json,
            status: 200,
            headers: { 'Content-Type' => 'application/json' }
          }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.create_batch(records)

      assert_equal 2, call_count, "15 records should produce 2 requests (10 + 5)"
      assert_equal 15, result.successes.length
      assert result.all_succeeded?
    end

    it "should handle partial failure across chunks" do
      records = 15.times.map { |i| Airtable::Record.new(name: "Record #{i}") }
      call_count = 0
      stub_request(:post, @base_url)
        .to_return do |_request|
          call_count += 1
          if call_count == 1
            { body: { "records" => 10.times.map { |i| { "id" => "rec#{i}", "fields" => { "name" => "R" } } } }.to_json,
              status: 200, headers: { 'Content-Type' => 'application/json' } }
          else
            { body: { "error" => { "type" => "INVALID_REQUEST", "message" => "Bad data" } }.to_json,
              status: 422, headers: { 'Content-Type' => 'application/json' } }
          end
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.create_batch(records)

      refute result.all_succeeded?
      assert_equal 10, result.successes.length
      assert_equal 5, result.failures.length
      assert_equal "INVALID_REQUEST", result.failures.first[:error].type
    end

    it "should return empty result for empty input" do
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.create_batch([])
      assert result.all_succeeded?
      assert_empty result.successes
    end

    it "should return empty result for nil input" do
      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.create_batch(nil)
      assert result.all_succeeded?
      assert_empty result.successes
    end

    it "should send correct POST body format" do
      records = [Airtable::Record.new(name: "Alice", email: "alice@example.com")]
      request_body = nil
      stub_request(:post, @base_url)
        .to_return do |request|
          request_body = JSON.parse(request.body)
          { body: { "records" => [{ "id" => "rec1", "fields" => { "name" => "Alice" } }] }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.create_batch(records)

      assert request_body.key?("records"), "Body should contain 'records' key"
      assert_equal 1, request_body["records"].length
      assert request_body["records"][0].key?("fields"), "Each record should have 'fields'"
      refute request_body["records"][0].key?("id"), "Create should not include id"
    end
  end

  describe "update_batch" do
    it "should update records with PATCH" do
      records = 3.times.map { |i| Airtable::Record.new(name: "Updated #{i}", id: "rec#{i}") }
      request_body = nil
      stub_request(:patch, @base_url)
        .to_return do |request|
          request_body = JSON.parse(request.body)
          { body: { "records" => 3.times.map { |i| { "id" => "rec#{i}", "fields" => { "name" => "Updated #{i}" } } } }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.update_batch(records)

      assert result.all_succeeded?
      assert_equal 3, result.successes.length
      assert request_body["records"][0].key?("id"), "Update should include id"
      assert request_body["records"][0].key?("fields"), "Update should include fields"
    end

    it "should auto-chunk updates into groups of 10" do
      records = 22.times.map { |i| Airtable::Record.new(name: "R#{i}", id: "rec#{i}") }
      call_count = 0
      stub_request(:patch, @base_url)
        .to_return do |request|
          call_count += 1
          body = JSON.parse(request.body)
          count = body["records"].length
          { body: { "records" => count.times.map { |i| { "id" => "rec#{i}", "fields" => {} } } }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.update_batch(records)

      assert_equal 3, call_count, "22 records should produce 3 requests (10 + 10 + 2)"
      assert_equal 22, result.successes.length
    end
  end

  describe "destroy_batch" do
    it "should delete records with query params" do
      ids = ["rec1", "rec2", "rec3"]
      stub_request(:delete, /#{Regexp.escape(@base_url)}\?records/)
        .to_return(
          body: { "records" => ids.map { |id| { "id" => id, "deleted" => true } } }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.destroy_batch(ids)

      assert result.all_succeeded?
      assert_equal 3, result.successes.length
    end

    it "should auto-chunk deletes into groups of 10" do
      ids = 12.times.map { |i| "rec#{i}" }
      call_count = 0
      stub_request(:delete, /#{Regexp.escape(@base_url)}\?records/)
        .to_return do |_request|
          call_count += 1
          chunk_size = call_count == 1 ? 10 : 2
          { body: { "records" => chunk_size.times.map { |i| { "id" => "rec#{i}", "deleted" => true } } }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.destroy_batch(ids)

      assert_equal 2, call_count, "12 IDs should produce 2 requests (10 + 2)"
      assert_equal 12, result.successes.length
    end

    it "should handle delete errors" do
      ids = ["rec1", "rec2"]
      stub_request(:delete, /#{Regexp.escape(@base_url)}\?records/)
        .to_return(
          body: { "error" => { "type" => "NOT_FOUND", "message" => "Records not found" } }.to_json,
          status: 404,
          headers: { 'Content-Type' => 'application/json' }
        )

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.destroy_batch(ids)

      refute result.all_succeeded?
      assert_equal 2, result.failures.length
      assert_equal "NOT_FOUND", result.failures.first[:error].type
    end

    it "should encode record IDs in query params" do
      ids = ["recABC", "recDEF"]
      stub_request(:delete, "#{@base_url}?records[]=recABC&records[]=recDEF")
        .to_return(
          body: { "records" => ids.map { |id| { "id" => id, "deleted" => true } } }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.destroy_batch(ids)

      assert result.all_succeeded?
    end
  end

  describe "upsert" do
    it "should send performUpsert in request body" do
      records = [Airtable::Record.new(name: "Alice", email: "alice@example.com")]
      request_body = nil
      stub_request(:patch, @base_url)
        .to_return do |request|
          request_body = JSON.parse(request.body)
          { body: { "records" => [{ "id" => "rec1", "fields" => { "name" => "Alice" } }], "createdRecords" => ["rec1"] }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.upsert(records, fields_to_merge_on: ["Email"])

      assert request_body.key?("performUpsert"), "Body should contain performUpsert"
      assert_equal ["Email"], request_body["performUpsert"]["fieldsToMergeOn"]
      assert_equal 1, result.successes.length
      assert_equal ["rec1"], result.created_record_ids
    end

    it "should track which records were created vs updated" do
      records = [
        Airtable::Record.new(name: "Alice", email: "alice@example.com"),
        Airtable::Record.new(name: "Bob", email: "bob@example.com")
      ]
      stub_request(:patch, @base_url)
        .to_return(
          body: {
            "records" => [
              { "id" => "recExisting", "fields" => { "name" => "Alice" } },
              { "id" => "recNew", "fields" => { "name" => "Bob" } }
            ],
            "createdRecords" => ["recNew"]
          }.to_json,
          status: 200,
          headers: { 'Content-Type' => 'application/json' }
        )

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.upsert(records, fields_to_merge_on: ["Email"])

      assert_equal 2, result.successes.length
      assert_equal ["recNew"], result.created_record_ids
    end

    it "should auto-chunk upserts into groups of 10" do
      records = 15.times.map { |i| Airtable::Record.new(name: "R#{i}", email: "r#{i}@test.com") }
      call_count = 0
      stub_request(:patch, @base_url)
        .to_return do |request|
          call_count += 1
          body = JSON.parse(request.body)
          count = body["records"].length
          assert body.key?("performUpsert"), "Each chunk should include performUpsert"
          { body: { "records" => count.times.map { |i| { "id" => "rec#{i}", "fields" => {} } }, "createdRecords" => [] }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.upsert(records, fields_to_merge_on: ["Email"])

      assert_equal 2, call_count, "15 records should produce 2 requests"
      assert_equal 15, result.successes.length
    end

    it "should handle upsert partial failure" do
      records = 15.times.map { |i| Airtable::Record.new(name: "R#{i}") }
      call_count = 0
      stub_request(:patch, @base_url)
        .to_return do |_request|
          call_count += 1
          if call_count == 1
            { body: { "records" => 10.times.map { |i| { "id" => "rec#{i}", "fields" => {} } }, "createdRecords" => [] }.to_json,
              status: 200, headers: { 'Content-Type' => 'application/json' } }
          else
            { body: { "error" => { "type" => "INVALID_REQUEST", "message" => "Bad" } }.to_json,
              status: 422, headers: { 'Content-Type' => 'application/json' } }
          end
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      result = table.upsert(records, fields_to_merge_on: ["Name"])

      refute result.all_succeeded?
      assert_equal 10, result.successes.length
      assert_equal 5, result.failures.length
    end

    it "should support multiple merge fields" do
      records = [Airtable::Record.new(name: "Alice", email: "alice@test.com")]
      request_body = nil
      stub_request(:patch, @base_url)
        .to_return do |request|
          request_body = JSON.parse(request.body)
          { body: { "records" => [{ "id" => "rec1", "fields" => {} }], "createdRecords" => [] }.to_json,
            status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.upsert(records, fields_to_merge_on: ["Email", "Company"])

      assert_equal ["Email", "Company"], request_body["performUpsert"]["fieldsToMergeOn"]
    end
  end

  describe "BatchResult" do
    it "should track successes and failures" do
      result = Airtable::BatchResult.new
      result.add_success(Airtable::Record.new(name: "Alice"))
      result.add_failure(Airtable::Record.new(name: "Bob"), StandardError.new("fail"))

      refute result.all_succeeded?
      assert_equal 1, result.successes.length
      assert_equal 1, result.failures.length
      assert_equal "fail", result.failures.first[:error].message
    end

    it "should report all_succeeded? when no failures" do
      result = Airtable::BatchResult.new
      result.add_success(Airtable::Record.new(name: "Alice"))

      assert result.all_succeeded?
    end

    it "should track created record IDs" do
      result = Airtable::BatchResult.new
      result.add_created_ids(["rec1", "rec2"])
      result.add_created_ids(["rec3"])

      assert_equal ["rec1", "rec2", "rec3"], result.created_record_ids
    end
  end
end
