require 'test_helper'

describe "query parameters and formula escaping" do
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

  describe "fields parameter" do
    it "should include fields[] in select query" do
      requested_url = nil
      stub_request(:get, /#{Regexp.escape(@base_url)}/)
        .to_return do |request|
          requested_url = request.uri.to_s
          { body: { "records" => [] }.to_json, status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.select(fields: ['Name', 'Email'])

      assert_includes requested_url, "fields%5B%5D=Name"
      assert_includes requested_url, "fields%5B%5D=Email"
    end

    it "should include fields[] in records query" do
      requested_url = nil
      stub_request(:get, /#{Regexp.escape(@base_url)}/)
        .to_return do |request|
          requested_url = request.uri.to_s
          { body: { "records" => [] }.to_json, status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.records(fields: ['Name'])

      assert_includes requested_url, "fields%5B%5D=Name"
    end
  end

  describe "view parameter" do
    it "should include view in select query" do
      requested_url = nil
      stub_request(:get, /#{Regexp.escape(@base_url)}/)
        .to_return do |request|
          requested_url = request.uri.to_s
          { body: { "records" => [] }.to_json, status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.select(view: 'Main View')

      assert_includes requested_url, "view=Main"
    end

    it "should include view in records query" do
      requested_url = nil
      stub_request(:get, /#{Regexp.escape(@base_url)}/)
        .to_return do |request|
          requested_url = request.uri.to_s
          { body: { "records" => [] }.to_json, status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.records(view: 'Grid view')

      assert_includes requested_url, "view=Grid"
    end
  end

  describe "combined parameters" do
    it "should support fields, view, and formula together" do
      requested_url = nil
      stub_request(:get, /#{Regexp.escape(@base_url)}/)
        .to_return do |request|
          requested_url = request.uri.to_s
          { body: { "records" => [] }.to_json, status: 200, headers: { 'Content-Type' => 'application/json' } }
        end

      table = Airtable::Client.new(@client_key).table(@app_key, @sheet_name)
      table.select(fields: ['Name', 'Email'], view: 'Main View', formula: '{Status} = "Active"')

      assert_includes requested_url, "fields%5B%5D=Name"
      assert_includes requested_url, "fields%5B%5D=Email"
      assert_includes requested_url, "view=Main"
      assert_includes requested_url, "filterByFormula="
    end
  end

  describe "Airtable.escape_formula_value" do
    it "should wrap plain string in single quotes" do
      assert_equal "'Alice'", Airtable.escape_formula_value("Alice")
    end

    it "should escape single quotes" do
      assert_equal "'O\\'Brien'", Airtable.escape_formula_value("O'Brien")
    end

    it "should escape backslashes" do
      assert_equal "'path\\\\to'", Airtable.escape_formula_value("path\\to")
    end

    it "should escape both single quotes and backslashes" do
      assert_equal "'it\\'s a \\\\test'", Airtable.escape_formula_value("it's a \\test")
    end

    it "should convert non-string values" do
      assert_equal "'123'", Airtable.escape_formula_value(123)
    end

    it "should handle nil" do
      assert_equal "''", Airtable.escape_formula_value(nil)
    end

    it "should handle empty string" do
      assert_equal "''", Airtable.escape_formula_value("")
    end

    it "should produce a safe formula when used with select" do
      email = "user's@test.com"
      formula = "{Email} = #{Airtable.escape_formula_value(email)}"
      assert_equal "{Email} = 'user\\'s@test.com'", formula
    end
  end
end
