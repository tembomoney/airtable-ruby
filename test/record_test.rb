require 'test_helper'

describe Airtable do
  describe Airtable::Record do
    it "should not return id in fields_for_update" do
      record = Airtable::Record.new(:name => "Sarah Jaine", :email => "sarah@jaine.com", :id => 12345)
      refute_includes record.fields_for_update.keys, :id
    end

    it "returns new columns in fields_for_update" do
      record = Airtable::Record.new(:name => "Sarah Jaine", :email => "sarah@jaine.com", :id => 12345)
      record[:website] = "http://sarahjaine.com"
      assert record.fields_for_update.key?(:website), "Expected fields_for_update to include :website"
    end

    it "returns fields_for_update in original capitalization" do
      record = Airtable::Record.new("Name" => "Sarah Jaine")
      assert_includes record.fields_for_update.keys, "Name"
    end
  end
end
