module Airtable
  # Accumulates results from batch API calls across multiple chunks.
  #
  # Each chunk of up to 10 records is sent as a separate request. Successes
  # and failures are collected so the caller can handle partial failures.
  class BatchResult
    attr_reader :successes, :failures, :created_record_ids

    def initialize
      @successes = []
      @failures = []
      @created_record_ids = []
    end

    def add_success(record)
      @successes << record
    end

    def add_failure(record_or_input, error)
      @failures << { record: record_or_input, error: error }
    end

    def add_created_ids(ids)
      @created_record_ids.concat(ids)
    end

    def all_succeeded?
      @failures.empty?
    end
  end
end
