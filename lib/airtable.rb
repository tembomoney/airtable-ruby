require 'net/http'
require 'json'
require 'uri'
require 'openssl'
require 'delegate'
require 'active_support/core_ext/hash'

module Airtable
  # Escape a value for safe interpolation into Airtable formula strings.
  # Airtable formulas use single-quoted strings. This method escapes
  # backslashes and single quotes, then wraps in single quotes.
  #
  # Usage:
  #   formula = "{Email} = #{Airtable.escape_formula_value(user.email)}"
  #   table.select(formula: formula)
  def self.escape_formula_value(value)
    escaped = value.to_s.gsub('\\', '\\\\\\\\').gsub("'", "\\\\'")
    "'#{escaped}'"
  end
end

require 'airtable/version'
require 'airtable/resource'
require 'airtable/record'
require 'airtable/record_set'
require 'airtable/table'
require 'airtable/client'
require 'airtable/error'
require 'airtable/rate_limiter'
require 'airtable/batch_result'
