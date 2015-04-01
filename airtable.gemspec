# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'airtable/version'

Gem::Specification.new do |spec|
  spec.name          = "airtable"
  spec.version       = Airtable::VERSION
  spec.authors       = ["Nathan Esquenazi"]
  spec.email         = ["nesquena@gmail.com"]
  spec.summary       = %q{Easily connect to airtable data using ruby}
  spec.description   = %q{Easily connect to airtable data using ruby with access to all of the airtable features.}
  spec.homepage      = "https://github.com/nesquena/airtable-ruby"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "httparty"
  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
end
