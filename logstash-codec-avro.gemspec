Gem::Specification.new do |s|

  s.name            = 'logstash-codec-avro'
  s.version         = '2.0.1'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Encode and decode avro formatted data"
  s.description     = "Encode and decode avro formatted data"
  s.authors         = ["Elastic"]
  s.email           = 'info@elastic.co'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths   = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "codec" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", ">= 2.0.0.snapshot", "< 3.0.0"

  s.add_runtime_dependency "avro"  #(Apache 2.0 license)

  s.add_development_dependency 'logstash-devutils'
end

