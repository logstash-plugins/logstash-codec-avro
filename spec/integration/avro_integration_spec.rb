# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'logstash/codecs/avro'
require 'logstash/event'
require 'avro'
require 'base64'
require 'manticore'

describe "Avro Codec Integration Tests", :integration => true do
  INTEGRATION_DIR = File.expand_path('../', __FILE__)

  let(:test_schema) do
    {
      "type" => "record",
      "name" => "TestRecord",
      "namespace" => "com.example",
      "fields" => [
        { "name" => "message", "type" => "string" },
        { "name" => "timestamp", "type" => "long" }
      ]
    }
  end

  let(:test_schema_json) { test_schema.to_json }
  let(:test_event_data) do
    {
      "message" => "test message",
      "timestamp" => Time.now.to_i
    }
  end

  let(:config) {{ }}
  let(:codec) { LogStash::Codecs::Avro.new(config).tap { |c| c.register } }

  def run_integration_script(script_name)
    Dir.chdir(INTEGRATION_DIR) do
      result = system("./#{script_name}")
      puts "Script #{script_name} #{result ? 'succeeded' : 'failed'}"
      result
    end
  end

  def register_schema(schema_registry_url, schema_json, username: nil, password: nil, ssl_options: {})
    client_options = {}

    if username && password
      client_options[:auth] = { user: username, password: password }
    end

    client_options[:ssl] = ssl_options unless ssl_options.empty?

    client = Manticore::Client.new(client_options)

    response = client.post("#{schema_registry_url}/subjects/test-value/versions",
                           headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                           body: { schema: schema_json }.to_json
    ).call

    raise "Failed to register schema: #{response.code} #{response.body}" unless response.code == 200

    JSON.parse(response.body)["id"]
  ensure
    client&.close
  end

  def encode_avro_data(schema_json, data)
    schema = Avro::Schema.parse(schema_json)
    dw = Avro::IO::DatumWriter.new(schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(data, encoder)
    Base64.strict_encode64(buffer.string)
  end

  def create_test_schema_file(filename = "test_schema.avsc")
    schema_path = File.join(Dir.tmpdir, filename)
    File.write(schema_path, test_schema_json)
    schema_path
  end

  def wait_for_schema_registry(url, timeout: 60, username: nil, password: nil, ssl_options: {})
    start_time = Time.now
    client_options = {}

    if username && password
      client_options[:auth] = { user: username, password: password }
    end

    client_options[:ssl] = ssl_options unless ssl_options.empty?

    puts "Waiting for Schema Registry at #{url}..."
    attempt = 0

    loop do
      attempt += 1
      begin
        client = Manticore::Client.new(client_options)
        response = client.get(url).call
        if response.code == 200
          puts "✓ Schema Registry is ready after #{attempt} attempts"
          return true
        end
      rescue => e
        # Continue waiting
        puts "  Attempt #{attempt}: #{e.class.name} - #{e.message[0..80]}" if attempt % 5 == 0
      ensure
        client&.close if client
      end

      if Time.now - start_time > timeout
        raise "Schema Registry at #{url} did not become available within #{timeout} seconds after #{attempt} attempts"
      end

      sleep 2
    end
  end

  context "Schema Registry without authentication" do
    let(:schema_registry_url) { "http://localhost:8081" }

    before(:all) do
      run_integration_script("start_schema_registry.sh")
      wait_for_schema_registry("http://localhost:8081")
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
      sleep 2
    end

    context "fetching schema via HTTP" do
      let(:schema_subject) { "test-no-auth-#{Time.now.to_i}" }
      let(:full_schema_url) do
        url = "#{schema_registry_url}/subjects/#{schema_subject}/versions/latest"
        puts "Constructed schema URL: #{url}"
        raise "Schema URL is empty!" if url.nil? || url.empty? || !url.start_with?('http')
        url
      end

      let(:config) { super().merge({'schema_uri' => full_schema_url}) }

      before do
        client = Manticore::Client.new
        response = client.post("#{schema_registry_url}/subjects/#{schema_subject}/versions",
                               headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                               body: { schema: test_schema_json }.to_json
        ).call
        puts "Schema registration response: #{response.code}"
        expect(response.code).to eq(200)
        client.close
      end

      it "fetches and decodes schema from Schema Registry" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = []
        codec.decode(encoded_data) do |event|
          events << event
        end

        expect(events.size).to eq(1)
        expect(events.first.get("message")).to eq(test_event_data["message"])
        expect(events.first.get("timestamp")).to eq(test_event_data["timestamp"])
      end

      it "encodes data using schema from schema registry" do
        event = LogStash::Event.new(test_event_data)
        encoded_data = nil

        codec.on_event do |e, data|
          encoded_data = data
        end

        codec.encode(event)

        expect(encoded_data).not_to be_nil

        events = []
        codec.decode(encoded_data) do |decoded_event|
          events << decoded_event
        end

        expect(events.first.get("message")).to eq(test_event_data["message"])
      end
    end
  end
end
