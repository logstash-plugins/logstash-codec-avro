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
  let(:config) { {} }
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

      let(:config) { super().merge({ 'schema_uri' => full_schema_url }) }

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

  context "Schema Registry with authentication" do
    let(:schema_registry_url) { "http://localhost:8081" }
    let(:username) { "barney" }
    let(:password) { "changeme" }

    before(:all) do
      run_integration_script("stop_schema_registry.sh")
      sleep 2
      run_integration_script("start_auth_schema_registry.sh")
      sleep 5
      wait_for_schema_registry("http://localhost:8081", username: "barney", password: "changeme")
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
      sleep 2
    end

    context "with valid credentials" do
      let(:schema_subject) { "test-auth-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_url}/subjects/#{schema_subject}/versions/latest" }
      let(:config) { super().merge({ 'schema_uri' => full_schema_url, 'username' => username, 'password' => password }) }

      before do
        client_options = { auth: { user: username, password: password } }
        client = Manticore::Client.new(client_options)
        response = client.post("#{schema_registry_url}/subjects/#{schema_subject}/versions",
                               headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                               body: { schema: test_schema_json }.to_json
        ).call
        expect(response.code).to eq(200)
        client.close
      end

      it "fetches schema with valid credentials" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = []
        codec.decode(encoded_data) do |event|
          events << event
        end

        expect(events.size).to eq(1)
        expect(events.first.get("message")).to eq(test_event_data["message"])
      end

      it "encodes data with authentication" do
        event = LogStash::Event.new(test_event_data)
        encoded_data = nil

        codec.on_event do |e, data|
          encoded_data = data
        end

        codec.encode(event)
        expect(encoded_data).not_to be_nil
      end
    end

    context "with invalid credentials" do
      let(:schema_subject) { "test-invalid-auth-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_url}/subjects/#{schema_subject}/versions/latest" }

      before do
        client_options = { auth: { user: username, password: password } }
        client = Manticore::Client.new(client_options)
        response = client.post("#{schema_registry_url}/subjects/#{schema_subject}/versions",
                               headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                               body: { schema: test_schema_json }.to_json
        ).call
        expect(response.code).to eq(200)
        client.close
      end

      it "fails with invalid credentials" do
        expect {
          invalid_config = { 'schema_uri' => full_schema_url, 'username' => 'invalid', 'password' => 'wrong' }
          LogStash::Codecs::Avro.new(invalid_config).tap { |c| c.register }
        }.to raise_error(/401|403|HTTP request failed/)
      end
    end
  end

  context "Schema Registry with SSL/TLS" do
    let(:schema_registry_https_url) { "https://localhost:8083" }
    let(:truststore_path) { File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks") }
    let(:truststore_password) { "changeit" }
    let(:ca_cert_path) { File.join(INTEGRATION_DIR, "tls_repository", "schema_reg_certificate.pem") }

    before(:all) do
      # Ensure non-auth registry is running (it includes HTTPS on 8083)
      run_integration_script("stop_schema_registry.sh")
      sleep 2
      run_integration_script("start_schema_registry.sh")
      sleep 5

      ssl_options = {
        truststore: File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks"),
        truststore_password: "changeit",
        truststore_type: "jks",
        verify: :default
      }
      wait_for_schema_registry("https://localhost:8083", ssl_options: ssl_options)
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
      sleep 2
    end

    context "with truststore configuration" do
      let(:schema_subject) { "test-ssl-truststore-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_https_url}/subjects/#{schema_subject}/versions/latest" }
      let(:config) do
        super().merge({
                        'schema_uri' => full_schema_url,
                        'ssl_enabled' => true,
                        'ssl_truststore_path' => truststore_path,
                        'ssl_truststore_password' => truststore_password,
                        'ssl_truststore_type' => 'JKS',
                        'ssl_verification_mode' => 'full'
                      })
      end

      before do
        ssl_options = {
          truststore: truststore_path,
          truststore_password: truststore_password,
          truststore_type: "jks",
          verify: :default
        }
        client = Manticore::Client.new(ssl: ssl_options)
        response = client.post("#{schema_registry_https_url}/subjects/#{schema_subject}/versions",
                               headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                               body: { schema: test_schema_json }.to_json
        ).call
        expect(response.code).to eq(200)
        client.close
      end

      it "fetches schema using truststore" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = []
        codec.decode(encoded_data) do |event|
          events << event
        end

        expect(events.size).to eq(1)
        expect(events.first.get("message")).to eq(test_event_data["message"])
      end
    end

    context "with CA certificate configuration" do
      let(:schema_subject) { "test-ssl-ca-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_https_url}/subjects/#{schema_subject}/versions/latest" }
      let(:config) do
        super().merge({
                        'schema_uri' => full_schema_url,
                        'ssl_enabled' => true,
                        'ssl_certificate_authorities' => [ca_cert_path],
                        'ssl_verification_mode' => 'full'
                      })
      end

      before do
        ssl_options = { ca_file: ca_cert_path, verify: :default }
        client = Manticore::Client.new(ssl: ssl_options)
        response = client.post("#{schema_registry_https_url}/subjects/#{schema_subject}/versions",
                               headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                               body: { schema: test_schema_json }.to_json
        ).call
        expect(response.code).to eq(200)
        client.close
      end

      it "fetches schema using CA certificate" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = []
        codec.decode(encoded_data) do |event|
          events << event
        end

        expect(events.size).to eq(1)
        expect(events.first.get("message")).to eq(test_event_data["message"])
      end
    end
  end

  context "Schema Registry with authentication and SSL" do
    let(:schema_registry_https_url) { "https://localhost:8083" }
    let(:username) { "barney" }
    let(:password) { "changeme" }
    let(:truststore_path) { File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks") }
    let(:truststore_password) { "changeit" }

    before(:all) do
      # Start authenticated registry (includes HTTPS)
      run_integration_script("stop_schema_registry.sh")
      sleep 3
      run_integration_script("start_auth_schema_registry.sh")
      sleep 10

      ssl_options = {
        truststore: File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks"),
        truststore_password: "changeit",
        truststore_type: "jks",
        verify: :default
      }
      wait_for_schema_registry("https://localhost:8083", username: "barney", password: "changeme", ssl_options: ssl_options)
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
      sleep 2
    end

    context "with valid credentials and truststore" do
      let(:schema_subject) { "test-auth-ssl-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_https_url}/subjects/#{schema_subject}/versions/latest" }
      let(:config) do
        super().merge({
                        'schema_uri' => full_schema_url,
                        'username' => username,
                        'password' => password,
                        'ssl_enabled' => true,
                        'ssl_truststore_path' => truststore_path,
                        'ssl_truststore_password' => truststore_password,
                        'ssl_truststore_type' => 'JKS',
                        'ssl_verification_mode' => 'full'
                      })
      end

      before do
        ssl_options = {
          truststore: truststore_path,
          truststore_password: truststore_password,
          truststore_type: "jks",
          verify: :default
        }
        client_options = {
          auth: { user: username, password: password },
          ssl: ssl_options
        }
        client = Manticore::Client.new(client_options)
        response = client.post("#{schema_registry_https_url}/subjects/#{schema_subject}/versions",
                               headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                               body: { schema: test_schema_json }.to_json
        ).call
        expect(response.code).to eq(200)
        client.close
      end

      it "fetches schema with both authentication and SSL" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = []
        codec.decode(encoded_data) do |event|
          events << event
        end

        expect(events.size).to eq(1)
        expect(events.first.get("message")).to eq(test_event_data["message"])
      end

      it "encodes data with authentication and SSL" do
        event = LogStash::Event.new(test_event_data)
        encoded_data = nil

        codec.on_event do |e, data|
          encoded_data = data
        end

        codec.encode(event)
        expect(encoded_data).not_to be_nil
      end
    end
  end
end
