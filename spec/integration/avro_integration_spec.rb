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

  def encode_avro_data(schema_json, data)
    schema = Avro::Schema.parse(schema_json)
    dw = Avro::IO::DatumWriter.new(schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(data, encoder)
    Base64.strict_encode64(buffer.string)
  end

  def decode_with_codec(codec, encoded_data)
    events = []
    codec.decode(encoded_data) do |event|
      events << event
    end
    events
  end

  def expect_decoded_event_matches(events, expected_data)
    expect(events.size).to eq(1)
    expect(events.first.get("message")).to eq(expected_data["message"])
    expect(events.first.get("timestamp")).to eq(expected_data["timestamp"])
  end

  def create_manticore_client(username: nil, password: nil, ssl_options: {})
    client_options = {}
    if username && password
      client_options[:auth] = { user: username, password: password }
    end
    client_options[:ssl] = ssl_options unless ssl_options.empty?

    Manticore::Client.new(client_options)
  end

  def wait_for_schema_registry(url, username: nil, password: nil, ssl_options: {})
    puts "Waiting for Schema Registry at #{url}..."
    client = create_manticore_client(username: username, password: password, ssl_options: ssl_options)

    Stud.try(20.times, [Manticore::SocketException, StandardError, RSpec::Expectations::ExpectationNotMetError]) do
      response = client.get(url).call
      expect(response.code).to eq(200)
    end
    client.close
  end

  def register_schema(base_url, subject, schema_json, username: nil, password: nil, ssl_options: {})
    client = create_manticore_client(username: username, password: password, ssl_options: ssl_options)
    response = client.post("#{base_url}/subjects/#{subject}/versions",
                           headers: { "Content-Type" => "application/vnd.schemaregistry.v1+json" },
                           body: { schema: schema_json }.to_json
    ).call
    expect(response.code).to eq(200)
    client.close
  end

  context "Schema Registry without authentication" do
    let(:schema_registry_url) { "http://localhost:8081" }

    before(:all) do
      run_integration_script("start_schema_registry.sh")
      wait_for_schema_registry("http://localhost:8081")
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
    end

    context "fetching schema via HTTP" do
      let(:schema_subject) { "test-no-auth-#{Time.now.to_i}" }
      let(:full_schema_url) do
        url = "#{schema_registry_url}/subjects/#{schema_subject}/versions/latest"
        puts "Constructed schema URL: #{url}"
        url
      end

      let(:config) { super().merge({ 'schema_uri' => full_schema_url }) }

      before do
        register_schema(schema_registry_url, schema_subject, test_schema_json)
      end

      it "fetches and decodes schema from Schema Registry" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
      end

      it "encodes data using schema from schema registry" do
        event = LogStash::Event.new(test_event_data)
        encoded_data = nil

        codec.on_event do |e, data|
          encoded_data = data
        end

        codec.encode(event)

        expect(encoded_data).not_to be_nil

        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
      end
    end
  end

  context "Schema Registry with authentication" do
    let(:schema_registry_url) { "http://localhost:8081" }
    let(:username) { "barney" }
    let(:password) { "changeme" }

    before(:all) do
      run_integration_script("stop_schema_registry.sh")
      run_integration_script("start_auth_schema_registry.sh")
      wait_for_schema_registry("http://localhost:8081", username: "barney", password: "changeme")
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
    end

    context "with valid credentials" do
      let(:schema_subject) { "test-auth-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_url}/subjects/#{schema_subject}/versions/latest" }
      let(:config) { super().merge({ 'schema_uri' => full_schema_url, 'username' => username, 'password' => password }) }

      before do
        register_schema(schema_registry_url, schema_subject, test_schema_json,
                       username: username, password: password)
      end

      it "fetches schema with valid credentials" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
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
        register_schema(schema_registry_url, schema_subject, test_schema_json,
                       username: username, password: password)
      end

      it "fails with invalid credentials" do
        expect {
          invalid_config = { 'schema_uri' => full_schema_url, 'username' => 'invalid', 'password' => 'wrong' }
          LogStash::Codecs::Avro.new(invalid_config).tap { |c| c.register }
        }.to raise_error(/401 Unauthorized/)
      end
    end
  end

  context "Schema Registry with truststore configuration" do
    let(:schema_registry_https_url) { "https://localhost:8083" }
    let(:truststore_path) { File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks") }
    let(:truststore_password) { "changeit" }
    let(:ca_cert_path) { File.join(INTEGRATION_DIR, "tls_repository", "schema_reg_certificate.pem") }

    before(:all) do
      # Ensure non-auth registry is running (it includes HTTPS on 8083)
      run_integration_script("stop_schema_registry.sh")
      run_integration_script("start_schema_registry.sh")

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
        register_schema(schema_registry_https_url, schema_subject, test_schema_json,
                       ssl_options: ssl_options)
      end

      it "fetches schema using truststore" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
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
        register_schema(schema_registry_https_url, schema_subject, test_schema_json,
                       ssl_options: ssl_options)
      end

      it "fetches schema using CA certificate" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
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
      run_integration_script("start_auth_schema_registry.sh")

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
        register_schema(schema_registry_https_url, schema_subject, test_schema_json,
                       username: username, password: password,
                       ssl_options: ssl_options)
      end

      it "fetches schema with both authentication and SSL" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
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

  context "Schema Registry with keystore configuration (mutual TLS)" do
    let(:schema_registry_https_url) { "https://localhost:8083" }

    let(:truststore_path) { File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks") }
    let(:truststore_password) { "changeit" }
    let(:keystore_path) { File.join(INTEGRATION_DIR, "tls_repository", "schema_reg.jks") }
    let(:keystore_password) { "changeit" }

    before(:all) do
      run_integration_script("stop_schema_registry.sh")
      run_integration_script("start_schema_registry_mutual.sh")

      ssl_options = {
        keystore: File.join(INTEGRATION_DIR, "tls_repository", "schema_reg.jks"),
        keystore_password: "changeit",
        keystore_type: "jks",
        truststore: File.join(INTEGRATION_DIR, "tls_repository", "clienttruststore.jks"),
        truststore_password: "changeit",
        truststore_type: "jks"
      }
      wait_for_schema_registry("https://localhost:8083", ssl_options: ssl_options)
    end

    after(:all) do
      run_integration_script("stop_schema_registry.sh")
    end

    context "with keystore and truststore" do
      let(:schema_subject) { "test-mutual-tls-#{Time.now.to_i}" }
      let(:full_schema_url) { "#{schema_registry_https_url}/subjects/#{schema_subject}/versions/latest" }

      let(:config) do
        super().merge({
                        'schema_uri' => full_schema_url,
                        'ssl_enabled' => true,
                        'ssl_keystore_path' => keystore_path,
                        'ssl_keystore_password' => keystore_password,
                        'ssl_keystore_type' => 'JKS',
                        'ssl_truststore_path' => truststore_path,
                        'ssl_truststore_password' => truststore_password,
                        'ssl_truststore_type' => 'JKS',
                        'ssl_verification_mode' => 'full'
                      })
      end

      before do
        ssl_options = {
          keystore: keystore_path,
          keystore_password: keystore_password,
          keystore_type: "jks",
          truststore: truststore_path,
          truststore_password: truststore_password,
          truststore_type: "jks",
          verify: :default
        }
        register_schema(schema_registry_https_url, schema_subject, test_schema_json,
                       ssl_options: ssl_options)
      end

      it "fetches schema" do
        encoded_data = encode_avro_data(test_schema_json, test_event_data)
        events = decode_with_codec(codec, encoded_data)
        expect_decoded_event_matches(events, test_event_data)
      end
    end
  end
end
