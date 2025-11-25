# encoding: utf-8
require "open-uri"
require "manticore"
require "avro"
require "base64"
require "json"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"
require 'logstash/plugin_mixins/ecs_compatibility_support'
require 'logstash/plugin_mixins/ecs_compatibility_support/target_check'
require 'logstash/plugin_mixins/validator_support/field_reference_validation_adapter'
require 'logstash/plugin_mixins/event_support/event_factory_adapter'

# Read serialized Avro records as Logstash events
#
# This plugin is used to serialize Logstash events as
# Avro datums, as well as deserializing Avro datums into
# Logstash events.
#
# ==== Encoding
#
# This codec is for serializing individual Logstash events
# as Avro datums that are Avro binary blobs. It does not encode
# Logstash events into an Avro file.
#
#
# ==== Decoding
#
# This codec is for deserializing individual Avro records. It is not for reading
# Avro files. Avro files have a unique format that must be handled upon input.
#
#
# ==== Usage
# Example usage with Kafka input.
#
# [source,ruby]
# ----------------------------------
# input {
#   kafka {
#     codec => avro {
#         schema_uri => "/tmp/schema.avsc"
#     }
#   }
# }
# filter {
#   ...
# }
# output {
#   ...
# }
# ----------------------------------
class LogStash::Codecs::Avro < LogStash::Codecs::Base
  config_name "avro"

  include LogStash::PluginMixins::ECSCompatibilitySupport(:disabled, :v1, :v8 => :v1)
  include LogStash::PluginMixins::ECSCompatibilitySupport::TargetCheck

  extend LogStash::PluginMixins::ValidatorSupport::FieldReferenceValidationAdapter

  include LogStash::PluginMixins::EventSupport::EventFactoryAdapter

  BINARY_ENCODING = "binary".freeze
  BASE64_ENCODING = "base64".freeze

  # Set encoding for Avro's payload.
  #  Use `base64` (default) encoding to convert the raw binary bytes to a `base64` encoded string.
  #  Set this option to `binary` to use the plain binary bytes.
  config :encoding, :validate => [BINARY_ENCODING, BASE64_ENCODING], :default => BASE64_ENCODING

  # schema path to fetch the schema from.
  # This can be a 'http' or 'file' scheme URI
  # example:
  #
  # * http - `http://example.com/schema.avsc`
  # * file - `/path/to/schema.avsc`
  config :schema_uri, :validate => :string, :required => true

  # tag events with `_avroparsefailure` when decode fails
  config :tag_on_failure, :validate => :boolean, :default => false

  # Defines a target field for placing decoded fields.
  # If this setting is omitted, data gets stored at the root (top level) of the event.
  #
  # NOTE: the target is only relevant while decoding data into a new event.
  config :target, :validate => :field_reference

  # Proxy server URL for schema registry connections
  config :proxy, :validate => :uri

  # Username for HTTP basic authentication
  config :username, :validate => :string

  # Password for HTTP basic authentication
  config :password, :validate => :password

  # Enable SSL/TLS secured communication to remote schema registry
  config :ssl_enabled, :validate => :boolean

  # PEM-based SSL configuration (alternative to keystore/truststore)
  # Path to PEM encoded certificate file for client authentication
  config :ssl_certificate, :validate => :path

  # Path to PEM encoded private key file for client authentication
  config :ssl_key, :validate => :path

  # Path to PEM encoded CA certificate file(s) for server verification
  # Can be a single file or directory containing multiple CA certificates
  config :ssl_certificate_authorities, :validate => :path, :list => true

  # Options to verify the server's certificate.
  # "full": validates that the provided certificate has an issue date that’s within the not_before and not_after dates;
  # chains to a trusted Certificate Authority (CA); has a hostname or IP address that matches the names within the certificate.
  # "none": performs no certificate validation. Disabling this severely compromises security (https://www.cs.utexas.edu/~shmat/shmat_ccs12.pdf)
  config :ssl_verification_mode, :validate => %w[full none], :default => 'full'

  # The keystore path
  config :ssl_keystore_path, :validate => :path

  # The keystore password
  config :ssl_keystore_password, :validate => :password

  # Keystore type (JKS or PKCS12)
  config :ssl_keystore_type, :validate => %w[JKS PKCS12]

  # The truststore path
  config :ssl_truststore_path, :validate => :path

  # The truststore password
  config :ssl_truststore_password, :validate => :password

  # Truststore type (JKS or PKCS12)
  config :ssl_truststore_type, :validate => %w[JKS PKCS12]

  # The list of cipher suites to use, listed by priorities.
  # Supported cipher suites vary depending on which version of Java is used.
  config :ssl_cipher_suites, :validate => :string, :list => true

  # SSL supported protocols
  config :ssl_supported_protocols, :validate => %w[TLSv1.1 TLSv1.2 TLSv1.3], :default => [], :list => true

  public
  def initialize(*params)
    super
    @original_field = ecs_select[disabled: nil, v1: '[event][original]']
  end

  def register
    @schema = Avro::Schema.parse(fetch_schema(schema_uri))
  end

  public
  def decode(data)
    if encoding == BASE64_ENCODING
      datum = StringIO.new(Base64.strict_decode64(data)) rescue StringIO.new(data)
    else
      datum = StringIO.new(data)
    end
    decoder = Avro::IO::BinaryDecoder.new(datum)
    datum_reader = Avro::IO::DatumReader.new(@schema)
    event = targeted_event_factory.new_event(datum_reader.read(decoder))
    event.set(@original_field, data.dup.freeze) if @original_field
    yield event
  rescue => e
    if tag_on_failure
      @logger.error("Avro parse error, original data now in message field", :error => e)
      yield event_factory.new_event("message" => data, "tags" => ["_avroparsefailure"])
    else
      raise e
    end
  end

  public
  def encode(event)
    dw = Avro::IO::DatumWriter.new(@schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(event.to_hash, encoder)
    if encoding == BASE64_ENCODING
      @on_event.call(event, Base64.strict_encode64(buffer.string))
    else
      @on_event.call(event, buffer.string)
    end  
  end

  private
  def fetch_schema(uri_string)
    http_connection = uri_string.start_with?('http://')
    https_connection = uri_string.start_with?('https://')

    if http_connection
      ssl_config_provided = original_params.keys.select {|k| k.start_with?("ssl_") }
      if ssl_config_provided.any?
        raise_config_error! "When SSL is disabled, the following provided parameters are not allowed: #{ssl_config_provided}"
      end

      credentials_configured = @username && @password
      @logger.warn("Credentials are being sent over unencrypted HTTP. This may bring security risk.") if credentials_configured
      fetch_remote_schema(uri_string)
    elsif https_connection
      validate_ssl_settings!
      fetch_remote_schema(uri_string)
    else
      # local schema
      URI.open(uri_string, &:read)
    end
  end

  def fetch_remote_schema(uri_string)
    client = nil
    client_options = {}

    if @proxy && !@proxy.empty?
      client_options[:proxy] = @proxy.to_s
    end

    basic_auth_options = build_basic_auth
    client_options[:auth] = basic_auth_options unless basic_auth_options.empty?

    if @ssl_enabled
      ssl_options = build_ssl_options
      client_options[:ssl] = ssl_options unless ssl_options.empty?
    end

    client = Manticore::Client.new(client_options)
    response = client.get(uri_string).call

    unless response.code == 200
      raise "HTTP request failed: #{response.code} #{response.message}"
    end

    body = response.body
    # Response may contain schema metadata, schema field is what we need
    # Example response: {"subject":"test-no-auth-1763597024","version":1,"id":1,"guid":"5c6c5f26-e876-e5ab-02b0-8d9bebbc90d7","schemaType":"AVRO","schema":"{"type":"record","name":"TestRecord","namespace":"com.example","fields":[{"name":"message","type":"string"},{"name":"timestamp","type":"long"}]}","ts":1763597024561,"deleted":false}
    parsed = JSON.parse(body)
    if parsed.is_a?(Hash) && parsed.has_key?('schema')
      parsed['schema']
    else
      # fallback to use the response as it is
      body
    end
  rescue JSON::ParserError
    # Not JSON, return as-is (probably a direct schema)
    body
  ensure
    client.close if client
  end

  def build_basic_auth
    if !@username && !@password
      return {}
    end

    raise LogStash::ConfigurationError, "`username` requires `password`" if @username && !@password
    raise LogStash::ConfigurationError, "`password` is not allowed unless `username` is specified" if !@username && @password

    raise LogStash::ConfigurationError, "Empty `username` or `password` is not allowed" if @username.empty? || @password.value.empty?

    {:user => @username, :password => @password.value}
  end

  def validate_ssl_settings!
    @ssl_enabled = true if @ssl_enabled.nil?
    raise_config_error! "Secured #{@schema_uri} connection requires `ssl_enabled => true`. " unless @ssl_enabled
    @ssl_verification_mode = "full".freeze if @ssl_verification_mode.nil?

    # optional: presenting our identity
    raise_config_error! "`ssl_certificate` and `ssl_keystore_path` cannot be used together." if @ssl_certificate && @ssl_keystore_path
    raise_config_error! "`ssl_certificate` requires `ssl_key`" if @ssl_certificate && !@ssl_key
    ensure_readable_and_non_writable! "ssl_certificate", @ssl_certificate if @ssl_certificate

    raise_config_error! "`ssl_key` is not allowed unless `ssl_certificate` is specified" if @ssl_key && !@ssl_certificate
    ensure_readable_and_non_writable! "ssl_key", @ssl_key if @ssl_key

    raise_config_error! "`ssl_keystore_path` requires `ssl_keystore_password`" if @ssl_keystore_path && !@ssl_keystore_password
    raise_config_error! "`ssl_keystore_password` is not allowed unless `ssl_keystore_path` is specified" if @ssl_keystore_password && !@ssl_keystore_path
    raise_config_error! "`ssl_keystore_password` cannot be empty" if @ssl_keystore_password && @ssl_keystore_password.value.empty?
    raise_config_error! "`ssl_keystore_type` is not allowed unless `ssl_keystore_path` is specified" if @ssl_keystore_type && !@ssl_keystore_path

    ensure_readable_and_non_writable! "ssl_keystore_path", @ssl_keystore_path if @ssl_keystore_path

    # establishing trust of the server we connect to
    # system-provided trust requires verification mode enabled
    if @ssl_verification_mode == "none"
      raise_config_error! "`ssl_truststore_path` requires `ssl_verification_mode` to be `full`" if @ssl_truststore_path
      raise_config_error! "`ssl_truststore_password` requires `ssl_truststore_path` and `ssl_verification_mode => 'full'`" if @ssl_truststore_password
      raise_config_error! "`ssl_certificate_authorities` requires `ssl_verification_mode` to be `full`" if @ssl_certificate_authorities
    end

    raise_config_error! "`ssl_truststore_path` and `ssl_certificate_authorities` cannot be used together." if @ssl_truststore_path && @ssl_certificate_authorities
    raise_config_error! "`ssl_truststore_path` requires `ssl_truststore_password`" if @ssl_truststore_path && !@ssl_truststore_password
    ensure_readable_and_non_writable! "ssl_truststore_path", @ssl_truststore_path if @ssl_truststore_path

    raise_config_error! "`ssl_truststore_password` is not allowed unless `ssl_truststore_path` is specified" if !@ssl_truststore_path && @ssl_truststore_password
    raise_config_error! "`ssl_truststore_password` cannot be empty" if @ssl_truststore_password && @ssl_truststore_password.value.empty?

    if !@ssl_truststore_path && @ssl_certificate_authorities&.empty?
      raise_config_error! "`ssl_certificate_authorities` cannot be empty"
    end

    if @ssl_certificate_authorities && !@ssl_certificate_authorities.empty?
      raise_config_error! "Multiple values on `ssl_certificate_authorities` are not supported by this plugin" if @ssl_certificate_authorities.size > 1
      ensure_readable_and_non_writable! "ssl_certificate_authorities", @ssl_certificate_authorities.first
    end
  end

  def build_ssl_options
    ssl_options = {}

    ssl_options[:client_cert] = @ssl_certificate if @ssl_certificate
    ssl_options[:client_key] = @ssl_key if @ssl_key

    ssl_options[:ca_file] = @ssl_certificate_authorities&.first if @ssl_certificate_authorities

    ssl_options[:cipher_suites] = @ssl_cipher_suites if @ssl_cipher_suites

    ssl_options[:verify] = :default if @ssl_verification_mode == 'full'
    ssl_options[:verify] = :disable if @ssl_verification_mode == 'none'

    ssl_options[:keystore] = @ssl_keystore_path if @ssl_keystore_path
    ssl_options[:keystore_password] = @ssl_keystore_password&.value if @ssl_keystore_path && @ssl_keystore_password
    ssl_options[:keystore_type] = @ssl_keystore_type.downcase if @ssl_keystore_path && @ssl_keystore_type

    ssl_options[:truststore] = @ssl_truststore_path if @ssl_truststore_path
    ssl_options[:truststore_password] = @ssl_truststore_password&.value if @ssl_truststore_path && @ssl_truststore_password
    ssl_options[:truststore_type] = @ssl_truststore_type.downcase if @ssl_truststore_path && @ssl_truststore_type

    ssl_options[:protocols] = @ssl_supported_protocols if @ssl_supported_protocols && @ssl_supported_protocols&.any?

    ssl_options
  end

  ##
  # @param message [String]
  # @raise [LogStash::ConfigurationError]
  def raise_config_error!(message)
    raise LogStash::ConfigurationError, message
  end

  def ensure_readable_and_non_writable!(name, path)
    raise_config_error! "Specified #{name} #{path} path must be readable." unless File.readable?(path)
    raise_config_error! "Specified #{name} #{path} path must not be writable." if File.writable?(path)
  end
end
