# encoding: utf-8
require "open-uri"
require "avro"
require "base64"
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

  BINARY_ENCODING = "binary"
  BASE64_ENCODING = "base64"

  # Select which encoding to use to represent Avro's payload.
  # By default, it uses `base64`, however could be customized with `binary` to use the plain binary bytes.
  # With `base64` encoding the raw binary bytes are converted to a `base64` encoded string.
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

  def open_and_read(uri_string)
    open(uri_string).read
  end

  public
  def initialize(*params)
    super
    @original_field = ecs_select[disabled: nil, v1: '[event][original]']
  end

  def register
    @schema = Avro::Schema.parse(open_and_read(schema_uri))
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
end
