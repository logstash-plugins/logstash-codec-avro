# encoding: utf-8
require "open-uri"
require "avro"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"

class LogStash::Codecs::Avro < LogStash::Codecs::Base
  config_name "avro"


  # schema path to fetch the schema from
  # This can be a 'http' or 'file' scheme URI
  # example:
  #     http - "http://example.com/schema.avsc"
  #     file - "/path/to/schema.avsc"
  config :schema_uri, :validate => :string, :required => true

  def open_and_read(uri_string)
    open(uri_string).read
  end

  public
  def register
    @schema = Avro::Schema.parse(open_and_read(schema_uri))
  end

  public
  def decode(data)
    datum = StringIO.new(data)
    decoder = Avro::IO::BinaryDecoder.new(datum)
    datum_reader = Avro::IO::DatumReader.new(@schema)
    yield LogStash::Event.new(datum_reader.read(decoder))
  end

  public
  def encode(event)
    dw = Avro::IO::DatumWriter.new(@schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(event.to_hash, encoder)
    @on_event.call(event, buffer.string)
  end
end
