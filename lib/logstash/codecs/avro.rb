# encoding: utf-8
require "open-uri"
require "avro"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"

class LogStash::Codecs::Avro < LogStash::Codecs::Base
  config_name "avro"

  milestone 1

  config :writer_schema_uri, :validate => :string, :required => true
  config :reader_schema_uri, :validate => :string

  def open_and_read(uri_string)
    open(uri_string).read
  end

  public
  def register
    @writer_schema = Avro::Schema.parse(open_and_read(writer_schema_uri))
    @reader_schema = reader_schema_uri.nil?() ? @writer_schema : Avro::Schema.parse(open_and_read(reader_schema_uri))
  end

  public
  def decode(data)
    datum = StringIO.new(data)
    decoder = Avro::IO::BinaryDecoder.new(datum)
    datum_reader = Avro::IO::DatumReader.new(@writer_schema, @reader_schema)
    yield LogStash::Event.new(datum_reader.read(decoder))
  end

  public
  def encode(event)
    dw = Avro::IO::DatumWriter.new(@writer_schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(event.to_hash, encoder)
    @on_event.call(buffer.string)
  end
end
