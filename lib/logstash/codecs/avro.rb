# encoding: utf-8
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"

class LogStash::Codecs::Avro < LogStash::Codecs::Base
  config_name "avro"

  milestone 1

  config :schema_file, :validate => :string, :required => true

  public
  def register
    require "avro"
    @schema = Avro::Schema.parse(File.read(schema_file))
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
    # TODO(talevy): figure out metadata field handling
    datum = event.to_hash
    dw.write(datum, encoder)

    @on_event.call(buffer.string)
  end
end
