# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require 'avro'
require 'logstash/codecs/avro'
require 'logstash/event'

describe LogStash::Codecs::Avro do
  let (:avro_config) {{'schema_uri' => '
                        {"type": "record", "name": "Test",
                        "fields": [{"name": "foo", "type": ["null", "string"]},
                                   {"name": "bar", "type": "int"}]}'}}
  let (:test_event) { LogStash::Event.new({"foo" => "hello", "bar" => 10}) }

  let(:config) {
    {
        "schema_uri" => avro_config['schema_uri'],
        "strip_headers" => false,
    }
  }

  subject do
    allow_any_instance_of(LogStash::Codecs::Avro).to \
      receive(:open_and_read).and_return(avro_config['schema_uri'])
    next LogStash::Codecs::Avro.new(config)
  end

  describe "#decode" do
    subject do
      allow_any_instance_of(LogStash::Codecs::Avro).to \
      receive(:open_and_read).and_return(avro_config['schema_uri'])
      next LogStash::Codecs::Avro.new(config)
    end

    context "with strip headers false" do
      it "should return an LogStash::Event from avro data" do
        schema = Avro::Schema.parse(avro_config['schema_uri'])
        dw = Avro::IO::DatumWriter.new(schema)
        buffer = StringIO.new
        encoder = Avro::IO::BinaryEncoder.new(buffer)
        dw.write(test_event.to_hash, encoder)
        got_event = false

        subject.decode(buffer.string) do |event|
          insist { event.is_a? LogStash::Event }
          insist { event.get("foo") } == test_event.get("foo")
          insist { event.get("bar") } == test_event.get("bar")
          got_event = true
        end

        insist { got_event }
      end
    end

    context "with strip headers true" do
      let(:settings) {
        {
            "schema_uri" => avro_config['schema_uri'],
            "strip_headers" => true,
        }
      }
      subject do
        allow_any_instance_of(LogStash::Codecs::Avro).to \
        receive(:open_and_read).and_return(avro_config['schema_uri'])
        next LogStash::Codecs::Avro.new(settings)
      end

      it "should return an LogStash::Event from avro data and strip headers" do
        schema = Avro::Schema.parse(avro_config['schema_uri'])
        dw = Avro::IO::DatumWriter.new(schema)
        buffer = StringIO.new

        headers = [MAGIC_BYTE,1234]
        header = headers.pack("cI>")
        buffer.write(header)

        encoder = Avro::IO::BinaryEncoder.new(buffer)
        dw.write(test_event.to_hash, encoder)
        got_event = false

        subject.decode(buffer.string) do |event|
          insist { event.is_a? LogStash::Event }
          insist { event.get("foo") } == test_event.get("foo")
          insist { event.get("bar") } == test_event.get("bar")
          got_event = true
        end

        insist { got_event }
      end
    end
  end

  context "#encode" do
    it "should return avro data from a LogStash::Event" do
      got_event = false
      subject.on_event do |event, data|
        schema = Avro::Schema.parse(avro_config['schema_uri'])
        datum = StringIO.new(data)
        decoder = Avro::IO::BinaryDecoder.new(datum)
        datum_reader = Avro::IO::DatumReader.new(schema)
        record = datum_reader.read(decoder)

        insist { record["foo"] } == test_event.get("foo")
        insist { record["bar"] } == test_event.get("bar")
        insist { event.is_a? LogStash::Event }
        got_event = true
      end
      subject.encode(test_event)
      insist { got_event }
    end
  end
end
