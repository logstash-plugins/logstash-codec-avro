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

  subject do
    allow_any_instance_of(LogStash::Codecs::Avro).to \
      receive(:open_and_read).and_return(avro_config['schema_uri'])
    next LogStash::Codecs::Avro.new(avro_config)
  end

  context "#decode" do
    it "should return an LogStash::Event from avro data" do
      schema = Avro::Schema.parse(avro_config['schema_uri'])
      dw = Avro::IO::DatumWriter.new(schema)
      buffer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(buffer)
      dw.write(test_event.to_hash, encoder)
      payload = buffer.string.to_java_bytes

      subject.decode(payload) do |event|
        insist { event.is_a? LogStash::Event }
        insist { event.get("foo") } == test_event.get("foo")
        insist { event.get("bar") } == test_event.get("bar")
      end
    end

    it "should throw exception if decoding fails" do
      expect { subject.decode("not avro") { |_| } }.to raise_error NoMethodError
    end
  end

  context "#decode with tag_on_failure" do
    let (:avro_config) { super.merge("tag_on_failure" => true) }

    it "should tag event on failure" do
      subject.decode("not avro") do |event|
        insist { event.is_a? LogStash::Event }
        insist { event.get("tags") } == ["_avroparsefailure"]
      end
    end
  end

  context "#encode" do
    it "should return avro data from a LogStash::Event" do
      got_event = false
      subject.on_event do |event, data|
        schema = Avro::Schema.parse(avro_config['schema_uri'])
        datum = StringIO.new(String.from_java_bytes(data))
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
