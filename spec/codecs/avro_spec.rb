# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'insist'
require 'avro'
require 'base64'
require 'logstash/codecs/avro'
require 'logstash/event'

describe LogStash::Codecs::Avro do

  context "non binary data" do
    let (:avro_config) {{ 'schema_uri' => '
                        {"type": "record", "name": "Test",
                        "fields": [{"name": "foo", "type": ["null", "string"]},
                                   {"name": "bar", "type": "int"}]}' }}
    let (:test_event) {LogStash::Event.new({ "foo" => "hello", "bar" => 10 })}

    subject do
      allow_any_instance_of(LogStash::Codecs::Avro).to \
      receive(:open_and_read).and_return(avro_config['schema_uri'])
      next LogStash::Codecs::Avro.new(avro_config)
    end

    context "#decode" do
      it "should return an LogStash::Event from raw and base64 encoded avro data" do
        schema = Avro::Schema.parse(avro_config['schema_uri'])
        dw = Avro::IO::DatumWriter.new(schema)
        buffer = StringIO.new
        encoder = Avro::IO::BinaryEncoder.new(buffer)
        dw.write(test_event.to_hash, encoder)

        subject.decode(Base64.strict_encode64(buffer.string)) do |event|
          insist {event.is_a? LogStash::Event}
          insist {event.get("foo")} == test_event.get("foo")
          insist {event.get("bar")} == test_event.get("bar")
        end
        subject.decode(buffer.string) do |event|
          insist {event.is_a? LogStash::Event}
          insist {event.get("foo")} == test_event.get("foo")
          insist {event.get("bar")} == test_event.get("bar")
        end
      end

      it "should throw exception if decoding fails" do
        expect {subject.decode("not avro") {|_| }}.to raise_error NoMethodError
      end
    end

    context "#decode with tag_on_failure" do
      let (:avro_config) {super.merge("tag_on_failure" => true)}

      it "should tag event on failure" do
        subject.decode("not avro") do |event|
          insist {event.is_a? LogStash::Event}
          insist {event.get("tags")} == ["_avroparsefailure"]
        end
      end
    end

    context "#encode" do
      it "should return avro data from a LogStash::Event" do
        got_event = false
        subject.on_event do |event, data|
          schema = Avro::Schema.parse(avro_config['schema_uri'])
          datum = StringIO.new(Base64.strict_decode64(data))
          decoder = Avro::IO::BinaryDecoder.new(datum)
          datum_reader = Avro::IO::DatumReader.new(schema)
          record = datum_reader.read(decoder)

          insist {record["foo"]} == test_event.get("foo")
          insist {record["bar"]} == test_event.get("bar")
          insist {event.is_a? LogStash::Event}
          got_event = true
        end
        subject.encode(test_event)
        insist {got_event}
      end

      context "binary data" do

        let (:avro_config) {{ 'schema_uri' => '{"namespace": "com.systems.test.data",
                      "type": "record",
                      "name": "TestRecord",
                      "fields": [
                        {"name": "name", "type": ["string", "null"]},
                        {"name": "longitude", "type": ["double", "null"]},
                        {"name": "latitude", "type": ["double", "null"]}
                      ]
                    }' }}
        let (:test_event) {LogStash::Event.new({ "name" => "foo", "longitude" => 21.01234.to_f, "latitude" => 111.0123.to_f })}

        subject do
          allow_any_instance_of(LogStash::Codecs::Avro).to \
      receive(:open_and_read).and_return(avro_config['schema_uri'])
          next LogStash::Codecs::Avro.new(avro_config)
        end

        it "should correctly encode binary data" do
          schema = Avro::Schema.parse(avro_config['schema_uri'])
          dw = Avro::IO::DatumWriter.new(schema)
          buffer = StringIO.new
          encoder = Avro::IO::BinaryEncoder.new(buffer)
          dw.write(test_event.to_hash, encoder)

          subject.decode(Base64.strict_encode64(buffer.string)) do |event|
            insist {event.is_a? LogStash::Event}
            insist {event.get("name")} == test_event.get("name")
            insist {event.get("longitude")} == test_event.get("longitude")
            insist {event.get("latitude")} == test_event.get("latitude")
          end
        end
      end
    end
  end
end
