# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'insist'
require 'avro'
require 'base64'
require 'logstash/codecs/avro'
require 'logstash/event'
require 'logstash/plugin_mixins/ecs_compatibility_support/spec_helper'

describe LogStash::Codecs::Avro, :ecs_compatibility_support, :aggregate_failures do

  ecs_compatibility_matrix(:disabled, :v1, :v8 => :v1) do |ecs_select|
    before(:each) do
      allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
    end

    context "non binary data" do
      let (:avro_config) {{ 'schema_uri' => '
                        {"type": "record", "name": "Test",
                        "fields": [{"name": "foo", "type": ["null", "string"]},
                                   {"name": "bar", "type": "int"}]}' }}
      let (:test_event_hash) { { "foo" => "hello", "bar" => 10 } }
      let (:test_event) {LogStash::Event.new(test_event_hash)}

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
            expect(event.get('[event][original]')).to eq(Base64.strict_encode64(buffer.string)) if ecs_compatibility != :disabled
          end
          subject.decode(buffer.string) do |event|
            insist {event.is_a? LogStash::Event}
            insist {event.get("foo")} == test_event.get("foo")
            insist {event.get("bar")} == test_event.get("bar")
            expect(event.get('[event][original]')).to eq(buffer.string) if ecs_compatibility != :disabled
          end
        end

        it "should throw exception if decoding fails" do
          expect {subject.decode("not avro") {|_| }}.to raise_error NoMethodError
        end
      end

      context "base64_encoding is false" do
        let (:avro_config) { super().merge('base64_encoding' => false) }

        it "should return an LogStash::Event from raw and base64 encoded avro data" do
          schema = Avro::Schema.parse(avro_config['schema_uri'])
          dw = Avro::IO::DatumWriter.new(schema)
          buffer = StringIO.new
          encoder = Avro::IO::BinaryEncoder.new(buffer)
          dw.write(test_event.to_hash, encoder)

          subject.decode(buffer.string) do |event|
            expect(event).to be_a_kind_of(LogStash::Event)
            expect(event.get("foo")).to eq(test_event.get("foo"))
            expect(event.get("bar")).to eq(test_event.get("bar"))
            expect(event.get('[event][original]')).to eq(buffer.string) if ecs_compatibility != :disabled
          end
        end

        it "should raise an error if base64 encoded data is provided" do
          schema = Avro::Schema.parse(avro_config['schema_uri'])
          dw = Avro::IO::DatumWriter.new(schema)
          buffer = StringIO.new
          encoder = Avro::IO::BinaryEncoder.new(buffer)
          dw.write(test_event.to_hash, encoder)

          expect {subject.decode(Base64.strict_encode64(buffer.string))}.to raise_error
        end
      end

      context "#decode with tag_on_failure" do
        let (:avro_config) {{ 'schema_uri' => '
                        {"type": "record", "name": "Test",
                        "fields": [{"name": "foo", "type": ["null", "string"]},
                                   {"name": "bar", "type": "int"}]}',
                              'tag_on_failure' => true}}

        it "should tag event on failure" do
          subject.decode("not avro") do |event|
            insist {event.is_a? LogStash::Event}
            insist {event.get("tags")} == ["_avroparsefailure"]
          end
        end
      end

      context "#decode with target" do
        let(:avro_target) { "avro_target" }
        let (:avro_config) {{ 'schema_uri' => '
                        {"type": "record", "name": "Test",
                        "fields": [{"name": "foo", "type": ["null", "string"]},
                                   {"name": "bar", "type": "int"}]}',
                              'target' => avro_target}}

        it "should return an LogStash::Event with content in target" do
          schema = Avro::Schema.parse(avro_config['schema_uri'])
          dw = Avro::IO::DatumWriter.new(schema)
          buffer = StringIO.new
          encoder = Avro::IO::BinaryEncoder.new(buffer)
          dw.write(test_event.to_hash, encoder)

          subject.decode(buffer.string) do |event|
            insist {event.get("[#{avro_target}][foo]")} == test_event.get("foo")
            insist {event.get("[#{avro_target}][bar]")} == test_event.get("bar")
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

        context "base64_encoding is false" do
          let (:avro_config) { super().merge('base64_encoding' => false) }

          it "should return avro data from a LogStash::Event not base64 encoded" do
            got_event = false
            subject.on_event do |event, data|
              schema = Avro::Schema.parse(avro_config['schema_uri'])
              datum = StringIO.new(data)
              decoder = Avro::IO::BinaryDecoder.new(datum)
              datum_reader = Avro::IO::DatumReader.new(schema)
              record = datum_reader.read(decoder)

              expect(event).to be_a_kind_of(LogStash::Event)
              expect(event.get("foo")).to eq(test_event.get("foo"))
              expect(event.get("bar")).to eq(test_event.get("bar"))
              got_event = true
            end
            subject.encode(test_event)
            expect(got_event).to be true
          end
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
end
