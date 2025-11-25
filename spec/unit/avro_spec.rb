# encoding: utf-8
require 'logstash/devutils/rspec/spec_helper'
require 'insist'
require 'avro'
require 'base64'
require 'logstash/codecs/avro'
require 'logstash/event'
require 'logstash/plugin_mixins/ecs_compatibility_support/spec_helper'

describe LogStash::Codecs::Avro, :ecs_compatibility_support, :aggregate_failures do
  let(:paths) do
    {
      # path has to be created, otherwise config :path validation fails
      # and since we cannot control the chmod operations on paths, we should stub file readable? and writable? operations
      :test_path => "spec/unit/resources/do_not_remove_path"
    }
  end

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
      receive(:fetch_schema).and_return(avro_config['schema_uri'])
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

      context "with binary encoding" do
        let (:avro_config) { super().merge('encoding' => 'binary') }

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

        context "with binary encoding" do
          let (:avro_config) { super().merge('encoding' => 'binary') }

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
      receive(:fetch_schema).and_return(avro_config['schema_uri'])
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

  context "remote schema registry" do
    let(:test_schema) do
      '{"type": "record", "name": "Test",
         "fields": [{"name": "foo", "type": ["null", "string"]},
                    {"name": "bar", "type": "int"}]}'
    end

    subject do
      allow_any_instance_of(LogStash::Codecs::Avro).to receive(:fetch_schema).and_return(test_schema)
      next LogStash::Codecs::Avro.new(avro_config)
    end

    context "basic authentication" do

      context "with both username and password" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
            'username' => 'test_user',
            'password' => 'test_&^%$!password'
          }
        end

        it "uses user and password" do
          auth = subject.send(:build_basic_auth)
          expect(auth).to eq({:user => 'test_user', :password => 'test_&^%$!password'})
        end

        it "includes valid credentials in auth hash" do
          auth = subject.send(:build_basic_auth)
          expect(auth[:user]).not_to be_empty
          expect(auth[:password]).not_to be_empty
        end
      end

      context "with only username" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
            'username' => 'test_user'
          }
        end

        it "raises ConfigurationError" do
          expect { subject.send(:build_basic_auth) }.to raise_error(LogStash::ConfigurationError, /`username` requires `password`/)
        end
      end

      context "with only password" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
            'password' => 'test_&^%$!password'
          }
        end

        it "raises ConfigurationError" do
          expect { subject.send(:build_basic_auth) }.to raise_error(LogStash::ConfigurationError, /`password` is not allowed unless `username` is specified/)
        end
      end

      context "with empty username" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
            'username' => '',
            'password' => 'test_&^%$!password'
          }
        end

        it "raises ConfigurationError" do
          expect { subject.send(:build_basic_auth) }.to raise_error(LogStash::ConfigurationError, /Empty `username` or `password` is not allowed/)
        end
      end

      context "with empty password" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
            'username' => 'test_user',
            'password' => ''
          }
        end

        it "raises ConfigurationError" do
          expect { subject.send(:build_basic_auth) }.to raise_error(LogStash::ConfigurationError, /Empty `username` or `password` is not allowed/)
        end
      end

      context "with neither username nor password" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc'
          }
        end

        it "returns empty hash" do
          auth = subject.send(:build_basic_auth)
          expect(auth).to be_empty
        end
      end

      context "with unsecure connection and credentials" do
        let(:avro_config) do
          {
            'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
            'username' => 'test_user',
            'password' => 'test_&^%$!password'
          }
        end

        it "still returns valid auth hash" do
          allow(subject.logger).to receive(:warn)
          auth = subject.send(:build_basic_auth)
          expect(auth).to eq({:user => 'test_user', :password => 'test_&^%$!password'})
        end
      end
    end

    context "secured connection against schema registry" do

      before do
        allow(File).to receive(:readable?).and_return(true)
        allow(File).to receive(:writable?).and_return(false)
      end

      context "explicit and inferred SSL" do
        context "with explicit ssl_enabled => true" do
          let(:avro_config) do
            {
              'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
              'ssl_enabled' => true
            }
          end

          it "enables SSL" do
            expect(subject.instance_variable_get(:@ssl_enabled)).to be true
          end
        end

        context "with HTTPS URI (inferred ssl_enabled)" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc'
            }
          end

          it "automatically enables SSL for HTTPS URIs" do
            subject.send(:validate_ssl_settings!)
            expect(subject.instance_variable_get(:@ssl_enabled)).to be true
          end
        end

        context "with explicit ssl_enabled => false" do
          let(:avro_config) do
            {
              'schema_uri' => 'http://schema-registry.example.com/schema.avsc',
              'ssl_enabled' => false
            }
          end

          it "disables SSL" do
            expect(subject.instance_variable_get(:@ssl_enabled)).to be false
          end
        end

        context "with explicit ssl_enabled => true and HTTPS URI" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_enabled' => false
            }
          end

          it "requires ssl_enabled => true" do
            expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                  LogStash::ConfigurationError,
                                                                  /Secured https:\/\/schema-registry.example.com\/schema.avsc connection requires `ssl_enabled => true`. /
                                                                )
          end
        end
      end

      context "SSL verification" do
        context "with ssl_verification_mode => 'full'" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_verification_mode' => 'full'
            }
          end

          it "sets verification mode to full" do
            expect(subject.instance_variable_get(:@ssl_verification_mode)).to eq('full')
          end

          it "builds SSL options with default verify mode" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:verify]).to eq(:default)
          end
        end

        context "with ssl_verification_mode => 'none'" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_verification_mode' => 'none'
            }
          end

          it "sets verification mode to none" do
            expect(subject.instance_variable_get(:@ssl_verification_mode)).to eq('none')
          end

          it "builds SSL options with disable verify mode" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:verify]).to eq(:disable)
          end
        end

        context "with default ssl_verification_mode" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc'
            }
          end

          it "defaults to 'full'" do
            expect(subject.instance_variable_get(:@ssl_verification_mode)).to eq('full')
          end
        end
      end

      context "keystore configuration" do
        context "with ssl_keystore_path" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_keystore_path' => paths[:test_path],
              'ssl_keystore_password' => 'keystore_pass',
              'ssl_keystore_type' => 'jks'
            }
          end

          it "configures keystore options" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:keystore]).to eq(paths[:test_path])
            expect(ssl_options[:keystore_password]).to eq('keystore_pass')
            expect(ssl_options[:keystore_type]).to eq('jks')
          end
        end

        context "with ssl_keystore_path and pkcs12 type" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_keystore_path' => paths[:test_path],
              'ssl_keystore_password' => 'keystore_pass',
              'ssl_keystore_type' => 'pkcs12'
            }
          end

          it "configures pkcs12 keystore" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:keystore_type]).to eq('pkcs12')
          end
        end
      end

      context "truststore configuration" do
        context "with ssl_truststore_path" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_truststore_path' => paths[:test_path],
              'ssl_truststore_password' => 'truststore_pass',
              'ssl_truststore_type' => 'jks'
            }
          end

          it "configures truststore options" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:truststore]).to eq(paths[:test_path])
            expect(ssl_options[:truststore_password]).to eq('truststore_pass')
            expect(ssl_options[:truststore_type]).to eq('jks')
          end
        end

        context "with ssl_truststore_path and pkcs12 type" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_truststore_path' => paths[:test_path],
              'ssl_truststore_password' => 'truststore_pass',
              'ssl_truststore_type' => 'pkcs12'
            }
          end

          it "configures pkcs12 truststore" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:truststore_type]).to eq('pkcs12')
          end
        end
      end

      context "CA configuration" do

        context "with single CA file" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_certificate_authorities' => [paths[:test_path]]
            }
          end

          it "configures CA certificate" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:ca_file]).to eq(paths[:test_path])
          end
        end

        context "with multiple CA files" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_certificate_authorities' => [paths[:test_path], paths[:test_path]]
            }
          end

          it "raises ConfigurationError for multiple CAs" do
            expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                  LogStash::ConfigurationError,
                                                                  /Multiple values on `ssl_certificate_authorities` are not supported/
                                                                )
          end
        end

        context "with empty ssl_certificate_authorities" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_certificate_authorities' => []
            }
          end

          it "raises ConfigurationError" do
            expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                  LogStash::ConfigurationError,
                                                                  /`ssl_certificate_authorities` cannot be empty/
                                                                )
          end
        end
      end

      context "cipher suites" do
        context "with specified cipher suites" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_cipher_suites' => %w[TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]
            }
          end

          it "configures cipher suites" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:cipher_suites]).to eq(%w[TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256])
          end
        end

        context "without cipher suites" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc'
            }
          end

          it "does not include cipher_suites in SSL options" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options).not_to have_key(:cipher_suites)
          end
        end
      end

      context "supported protocols" do
        context "with TLSv1.2 and TLSv1.3" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_supported_protocols' => %w[TLSv1.2 TLSv1.3]
            }
          end

          it "configures supported protocols" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:protocols]).to eq(%w[TLSv1.2 TLSv1.3])
          end
        end

        context "with only TLSv1.3" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_supported_protocols' => ['TLSv1.3']
            }
          end

          it "configures only TLSv1.3" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options[:protocols]).to eq(['TLSv1.3'])
          end
        end

        context "with empty ssl_supported_protocols" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
              'ssl_supported_protocols' => []
            }
          end

          it "does not include protocols in SSL options" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options).not_to have_key(:protocols)
          end
        end

        context "without ssl_supported_protocols" do
          let(:avro_config) do
            {
              'schema_uri' => 'https://schema-registry.example.com/schema.avsc'
            }
          end

          it "uses default (no protocols key)" do
            ssl_options = subject.send(:build_ssl_options)
            expect(ssl_options).not_to have_key(:protocols)
          end
        end
      end

      context "SSL validations" do
        context "PEM certificate validation" do
          context "when both ssl_certificate and ssl_keystore_path are set" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_certificate' => paths[:test_path],
                'ssl_key' => paths[:test_path],
                'ssl_keystore_path' => paths[:test_path],
                'ssl_keystore_password' => 'password'
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_certificate` and `ssl_keystore_path` cannot be used together/
                                                                  )
            end
          end

          context "when ssl_certificate is set without ssl_key" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_certificate' => paths[:test_path]
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_certificate` requires `ssl_key`/
                                                                  )
            end
          end

          context "when ssl_key is set without ssl_certificate" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_key' => paths[:test_path]
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_key` is not allowed unless `ssl_certificate` is specified/
                                                                  )
            end
          end
        end

        context "keystore validation" do
          context "when ssl_keystore_password is set without ssl_keystore_path" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_keystore_password' => 'password'
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_keystore_password` is not allowed unless `ssl_keystore_path` is specified/
                                                                  )
            end
          end

          context "when ssl_keystore_password is empty" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_keystore_path' => paths[:test_path],
                'ssl_keystore_password' => ''
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_keystore_password` cannot be empty/
                                                                  )
            end
          end

          context "when ssl_keystore_type is set without ssl_keystore_path" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_keystore_type' => 'jks'
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_keystore_type` is not allowed unless `ssl_keystore_path` is specified/
                                                                  )
            end
          end
        end

        context "truststore validation" do
          context "when ssl_truststore_password is set without ssl_truststore_path" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_truststore_password' => 'password'
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_truststore_password` is not allowed unless `ssl_truststore_path` is specified/
                                                                  )
            end
          end

          context "when ssl_truststore_password is empty" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_truststore_path' => paths[:test_path],
                'ssl_truststore_password' => ''
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_truststore_password` cannot be empty/
                                                                  )
            end
          end

          context "when both ssl_truststore_path and ssl_certificate_authorities are set" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_truststore_path' => paths[:test_path],
                'ssl_truststore_password' => 'password',
                'ssl_certificate_authorities' => [paths[:test_path]]
              }
            end

            it "raises ConfigurationError" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_truststore_path` and `ssl_certificate_authorities` cannot be used together/
                                                                  )
            end
          end
        end

        context "verification mode validation" do
          context "when ssl_truststore_path is set with verification mode none" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_truststore_path' => paths[:test_path],
                'ssl_truststore_password' => 'password',
                'ssl_verification_mode' => 'none'
              }
            end

            it "requires `ssl_verification_mode` => 'full'" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_truststore_path` requires `ssl_verification_mode` to be `full`/
                                                                  )
            end
          end

          context "when ssl_truststore_password is set with verification mode none" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_truststore_password' => 'password',
                'ssl_verification_mode' => 'none'
              }
            end

            it "requires `ssl_verification_mode => 'full'" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_truststore_password` requires `ssl_truststore_path` and `ssl_verification_mode => 'full'`/
                                                                  )
            end
          end

          context "when ssl_certificate_authorities is set with verification mode none" do
            let(:avro_config) do
              {
                'schema_uri' => 'https://schema-registry.example.com/schema.avsc',
                'ssl_certificate_authorities' => [paths[:test_path]],
                'ssl_verification_mode' => 'none'
              }
            end

            it "requires `ssl_verification_mode => 'full'" do
              expect { subject.send(:validate_ssl_settings!) }.to raise_error(
                                                                    LogStash::ConfigurationError,
                                                                    /`ssl_certificate_authorities` requires `ssl_verification_mode` to be `full`/
                                                                  )
            end
          end
        end
      end
    end
  end
end
