## 3.5.0
  - Add SSL/TLS support for HTTPS schema registry connections
    - Add `ssl_enabled` option to enable/disable SSL
    - Add `ssl_certificate` and `ssl_key` options for PEM-based client authentication (unencrypted keys only)
    - Add `ssl_certificate_authorities` option for PEM-based server certificate validation
    - Add `ssl_verification_mode` option to control SSL verification (full, none)
    - Add `ssl_cipher_suites` option to configure cipher suites
    - Add `ssl_supported_protocols` option to configure TLS protocol versions (TLSv1.1, TLSv1.2, TLSv1.3)
    - Add `ssl_truststore_path` and `ssl_truststore_password` options for server certificate validation (JKS/PKCS12)
    - Add `ssl_keystore_path` and `ssl_keystore_password` options for mutual TLS authentication (JKS/PKCS12)
    - Add `ssl_truststore_type` and `ssl_keystore_type` options (JKS or PKCS12)
  - Add HTTP proxy support with `proxy` option
  - Add HTTP basic authentication support with `username` and `password` options

## 3.4.1
  - Fixes `(Errno::ENOENT) No such file or directory` error [#43](https://github.com/logstash-plugins/logstash-codec-avro/pull/43)

## 3.4.0
  - Add `encoding` option to select the encoding of Avro payload, could be `binary` or `base64` [#39](https://github.com/logstash-plugins/logstash-codec-avro/pull/39)

## 3.3.1
  - Pin avro gem to 1.10.x, as 1.11+ requires ruby 2.6+ [#37](https://github.com/logstash-plugins/logstash-codec-avro/pull/37)

## 3.3.0
  - Add ECS support. Add target option and event.original [#36](https://github.com/logstash-plugins/logstash-codec-avro/pull/36)

## 3.2.4
  - [DOC] Add clarifications on partial deserialization [#35](https://github.com/logstash-plugins/logstash-codec-avro/pull/35)

## 3.2.3
  - Update gemspec summary

## 3.2.2
  - Fix some documentation issues

## 3.2.0
 - Fixed an issue with the encoding that prevented certain fields from being serialized in a way compatible with the Kafka plugins

## 3.1.0
 - Introduce `tag_on_failure` option to tag events with `_avroparsefailure` instead of throwing an exception when decoding

## 3.0.0
 - breaking: Update to new Event API

## 2.0.4
 - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.0.3
 - New dependency requirements for logstash-core for the 5.0 release

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

