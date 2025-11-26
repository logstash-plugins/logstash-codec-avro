#!/bin/bash
# Setup Kafka and create test topics

set -ex
# check if KAFKA_VERSION env var is set
if [ -n "${KAFKA_VERSION+1}" ]; then
  echo "KAFKA_VERSION is $KAFKA_VERSION"
else
   KAFKA_VERSION=4.1.0
fi

KAFKA_MAJOR_VERSION="${KAFKA_VERSION%%.*}"

export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"

rm -rf build
mkdir build

echo "Setup Kafka version $KAFKA_VERSION"
if [ ! -e "kafka_2.13-$KAFKA_VERSION.tgz" ]; then
  echo "Kafka not present locally, downloading"
  curl -s -o "kafka_2.13-$KAFKA_VERSION.tgz" "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
fi
cp "kafka_2.13-$KAFKA_VERSION.tgz" "build/kafka.tgz"
mkdir "build/kafka" && tar xzf "build/kafka.tgz" -C "build/kafka" --strip-components 1

echo "Use KRaft for Kafka version $KAFKA_VERSION"
echo "log.dirs=${PWD}/build/kafka-logs" >> build/kafka/config/server.properties

build/kafka/bin/kafka-storage.sh format \
  --cluster-id $(build/kafka/bin/kafka-storage.sh random-uuid) \
  --config build/kafka/config/server.properties \
  --ignore-formatted \
  --standalone

echo "Starting Kafka broker"
build/kafka/bin/kafka-server-start.sh -daemon "build/kafka/config/server.properties" --override advertised.host.name=127.0.0.1 --override log.dirs="${PWD}/build/kafka-logs"
sleep 10

echo "Setup Confluent Platform"
# check if CONFLUENT_VERSION env var is set
if [ -n "${CONFLUENT_VERSION+1}" ]; then
  echo "CONFLUENT_VERSION is $CONFLUENT_VERSION"
else
   CONFLUENT_VERSION=8.0.0
fi
if [ ! -e "confluent-community-$CONFLUENT_VERSION.tar.gz" ]; then
  echo "Confluent Platform not present locally, downloading"
  CONFLUENT_MINOR=$(echo "$CONFLUENT_VERSION" | sed -n 's/^\([[:digit:]]*\.[[:digit:]]*\)\.[[:digit:]]*$/\1/p')
  echo "CONFLUENT_MINOR is $CONFLUENT_MINOR"
  curl -s -o "confluent-community-$CONFLUENT_VERSION.tar.gz" "http://packages.confluent.io/archive/$CONFLUENT_MINOR/confluent-community-$CONFLUENT_VERSION.tar.gz"
fi
echo "Extracting confluent-community-$CONFLUENT_VERSION.tar.gz to build"
mkdir "build/confluent_platform" && tar xzf "confluent-community-$CONFLUENT_VERSION.tar.gz" -C "build/confluent_platform" --strip-components 1

echo "Configuring TLS on Schema registry"
rm -Rf tls_repository
mkdir tls_repository
./setup_keystore_and_truststore.sh
# configure schema-registry to handle https on 8083 port
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' 's/http:\/\/0.0.0.0:8081/http:\/\/0.0.0.0:8081, https:\/\/0.0.0.0:8083/g' "build/confluent_platform/etc/schema-registry/schema-registry.properties"
else
  sed -i 's/http:\/\/0.0.0.0:8081/http:\/\/0.0.0.0:8081, https:\/\/0.0.0.0:8083/g' "build/confluent_platform/etc/schema-registry/schema-registry.properties"
fi
echo "ssl.keystore.location=`pwd`/tls_repository/schema_reg.jks" >> "build/confluent_platform/etc/schema-registry/schema-registry.properties"
echo "ssl.keystore.password=changeit" >> "build/confluent_platform/etc/schema-registry/schema-registry.properties"
echo "ssl.key.password=changeit" >> "build/confluent_platform/etc/schema-registry/schema-registry.properties"

cp "build/confluent_platform/etc/schema-registry/schema-registry.properties" "build/confluent_platform/etc/schema-registry/schema-registry-mutual.properties"
echo "ssl.truststore.location=`pwd`/tls_repository/clienttruststore.jks" >> "build/confluent_platform/etc/schema-registry/schema-registry-mutual.properties"
echo "ssl.truststore.password=changeit" >> "build/confluent_platform/etc/schema-registry/schema-registry-mutual.properties"
echo "confluent.http.server.ssl.client.authentication=REQUIRED" >> "build/confluent_platform/etc/schema-registry/schema-registry-mutual.properties"

cp "build/confluent_platform/etc/schema-registry/schema-registry.properties" "build/confluent_platform/etc/schema-registry/authed-schema-registry.properties"
echo "authentication.method=BASIC" >> "build/confluent_platform/etc/schema-registry/authed-schema-registry.properties"
echo "authentication.roles=admin,developer,user" >> "build/confluent_platform/etc/schema-registry/authed-schema-registry.properties"
echo "authentication.realm=SchemaRegistry-Props" >> "build/confluent_platform/etc/schema-registry/authed-schema-registry.properties"
cp fixtures/jaas.config "build/confluent_platform/etc/schema-registry"

echo "Setting up a test topic"
build/kafka/bin/kafka-topics.sh --create --partitions 3 --replication-factor 1 --topic logstash_integration_topic_plain --bootstrap-server localhost:9092

cp fixtures/pwd "build/confluent_platform/etc/schema-registry"
echo "Setup complete, running specs"
