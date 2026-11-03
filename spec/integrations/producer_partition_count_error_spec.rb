# frozen_string_literal: true

# This integration test verifies that `produce(partition_key:)` raises
# `Rdkafka::Producer::PartitionCountError` when the partition count of a topic cannot be fetched
# and none is cached.
#
# A producer pointed at an unreachable address stands in for the outage, so no broker is needed.
#
# Exit codes:
# - 0: the dedicated error was raised with the original error as its cause
# - 1: no error, a different error, or the error lost the topic, code or cause

require "rdkafka"
require "securerandom"

TOPIC = "partition-count-error-#{SecureRandom.hex(6)}"

Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new

producer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9",
  "message.timeout.ms": 1_000
).producer

error = nil

begin
  producer.produce(topic: TOPIC, payload: "payload", partition_key: "key")
rescue Rdkafka::RdkafkaError => e
  error = e
ensure
  producer.close
end

exit(1) unless error.is_a?(Rdkafka::Producer::PartitionCountError)
exit(1) unless error.topic_name == TOPIC && error.message.include?("'#{TOPIC}'")
exit(1) unless error.cause.instance_of?(Rdkafka::RdkafkaError) && error.cause.code == error.code
exit(1) unless %i[transport timed_out all_brokers_down resolve].include?(error.code)
exit(1) unless Rdkafka::Producer.partitions_count_cache.to_h[TOPIC].nil?

exit(0)
