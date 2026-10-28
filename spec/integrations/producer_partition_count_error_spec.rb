# frozen_string_literal: true

# This integration test verifies that `produce(partition_key:)` raises
# `Rdkafka::Producer::PartitionCountError` when the partition count of a topic cannot be fetched
# and none is cached.
#
# It used to raise the bare metadata error (`transport` when no broker answers), which did not
# say that the partition count lookup failed or for which topic. The new error names the topic,
# keeps the code of the original error and has it as its `cause`, so `rescue RdkafkaError` and
# `#code` checks keep working.
#
# A producer pointed at an unreachable address stands in for the outage, so no broker is needed.
#
# Exit codes:
# - 0: the dedicated error was raised with the original error as its cause
# - 1: no error, a different error, or the error lost the topic, code or cause

require "rdkafka"
require "securerandom"

$stdout.sync = true

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

unless error.is_a?(Rdkafka::Producer::PartitionCountError)
  warn "FAIL: expected Rdkafka::Producer::PartitionCountError, got #{error.inspect}"
  exit(1)
end

if error.topic_name != TOPIC || !error.message.include?("'#{TOPIC}'")
  warn "FAIL: expected the error to name #{TOPIC}, got #{error.topic_name.inspect}: #{error.message}"
  exit(1)
end

unless error.cause.instance_of?(Rdkafka::RdkafkaError) && error.cause.code == error.code
  warn "FAIL: expected the original RdkafkaError as the cause, got #{error.cause.inspect}"
  exit(1)
end

unless %i[transport timed_out all_brokers_down resolve].include?(error.code)
  warn "FAIL: expected an outage error code, got #{error.code}"
  exit(1)
end

unless Rdkafka::Producer.partitions_count_cache.to_h[TOPIC].nil?
  warn "FAIL: the failed lookup was cached"
  exit(1)
end

puts "PASS: PartitionCountError (#{error.code}) raised with the original error as its cause"
exit(0)
