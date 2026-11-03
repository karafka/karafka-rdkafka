# frozen_string_literal: true

# This integration test verifies that a producer raises when a topic it already used gets a
# different topic config, instead of producing with the first config of that topic.
#
# librdkafka keeps one config per topic in a client: `rd_kafka_topic_new` for a known topic
# returns the existing handle and drops the new config. A later `acks: 0` produce to a topic first
# used with the defaults was sent with `acks: -1`, and a default produce to a topic first used
# with `acks: 0` was sent with `acks: 0`.
#
# Scenarios:
# 1. A default produce, then an `acks: 0` produce to the same topic raises
# 2. An `acks: 0` produce, then a default produce to the same topic raises
# 3. The same topic config again, and other configs for other topics, still produce
# 4. A new producer can use another config for a topic the first producer used
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: each different topic config for a used topic raised, and every other produce worked
# - 1: a different topic config for a used topic did not raise, or another produce failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

CONFIG = { "bootstrap.servers": "localhost:9092" }.freeze

ACKS_0 = { acks: 0 }.freeze

# Offset librdkafka reports for a message produced with `acks: 0`
NO_ACK_OFFSET = -1001

def raises_config_error?
  yield
  false
rescue Rdkafka::Config::ConfigError => e
  e.message.include?("already used with a different topic config")
end

default_first = "it-topic-config-per-topic-#{SecureRandom.uuid}"
acks_0_first = "it-topic-config-per-topic-#{SecureRandom.uuid}"

admin = Rdkafka::Config.new(CONFIG).admin

begin
  [default_first, acks_0_first].each do |topic|
    admin.create_topic(topic, 1, 1).wait(max_wait_timeout_ms: 15_000)
  end

  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
  until [default_first, acks_0_first].all? { |topic| admin.metadata(topic).topics.first&.dig(:partitions)&.any? }
    exit(1) if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
  end
ensure
  admin.close
end

producer = Rdkafka::Config.new(CONFIG).producer

begin
  exit(1) unless producer.produce(topic: default_first, payload: "x").wait(max_wait_timeout_ms: 30_000).offset >= 0
  exit(1) unless raises_config_error? { producer.produce(topic: default_first, payload: "x", topic_config: ACKS_0) }

  report = producer.produce(topic: acks_0_first, payload: "x", topic_config: ACKS_0).wait(max_wait_timeout_ms: 30_000)
  exit(1) unless report.offset == NO_ACK_OFFSET
  exit(1) unless raises_config_error? { producer.produce(topic: acks_0_first, payload: "x") }

  exit(1) unless producer.produce(topic: default_first, payload: "x").wait(max_wait_timeout_ms: 30_000).offset >= 0

  report = producer.produce(topic: acks_0_first, payload: "x", topic_config: { acks: 0 }).wait(max_wait_timeout_ms: 30_000)
  exit(1) unless report.offset == NO_ACK_OFFSET
ensure
  producer.close
end

other_producer = Rdkafka::Config.new(CONFIG).producer

begin
  report = other_producer.produce(topic: default_first, payload: "x", topic_config: ACKS_0).wait(max_wait_timeout_ms: 30_000)
  exit(1) unless report.offset == NO_ACK_OFFSET
ensure
  other_producer.close
end

exit(0)
