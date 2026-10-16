# frozen_string_literal: true

# This integration test verifies that a failed consumer close is reported, not lost.
#
# `rd_kafka_consumer_close` returns an error code, which used to be dropped at the FFI boundary,
# so closing a fatally-errored consumer looked like a clean close. Now:
#
# 1. Closing a consumer that has a fatal error passes a fatal `RdkafkaError` with the
#    "consumer close" prefix to `Rdkafka::Config.error_callback`. `#close` does not raise and
#    the consumer ends up closed.
# 2. Closing a healthy consumer reports nothing.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the failed close was reported and the clean close was not
# - 1: any scenario failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-consumer-close-fatal-#{SecureRandom.hex(6)}"
WAIT_MS = 30_000

failures = []
close_errors = []

Rdkafka::Config.error_callback = lambda do |error|
  close_errors << error if error.message_prefix == "consumer close"
end

def build_consumer
  Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "it-consumer-close-fatal-#{SecureRandom.hex(6)}",
    "auto.offset.reset": "earliest"
  ).consumer
end

def wait_for_assignment(consumer)
  deadline = Time.now + 30

  consumer.poll(100) while consumer.assignment.empty? && Time.now < deadline
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait(max_wait_timeout_ms: WAIT_MS)
admin.close

# Scenario 1: a fatally-errored consumer
begin
  consumer = build_consumer
  consumer.subscribe(TOPIC)
  wait_for_assignment(consumer)

  consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
    Rdkafka::Bindings.rd_kafka_test_fatal_error(inner, 47, "test fatal error")
  end

  begin
    consumer.close
  rescue => e
    failures << "fatal: #close raised #{e.class}: #{e.message}"
  end

  failures << "fatal: consumer is not closed" unless consumer.closed?

  if close_errors.size == 1
    error = close_errors.first
    failures << "fatal: error is not fatal" unless error.fatal?
    failures << "fatal: expected invalid_producer_epoch, got #{error.code}" unless error.code == :invalid_producer_epoch
  else
    failures << "fatal: expected 1 close error, got #{close_errors.size}"
  end
ensure
  consumer&.close
end

# Scenario 2: a healthy consumer
begin
  close_errors.clear

  consumer = build_consumer
  consumer.subscribe(TOPIC)
  wait_for_assignment(consumer)
  consumer.close

  failures << "clean: expected no close error, got #{close_errors.map(&:to_s)}" unless close_errors.empty?
ensure
  consumer&.close
end

if failures.empty?
  puts "OK"
  exit(0)
else
  failures.each { |failure| warn "FAIL: #{failure}" }
  exit(1)
end
