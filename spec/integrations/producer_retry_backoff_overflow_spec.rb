# frozen_string_literal: true

# This integration test verifies that a message the broker keeps rejecting with a retriable error
# waits `retry.backoff.max.ms` before each retry, however many retries it takes.
#
# librdkafka doubles the backoff of a produced message on each retry and computed it in a 32-bit
# `int` before capping it at `retry.backoff.max.ms` (confluentinc/librdkafka#5646). From the 29th
# retry with `retry.backoff.ms: 10` (the 26th with the default 100) the value overflowed to zero,
# a negative or a small number, so the message was retried at once, in a tight loop, until
# `message.timeout.ms`. karafka-rdkafka carries `dist/patches/rdkafka_retry_msgq_backoff_overflow.patch`
# for it.
#
# The mock cluster fails the first 40 Produce requests with NOT_ENOUGH_REPLICAS and records when it
# receives each of them. With `retry.backoff.ms: 10` and `retry.backoff.max.ms: 100` the backoff
# reaches the cap at the 5th retry, so from there every retry must come at least 100 ms after the
# previous attempt.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker.
#
# Exit codes:
# - 0: the message was delivered and every capped retry waited `retry.backoff.max.ms`
# - 1: a capped retry came early, or the message was not delivered

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

TOPIC = "it-producer-retry-backoff-overflow-#{SecureRandom.hex(6)}"
FAILED_ATTEMPTS = 40
RETRY_BACKOFF_MS = 10
RETRY_BACKOFF_MAX_MS = 100
FIRST_CAPPED_RETRY = 5

failures = []

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)

  producer = cluster.config(
    "retry.backoff.ms": RETRY_BACKOFF_MS,
    "retry.backoff.max.ms": RETRY_BACKOFF_MAX_MS,
    "linger.ms": 0,
    "message.timeout.ms": 60_000
  ).producer

  # Connect first, so that only the Produce requests of the message hit the injected errors
  producer.partition_count(TOPIC)

  cluster.push_request_errors(:produce, *Array.new(FAILED_ATTEMPTS, :not_enough_replicas))
  cluster.start_request_tracking

  report = producer
    .produce(topic: TOPIC, payload: "payload", partition: 0)
    .wait(max_wait_timeout_ms: 60_000)

  failures << "expected no delivery error, got #{report.error}" if report.error

  timestamps = cluster.request_timestamps(:produce)

  if timestamps.size == FAILED_ATTEMPTS + 1
    timestamps.each_cons(2).with_index(1) do |(previous, current), retry_number|
      next if retry_number < FIRST_CAPPED_RETRY

      waited_ms = (current - previous) / 1_000.0

      next if waited_ms >= RETRY_BACKOFF_MAX_MS

      failures << format("retry %d came %.1f ms after the previous attempt", retry_number, waited_ms)
    end
  else
    failures << "expected #{FAILED_ATTEMPTS + 1} Produce requests, got #{timestamps.size}"
  end
rescue => e
  failures << "#{e.class}: #{e.message}"
ensure
  producer&.close
end

if failures.empty?
  puts "OK"
  exit(0)
end

failures.each { |failure| puts "FAIL: #{failure}" }
exit(1)
