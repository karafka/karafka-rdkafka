# frozen_string_literal: true

# This integration test verifies that closing a producer does not deadlock when a callback running
# on the polling thread re-enters the native handle via `with_inner` (findings #4 + #5 of the
# NativeKafka synchronization redesign).
#
# Before the fix, `NativeKafka#close` ran inside `#synchronize`, holding the exclusive access mutex
# while it joined the polling thread. Any callback firing on that thread (a delivery report, or the
# documented oauthbearer token refresh) that called back into `with_inner` blocked on the same
# mutex, so the poll never returned, the join never completed, and `close` hung forever. The fix
# joins the polling thread before taking exclusive access, and updates the in-progress counter
# under a single mutex (so it can never be lost, which previously could also hang or crash close).
#
# We make the delivery callback re-enter `with_inner` on the polling thread, produce a batch, then
# close. A watchdog thread aborts the process if close does not return in time (i.e. it deadlocked).
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: close completed (no deadlock)
# - 1: close deadlocked (timed out)

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "native-kafka-close-deadlock-#{SecureRandom.hex(6)}"
MESSAGES = 100
CLOSE_TIMEOUT_S = 25

# Make sure the topic exists so deliveries (and their callbacks) happen promptly.
admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
begin
  admin.create_topic(TOPIC, 4, 1).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
ensure
  admin.close
end

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer

# The delivery callback runs on the polling thread. Re-entering `with_inner` here is exactly the
# shape that used to deadlock against a concurrent close.
producer.delivery_callback = lambda do |_report|
  native_kafka = producer.instance_variable_get(:@native_kafka)

  begin
    native_kafka.with_inner { |inner| Rdkafka::Bindings.rd_kafka_name(inner) }
  rescue Rdkafka::ClosedInnerError
    nil
  end
end

MESSAGES.times do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}", key: "key-#{i}")
end

closed_ok = false
watchdog = Thread.new do
  sleep(CLOSE_TIMEOUT_S)
  unless closed_ok
    warn "FAIL: producer.close did not return within #{CLOSE_TIMEOUT_S}s - close deadlocked against " \
         "the polling-thread callback re-entering with_inner"
    exit!(1)
  end
end

# With the bug, this hangs forever (watchdog fires). With the fix, it returns promptly.
producer.close
closed_ok = true
watchdog.kill

puts "PASS: producer.close completed without deadlocking against the polling-thread callback"
exit(0)
