# frozen_string_literal: true

# This integration test measures the statistics JSON size reduction when using
# statistics.unassigned.include=false for a producer with a 1000-partition topic.
#
# Producers never own partitions, so all partition data is unassigned.
# With the filter enabled, the topics section is empty, yielding significant savings.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: Filtered stats are significantly smaller (test passes)
# - 1: No significant reduction or error (test fails)

require "rdkafka"
require "securerandom"
require "json"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "stats-integration-producer-#{SecureRandom.hex(6)}"
PARTITIONS = 1_000

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end

# Producing to a just-created topic can transiently fail while this producer client has not yet
# fetched the new topic's metadata (the create/metadata waits above only cover the admin client):
# the delivery report comes back as unknown_topic_or_part, or the partition leaders are not
# elected yet (leader_not_available).
#
# With 1000 partitions the leader election can also simply outlast a single wait budget. librdkafka
# then keeps the message queued (message.timeout.ms, 5 minutes by default) rather than failing it,
# so the handle stays pending and the wait gives up with a WaitTimeoutError. That is a RuntimeError,
# not an RdkafkaError, so it needs its own rescue - the code-based one below can never see it.
# The wait itself uses the library default (Defaults::HANDLE_WAIT_TIMEOUT_MS, 60s), matching the
# sibling statistics_unassigned_* specs that produce to the same 1000-partition topic; the 15s
# budget this spec used to pass was a quarter of that and was the flake.
PRODUCE_RETRYABLE = %i[unknown_topic_or_part leader_not_available].freeze
PRODUCE_MAX_ATTEMPTS = 30
# A timed-out wait already burned the full 60s budget, so retry it far fewer times than the cheap
# error path to keep the worst case bounded.
PRODUCE_MAX_TIMEOUTS = 3

def produce_and_wait(producer, topic)
  attempts = 0
  timeouts = 0

  begin
    producer.produce(topic: topic, payload: "test").wait
  rescue Rdkafka::AbstractHandle::WaitTimeoutError
    timeouts += 1
    raise if timeouts >= PRODUCE_MAX_TIMEOUTS

    retry
  rescue Rdkafka::RdkafkaError => e
    attempts += 1
    raise if attempts >= PRODUCE_MAX_ATTEMPTS || !PRODUCE_RETRYABLE.include?(e.code)

    sleep 0.5
    retry
  end
end

has_partitions = ->(stats) {
  stats.any? { |s| (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100 }
}

# --- Unfiltered producer ---
unfiltered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { unfiltered_stats << published }

unfiltered_producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": true
).producer

produce_and_wait(unfiltered_producer, TOPIC)

(30 * 20).times do
  break if has_partitions.call(unfiltered_stats)
  sleep 0.05
end

unfiltered_producer.close

# --- Filtered producer ---
filtered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { filtered_stats << published }

filtered_producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": false
).producer

produce_and_wait(filtered_producer, TOPIC)

(10 * 20).times do
  break if filtered_stats.size >= 2
  sleep 0.05
end

filtered_producer.close

Rdkafka::Config.statistics_callback = nil

# --- Cleanup ---
begin
  admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
end
admin.close

# --- Results ---
unfiltered_stat = unfiltered_stats.reverse.find do |s|
  (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100
end
unfiltered_json = JSON.generate(unfiltered_stat)
filtered_json = JSON.generate(filtered_stats.last)

unfiltered_size = unfiltered_json.bytesize
filtered_size = filtered_json.bytesize
reduction = ((1.0 - filtered_size.to_f / unfiltered_size) * 100).round(1)

puts
puts "Producer statistics JSON size (#{PARTITIONS} partitions):"
puts "  Unfiltered: #{unfiltered_size} bytes"
puts "  Filtered:   #{filtered_size} bytes"
puts "  Reduction:  #{reduction}%"
puts

if filtered_size < unfiltered_size / 2
  puts "PASS: Filtered stats are #{reduction}% smaller"
  exit(0)
else
  puts "FAIL: Expected at least 50% reduction, got #{reduction}%"
  exit(1)
end
