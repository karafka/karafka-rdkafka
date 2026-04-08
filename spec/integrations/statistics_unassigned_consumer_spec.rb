# frozen_string_literal: true

# This integration test measures the statistics JSON size reduction when using
# statistics.unassigned.include=false for a consumer with a 1000-partition topic.
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
TOPIC = "stats-integration-consumer-#{SecureRandom.hex(6)}"
PARTITIONS = 1_000

puts "Creating topic #{TOPIC} with #{PARTITIONS} partitions..."

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end


puts "Topic created."

poll_until = ->(consumer, target, &condition) {
  (30 * 20).times do
    break if condition.call(target)
    begin
      consumer.poll(50)
    rescue Rdkafka::RdkafkaError
      nil
    end
  end
}

# --- Unfiltered consumer (run first to wait for metadata) ---
unfiltered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { unfiltered_stats << published }

unfiltered_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "stats-unfiltered-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": true
).consumer

unfiltered_consumer.subscribe(TOPIC)
poll_until.call(unfiltered_consumer, unfiltered_stats) { |s|
  s.any? { |stat| (stat["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100 }
}
unfiltered_consumer.close

# --- Filtered consumer ---
filtered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { filtered_stats << published }

filtered_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "stats-filtered-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": false
).consumer

filtered_consumer.subscribe(TOPIC)
poll_until.call(filtered_consumer, filtered_stats) { |s| s.size >= 2 }
filtered_consumer.close

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
puts "Consumer statistics JSON size (#{PARTITIONS} partitions):"
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
