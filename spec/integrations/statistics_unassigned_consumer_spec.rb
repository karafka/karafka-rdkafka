# frozen_string_literal: true

# This integration test verifies that a single consumer with
# statistics.unassigned.include=false still reports all assigned partitions.
#
# A single consumer subscribes to a 1000-partition topic and gets all partitions
# assigned. With the filter enabled, all partitions should still appear in the
# statistics because they are assigned (fetch_state != none).
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: All assigned partitions are present in filtered stats (test passes)
# - 1: Partitions are missing or error (test fails)

require "rdkafka"
require "securerandom"
require "json"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "stats-integration-consumer-#{SecureRandom.hex(6)}"
PARTITIONS = 1_000

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end

# --- Filtered consumer (all partitions should be assigned and reported) ---
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

(60 * 20).times do
  break if filtered_stats.any? { |s|
    (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100
  }
  begin
    filtered_consumer.poll(50)
  rescue Rdkafka::RdkafkaError
    nil
  end
end

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
filtered_stat = filtered_stats.reverse.find do |s|
  (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100
end

if filtered_stat.nil?
  puts "FAIL: No stats with partition data found"
  exit(1)
end

partitions = filtered_stat["topics"][TOPIC]["partitions"]
partition_count = partitions.keys.count { |k| k != "-1" }

puts
puts "Consumer with #{PARTITIONS} assigned partitions (filter enabled):"
puts "  Partitions reported: #{partition_count}"
puts "  JSON size:           #{JSON.generate(filtered_stat).bytesize} bytes"
puts

if partition_count >= PARTITIONS
  puts "PASS: All #{partition_count} assigned partitions present in filtered stats"
  exit(0)
else
  puts "FAIL: Expected #{PARTITIONS} partitions, got #{partition_count}"
  exit(1)
end
