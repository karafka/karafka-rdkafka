# frozen_string_literal: true

# This integration test verifies that with statistics.unassigned.include=false
# and 10 consumers in the same group, each consumer only reports its assigned
# partitions (~100 each out of 1000 total).
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: Each consumer reports only its assigned partitions (test passes)
# - 1: Partition counts don't match expectations (test fails)

require "rdkafka"
require "securerandom"
require "json"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "stats-integration-cg-filtered-#{SecureRandom.hex(6)}"
PARTITIONS = 1_000
CONSUMER_COUNT = 10

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end

group_id = "stats-group-filtered-#{SecureRandom.hex(4)}"
all_stats = []
Rdkafka::Config.statistics_callback = ->(published) { all_stats << published }

consumers = CONSUMER_COUNT.times.map do
  Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "statistics.interval.ms": 500,
    "statistics.unassigned.include": false
  ).consumer
end

consumers.each { |c| c.subscribe(TOPIC) }

# Poll all consumers until each has reported partition data in stats.
# Use the "name" field to identify per-consumer stats.
(120 * 20).times do
  consumers.each do |c|
    c.poll(25)
  rescue Rdkafka::RdkafkaError
    nil
  end

  names_with_partitions = all_stats
    .select { |s| (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 1 }
    .map { |s| s["name"] }
    .uniq

  break if names_with_partitions.size >= CONSUMER_COUNT
end

consumers.each(&:close)
Rdkafka::Config.statistics_callback = nil

# --- Cleanup ---
begin
  admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
end
admin.close

# --- Results ---
# Group latest stats by consumer name, pick only those with partition data
latest_by_name = {}
all_stats.each do |s|
  topic_data = (s["topics"][TOPIC] || {}).fetch("partitions", {})
  next if topic_data.empty?

  latest_by_name[s["name"]] = {
    partitions: topic_data.keys.count { |k| k != "-1" },
    json_size: JSON.generate(s).bytesize
  }
end

puts
puts "#{CONSUMER_COUNT} consumers, #{PARTITIONS} partitions, filter enabled:"

total_reported = 0
total_json_size = 0
all_ok = true

latest_by_name.each do |name, data|
  puts "  #{name}: #{data[:partitions]} partitions, #{data[:json_size]} bytes"
  total_reported += data[:partitions]
  total_json_size += data[:json_size]

  next if data[:partitions] > 0 && data[:partitions] < PARTITIONS

  puts "  FAIL: Expected partial assignment, got #{data[:partitions]}"
  all_ok = false
end

puts "  Total reported: #{total_reported}"
unless latest_by_name.empty?
  avg_json_size = total_json_size / latest_by_name.size
  puts "  Avg JSON size per consumer: #{avg_json_size} bytes"
end
puts

if latest_by_name.size < CONSUMER_COUNT
  puts "FAIL: Only #{latest_by_name.size}/#{CONSUMER_COUNT} consumers reported partition data"
  exit(1)
elsif !all_ok
  puts "FAIL: Some consumers reported unexpected partition counts"
  exit(1)
elsif total_reported >= PARTITIONS
  puts "PASS: All #{CONSUMER_COUNT} consumers report only assigned partitions (#{total_reported} total)"
  exit(0)
else
  puts "FAIL: Total reported partitions (#{total_reported}) less than #{PARTITIONS}"
  exit(1)
end
