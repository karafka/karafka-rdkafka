# frozen_string_literal: true

# This integration test verifies that with statistics.unassigned.include=true
# (default) and 10 consumers in the same group, each consumer reports ALL 1000
# partitions — including the ~900 it does not own.
#
# This contrasts with the filtered variant where each consumer only reports
# its ~100 assigned partitions.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: Each consumer reports all partitions (test passes)
# - 1: Some consumers are missing partitions (test fails)

require "rdkafka"
require "securerandom"
require "json"
require "socket"

# Skip when no Kafka broker is available (e.g. complementary CI without Kafka)
begin
  TCPSocket.new("localhost", 9092).close
rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, SocketError
  exit(0)
end

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "stats-integration-cg-unfiltered-#{SecureRandom.hex(6)}"
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

group_id = "stats-group-unfiltered-#{SecureRandom.hex(4)}"
all_stats = []
Rdkafka::Config.statistics_callback = ->(published) { all_stats << published }

consumers = CONSUMER_COUNT.times.map do
  Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "statistics.interval.ms": 500,
    "statistics.unassigned.include": true
  ).consumer
end

consumers.each { |c| c.subscribe(TOPIC) }

# Poll all consumers until each has reported all partitions in stats.
(120 * 20).times do
  consumers.each do |c|
    c.poll(25)
  rescue Rdkafka::RdkafkaError
    nil
  end

  names_with_all = all_stats
    .select { |s| (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100 }
    .map { |s| s["name"] }
    .uniq

  break if names_with_all.size >= CONSUMER_COUNT
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
latest_by_name = {}
all_stats.each do |s|
  topic_data = (s["topics"][TOPIC] || {}).fetch("partitions", {})
  count = topic_data.keys.count { |k| k != "-1" }
  next if count == 0

  latest_by_name[s["name"]] = {
    partitions: count,
    json_size: JSON.generate(s).bytesize
  }
end

puts
puts "#{CONSUMER_COUNT} consumers, #{PARTITIONS} partitions, filter disabled:"

all_ok = true

latest_by_name.each do |name, data|
  puts "  #{name}: #{data[:partitions]} partitions, #{data[:json_size]} bytes"

  next if data[:partitions] >= PARTITIONS

  puts "  FAIL: Expected #{PARTITIONS} partitions, got #{data[:partitions]}"
  all_ok = false
end

unless latest_by_name.empty?
  total_json_size = latest_by_name.values.sum { |d| d[:json_size] }
  avg_json_size = total_json_size / latest_by_name.size
  puts "  Avg JSON size per consumer: #{avg_json_size} bytes"
end

puts

if latest_by_name.size < CONSUMER_COUNT
  puts "FAIL: Only #{latest_by_name.size}/#{CONSUMER_COUNT} consumers reported partition data"
  exit(1)
elsif all_ok
  puts "PASS: All #{CONSUMER_COUNT} consumers report all #{PARTITIONS} partitions"
  exit(0)
else
  puts "FAIL: Some consumers did not report all partitions"
  exit(1)
end
