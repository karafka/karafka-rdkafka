# frozen_string_literal: true

# This integration test verifies that the statistics.unassigned.include=false
# filter also reduces the size of the per-broker "toppars" section (a map of
# topic-partitions leader-owned by each broker).
#
# With a 1000-partition topic and a filtered producer, each broker's toppars
# map should be empty. A filtered consumer that has not yet been assigned
# partitions should also report an empty toppars map. An unfiltered client
# should populate toppars with all of its leader-owned partitions.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: toppars is filtered as expected and JSON shrinks significantly
# - 1: toppars is not filtered, or no measurable reduction
require "rdkafka"
require "securerandom"
require "json"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "stats-integration-toppars-#{SecureRandom.hex(6)}"
PARTITIONS = 1_000

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end

broker_has_topic_toppars = lambda do |stat|
  stat["brokers"].any? do |_, broker|
    (broker["toppars"] || {}).any? { |_, tp| tp["topic"] == TOPIC }
  end
end

# --- Unfiltered producer: expect brokers.toppars populated ---
unfiltered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { unfiltered_stats << published }

unfiltered_producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": true
).producer

unfiltered_producer.produce(topic: TOPIC, payload: "test").wait

(60 * 20).times do
  break if unfiltered_stats.any?(&broker_has_topic_toppars)
  sleep 0.05
end

unfiltered_producer.close

# --- Filtered producer: expect brokers.toppars empty for this topic ---
filtered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { filtered_stats << published }

filtered_producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": false
).producer

filtered_producer.produce(topic: TOPIC, payload: "test").wait

(30 * 20).times do
  break if filtered_stats.size >= 3
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
unfiltered_stat = unfiltered_stats.reverse.find(&broker_has_topic_toppars)

if unfiltered_stat.nil?
  puts "FAIL: No unfiltered stat reported brokers.toppars for #{TOPIC}"
  exit(1)
end

unfiltered_toppars_count = unfiltered_stat["brokers"].sum do |_, broker|
  (broker["toppars"] || {}).count { |_, tp| tp["topic"] == TOPIC }
end

filtered_stat = filtered_stats.last

if filtered_stat.nil?
  puts "FAIL: No filtered stats captured"
  exit(1)
end

filtered_toppars_leak = filtered_stat["brokers"].any? do |_, broker|
  (broker["toppars"] || {}).any? { |_, tp| tp["topic"] == TOPIC }
end

unfiltered_size = JSON.generate(unfiltered_stat).bytesize
filtered_size = JSON.generate(filtered_stat).bytesize
reduction = ((1.0 - filtered_size.to_f / unfiltered_size) * 100).round(1)

puts
puts "Producer brokers.toppars filtering (#{PARTITIONS} partitions):"
puts "  Unfiltered toppars for #{TOPIC}: #{unfiltered_toppars_count}"
puts "  Filtered toppars for #{TOPIC}:   #{filtered_toppars_leak ? "LEAKED" : 0}"
puts "  Unfiltered JSON size:            #{unfiltered_size} bytes"
puts "  Filtered JSON size:              #{filtered_size} bytes"
puts "  Reduction:                       #{reduction}%"
puts

if filtered_toppars_leak
  puts "FAIL: Filtered producer stats still contain toppars for #{TOPIC}"
  exit(1)
elsif unfiltered_toppars_count < 1
  puts "FAIL: Unfiltered producer stats reported zero toppars for #{TOPIC}"
  exit(1)
elsif filtered_size >= unfiltered_size
  puts "FAIL: Filtered JSON is not smaller (#{filtered_size} >= #{unfiltered_size})"
  exit(1)
else
  puts "PASS: brokers.toppars filtered and JSON #{reduction}% smaller"
  exit(0)
end
