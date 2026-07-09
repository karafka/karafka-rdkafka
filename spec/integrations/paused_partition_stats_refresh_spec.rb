# frozen_string_literal: true

# This integration test verifies the `statistics.paused.refresh.ms` librdkafka patch
# (dist/patches/rdkafka_paused_partition_stats_refresh.patch).
#
# Background: librdkafka only updates a partition's hi_offset/ls_offset (and therefore
# consumer_lag) when it receives a Fetch response. A paused partition receives no Fetch
# responses, so those statistics freeze at their pre-pause values for as long as the
# pause lasts (see confluentinc/librdkafka#4996). The patch adds an opt-in, hidden
# config property (0 = disabled, the default) that piggybacks on librdkafka's existing
# per-partition low-watermark timer to probe the broker for the current watermark while
# app-paused, no more often than the configured interval, so consumer_lag keeps
# advancing instead of appearing to have caught up.
#
# We assert on `consumer_lag` rather than the raw `hi_offset` field: librdkafka's
# consumer default is `isolation.level: read_committed`, under which the patch (by
# design - see the isolation-level handling in rd_kafka_toppar_handle_Offset_wmark)
# only refreshes `ls_offset`, not `hi_offset`, to avoid understating the true
# watermark while a transaction is open. `consumer_lag` is computed from whichever
# of the two the configured isolation level actually uses, so it is the correct,
# isolation-level-agnostic signal - and it's also the statistic the original bug
# report is actually about.
#
# This test proves three things:
# 1. With the interval enabled, consumer_lag for a paused partition catches up to
#    messages produced *after* the pause, without the consumer ever fetching them.
# 2. With the property left at its default (0/off), behavior is unchanged from
#    stock librdkafka: consumer_lag stays near its pre-pause value for the same
#    wait window (regression guard + proof the setting actually gates the new
#    behavior).
# 3. A consumer can be paused and closed immediately without hanging or crashing
#    (guards the __DESTROY / rktp_ops-purge handling the patch relies on).
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: All three checks pass
# - 1: Any check fails or times out

require "rdkafka"
require "securerandom"
require "json"
require "timeout"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
INITIAL_MESSAGES = 5
ADDITIONAL_MESSAGES = 20
# The probe piggybacks on librdkafka's existing per-partition low-watermark timer,
# which fires no more often than every 10s regardless of statistics.interval.ms,
# and whose phase is set from toppar creation time, not from when we pause - so
# a window has to comfortably exceed 10s to be sure of catching a firing
# regardless of where in its cycle it happened to be when we started waiting.
WAIT_SECONDS = 40

def create_topic(admin, topic)
  admin.create_topic(topic, 1, 1).wait(max_wait_timeout_ms: 15_000)

  10.times do
    admin.metadata(topic)
    break
  rescue Rdkafka::RdkafkaError
    sleep 0.5
  end
end

def produce_messages(count, topic)
  producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
  count.times { |i| producer.produce(topic: topic, payload: "msg-#{i}").wait }
ensure
  producer.close
end

def consumer_lag(stats, topic)
  partitions = (stats["topics"][topic] || {}).fetch("partitions", {})
  partition = partitions["0"]
  partition && partition["consumer_lag"]
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin

# --- Check 1: refresh interval set -> consumer_lag advances while paused ---
enabled_topic = "paused-stats-refresh-on-#{SecureRandom.hex(6)}"
create_topic(admin, enabled_topic)
produce_messages(INITIAL_MESSAGES, enabled_topic)

enabled_stats = []
Rdkafka::Config.statistics_callback = ->(published) { enabled_stats << published }

enabled_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "paused-stats-refresh-on-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 200,
  # Set well below the underlying timer's ~10s floor to prove this is a
  # configurable rate limit, not a hardcoded interval - actual cadence is
  # still capped at ~10s regardless (see WAIT_SECONDS comment above).
  "statistics.paused.refresh.ms": 5_000
).consumer

enabled_consumer.subscribe(enabled_topic)

received = 0
(20 * 20).times do
  break if received >= INITIAL_MESSAGES
  begin
    msg = enabled_consumer.poll(50)
    received += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

tpl = Rdkafka::Consumer::TopicPartitionList.new
tpl.add_topic(enabled_topic, 0..0)
enabled_consumer.pause(tpl)

# rd_kafka_pause_partitions() is documented as asynchronous: it enqueues the
# pause and returns immediately, the actual pause is applied on librdkafka's
# internal toppar handler thread shortly after. Give it a moment to land
# before producing more messages, so we don't race an already in-flight
# Fetch request/response against the pause taking effect.
settle_deadline = Time.now + 1.5
until Time.now > settle_deadline
  begin
    msg = enabled_consumer.poll(100)
    received += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

produce_messages(ADDITIONAL_MESSAGES, enabled_topic)

deadline = Time.now + WAIT_SECONDS
until Time.now > deadline
  break if enabled_stats.any? { |s| (consumer_lag(s, enabled_topic) || 0) >= ADDITIONAL_MESSAGES }
  begin
    msg = enabled_consumer.poll(100)
    received += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

enabled_consumer.resume(tpl)

(20 * 20).times do
  break if received >= INITIAL_MESSAGES + ADDITIONAL_MESSAGES
  begin
    msg = enabled_consumer.poll(50)
    received += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

enabled_consumer.close
Rdkafka::Config.statistics_callback = nil

enabled_max_lag = enabled_stats.filter_map { |s| consumer_lag(s, enabled_topic) }.max

# --- Check 2: refresh interval left at default (0/off) -> consumer_lag stays frozen while paused ---
disabled_topic = "paused-stats-refresh-off-#{SecureRandom.hex(6)}"
create_topic(admin, disabled_topic)
produce_messages(INITIAL_MESSAGES, disabled_topic)

disabled_stats = []
Rdkafka::Config.statistics_callback = ->(published) { disabled_stats << published }

disabled_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "paused-stats-refresh-off-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 200
  # statistics.paused.refresh.ms intentionally omitted: defaults to 0/off
).consumer

disabled_consumer.subscribe(disabled_topic)

received_disabled = 0
(20 * 20).times do
  break if received_disabled >= INITIAL_MESSAGES
  begin
    msg = disabled_consumer.poll(50)
    received_disabled += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

disabled_tpl = Rdkafka::Consumer::TopicPartitionList.new
disabled_tpl.add_topic(disabled_topic, 0..0)
disabled_consumer.pause(disabled_tpl)

# See the comment on the equivalent settle loop above: let the async pause
# actually land before producing more messages and recording our baseline.
settle_deadline = Time.now + 1.5
until Time.now > settle_deadline
  begin
    msg = disabled_consumer.poll(100)
    received_disabled += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

baseline_lag = disabled_stats.filter_map { |s| consumer_lag(s, disabled_topic) }.max || 0

produce_messages(ADDITIONAL_MESSAGES, disabled_topic)

pre_wait_stats_count = disabled_stats.size
deadline = Time.now + WAIT_SECONDS
until Time.now > deadline
  begin
    msg = disabled_consumer.poll(100)
    received_disabled += 1 if msg
  rescue Rdkafka::RdkafkaError
    nil
  end
end

disabled_consumer.resume(disabled_tpl)
disabled_consumer.close
Rdkafka::Config.statistics_callback = nil

disabled_post_pause_lags = disabled_stats[pre_wait_stats_count..-1].to_a.filter_map { |s| consumer_lag(s, disabled_topic) }
disabled_max_lag = (disabled_stats.filter_map { |s| consumer_lag(s, disabled_topic) }.max || 0)
# One straggling Fetch response already in flight when the (asynchronous) pause
# landed can legitimately bump the lag by a message or two - that's stock
# librdkafka behavior, not the feature under test. What must NOT happen with
# the interval left at 0 is consumer_lag tracking the new production the way check 1 does.
disabled_slack = 2

# --- Check 3: pause immediately followed by close must not hang or crash ---
shutdown_topic = "paused-stats-refresh-shutdown-#{SecureRandom.hex(6)}"
create_topic(admin, shutdown_topic)
produce_messages(INITIAL_MESSAGES, shutdown_topic)

shutdown_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "paused-stats-refresh-shutdown-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 100,
  "statistics.paused.refresh.ms": 5_000
).consumer

shutdown_consumer.subscribe(shutdown_topic)

(20 * 20).times do
  break if shutdown_consumer.assignment.count > 0
  shutdown_consumer.poll(50)
rescue Rdkafka::RdkafkaError
  nil
end

shutdown_tpl = Rdkafka::Consumer::TopicPartitionList.new
shutdown_tpl.add_topic(shutdown_topic, 0..0)
shutdown_consumer.pause(shutdown_tpl)

shutdown_ok = begin
  Timeout.timeout(15) { shutdown_consumer.close }
  true
rescue Timeout::Error
  false
end

# --- Cleanup ---
[enabled_topic, disabled_topic, shutdown_topic].each do |topic|
  admin.delete_topic(topic).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
end
admin.close

# --- Results ---
puts
puts "Paused-partition statistics refresh:"
puts "  [interval set] max consumer_lag observed while paused:  #{enabled_max_lag.inspect} (want >= #{ADDITIONAL_MESSAGES})"
puts "  [default 0]    max consumer_lag observed while paused:  #{disabled_max_lag.inspect} (baseline #{baseline_lag}, want <= #{baseline_lag + disabled_slack})"
puts "  [shutdown] pause immediately followed by close:     #{shutdown_ok ? 'completed' : 'HUNG'}"
puts

failures = []

if enabled_max_lag.nil? || enabled_max_lag < ADDITIONAL_MESSAGES
  failures << "interval-enabled consumer never observed consumer_lag >= #{ADDITIONAL_MESSAGES} while paused"
end

if disabled_post_pause_lags.any? { |lag| lag > baseline_lag + disabled_slack }
  failures << "default (interval=0) consumer's consumer_lag tracked new production while paused - default behavior changed"
end

unless shutdown_ok
  failures << "closing a paused consumer with the refresh interval enabled hung (__DESTROY handling regression)"
end

if failures.empty?
  puts "PASS: paused-partition consumer_lag refresh works as expected, default is unchanged, shutdown is safe"
  exit(0)
else
  failures.each { |f| puts "FAIL: #{f}" }
  exit(1)
end
