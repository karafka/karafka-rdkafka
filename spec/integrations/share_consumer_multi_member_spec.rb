# frozen_string_literal: true

# This integration test verifies that two members of the same share group cooperatively consume
# from the same single-partition topic (queue semantics: both members receive records from the
# one partition, which a regular consumer group cannot do) and that in implicit mode every
# record is consumed exactly once across the group.
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: All records consumed exactly once across both members (test passes)
# - 1: Missing or duplicated records

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "share-multi-#{SecureRandom.hex(6)}"
GROUP = "share-multi-group-#{SecureRandom.hex(4)}"
MESSAGES = 200

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait(max_wait_timeout_ms: 15_000)
# share.auto.offset.reset is a broker-side group config defaulting to latest; set it to
# earliest before the group first attaches so the pre-produced records are delivered
admin.incremental_alter_configs(
  [
    {
      resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_GROUP,
      resource_name: GROUP,
      configs: [{name: "share.auto.offset.reset", value: "earliest", op_type: 0}]
    }
  ]
).wait(max_wait_timeout_ms: 15_000)
admin.close

# Pre-initialize the share group state with a short-lived member before the concurrent
# members join. Two members racing the very first attach of a share group can each observe an
# independent first delivery of the same records (delivery count 1 on both) with the current
# broker preview, which is not what this test is about.
bootstrap_member = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": GROUP
).share_consumer
bootstrap_member.subscribe(TOPIC)
5.times { bootstrap_member.poll(500) }
bootstrap_member.close

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
handles = MESSAGES.times.map do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}")
end
handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
producer.close

# The share consumer is single-threaded by design: one consumer per thread
consumed = Array.new(2) { [] }
consumed_mutex = Mutex.new
total = 0
# Joining a fresh share group (and the auto-created share coordinator state topic) can take a
# while, so run against a deadline instead of counting idle polls
deadline = Time.now + 60

threads = 2.times.map do |i|
  Thread.new do
    consumer = Rdkafka::Config.new(
      "bootstrap.servers": BOOTSTRAP,
      "group.id": GROUP,
      # Small batches so both members get a share of the records
      "max.poll.records": 10
    ).share_consumer

    consumer.subscribe(TOPIC)

    while Time.now < deadline && consumed_mutex.synchronize { total } < MESSAGES
      batch = consumer.poll(500)

      consumed_mutex.synchronize do
        batch.each { |message| consumed[i] << [message.payload, message.delivery_count] }
        total += batch.size
      end
    end

    consumer.close
  end
end

threads.each(&:join)

all = consumed.flat_map { |records| records.map(&:first) }
expected = MESSAGES.times.map { |i| "payload-#{i}" }

if all.sort != expected.sort
  missing = expected - all
  duplicated = all.tally.select { |_, count| count > 1 }
  puts "FAILED: expected each record exactly once across the group"
  puts "  member 0: #{consumed[0].size} records, member 1: #{consumed[1].size} records"
  puts "  member 0 delivery counts: #{consumed[0].map(&:last).tally}"
  puts "  member 1 delivery counts: #{consumed[1].map(&:last).tally}"
  puts "  missing: #{missing.first(5).inspect} (#{missing.size} total)"
  puts "  duplicated: #{duplicated.keys.first(5).inspect} (#{duplicated.size} total)"
  exit 1
end

puts "share consumer multi member OK " \
  "(member 0: #{consumed[0].size} records, member 1: #{consumed[1].size} records)"
