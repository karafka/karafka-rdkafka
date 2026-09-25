# frozen_string_literal: true

# This integration test verifies that two members of the same share group cooperatively consume
# from the same single-partition topic (queue semantics: both members receive records from the
# one partition, which a regular consumer group cannot do) and that every record is delivered to
# at least one member. Share groups are documented (KIP-932; librdkafka rdkafka.h's "Share
# groups" section) as at-least-once: a record whose acquisition lock expires before it is
# acknowledged - e.g. because a member is slow to issue its next poll under load - becomes
# available again and may be redelivered, possibly to a different member with an incremented
# delivery_count. That is expected broker behavior, not a client bug, so this test tolerates
# duplicates and only fails on a record that is never delivered at all.
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: Every record was delivered to at least one member (test passes)
# - 1: A record was never delivered

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
      configs: [{ name: "share.auto.offset.reset", value: "earliest", op_type: 0 }]
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
seen_payloads = Set.new
consumed_mutex = Mutex.new
# Joining a fresh share group (and the auto-created share coordinator state topic) can take a
# while, so run against a deadline instead of counting idle polls. Track distinct payloads
# rather than a gross delivery count so an at-least-once redelivery (see the file header) cannot
# end the loop before every record has actually been seen at least once.
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

    while Time.now < deadline && consumed_mutex.synchronize { seen_payloads.size } < MESSAGES
      batch = consumer.poll(500)

      consumed_mutex.synchronize do
        batch.each do |message|
          consumed[i] << [message.payload, message.delivery_count]
          seen_payloads << message.payload
        end
      end
    end

    consumer.close
  end
end

threads.each(&:join)

all = consumed.flat_map { |records| records.map(&:first) }
expected = MESSAGES.times.map { |i| "payload-#{i}" }
missing = expected - all

if missing.any?
  duplicated = all.tally.select { |_, count| count > 1 }
  puts "FAILED: expected every record to be delivered at least once"
  puts "  member 0: #{consumed[0].size} records, member 1: #{consumed[1].size} records"
  puts "  member 0 delivery counts: #{consumed[0].map(&:last).tally}"
  puts "  member 1 delivery counts: #{consumed[1].map(&:last).tally}"
  puts "  missing: #{missing.first(5).inspect} (#{missing.size} total)"
  puts "  duplicated: #{duplicated.keys.first(5).inspect} (#{duplicated.size} total)"
  exit 1
end

redelivered = all.tally.count { |_, count| count > 1 }
note = redelivered.positive? ? " (#{redelivered} record(s) redelivered under at-least-once share semantics)" : ""

puts "share consumer multi member OK " \
  "(member 0: #{consumed[0].size} records, member 1: #{consumed[1].size} records)#{note}"
