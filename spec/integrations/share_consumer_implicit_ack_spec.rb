# frozen_string_literal: true

# This integration test verifies KIP-932 share consumer implicit acknowledgement semantics (the
# default mode, share.acknowledgement.mode=implicit):
# - every record delivered by a poll is acknowledged as accepted by the following poll/commit,
#   with no per-record #acknowledge call
# - once accepted, records are not redelivered: a second member joining the same share group
#   afterwards receives nothing
# - the delivered records carry delivery_count 1, real payloads/keys and a timestamp
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: Implicit acknowledgement behaves as expected (test passes)
# - 1: An assertion failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "share-implicit-#{SecureRandom.hex(6)}"
GROUP = "share-implicit-group-#{SecureRandom.hex(4)}"
MESSAGES = 10

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait(max_wait_timeout_ms: 15_000)
# share.auto.offset.reset is a broker-side group config defaulting to latest; set it to earliest
# before the group first attaches so the pre-produced records are delivered
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

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
handles = MESSAGES.times.map do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}", key: "key-#{i}")
end
handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
producer.close

# First member: implicit mode is the default, so no share.acknowledgement.mode is set and no
# #acknowledge is called - polling alone accepts the previously delivered batch.
consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": GROUP
).share_consumer

consumer.subscribe(TOPIC)

# Track the first delivery of each distinct payload. KIP-932 share groups are at-least-once: a
# record whose acquisition lock expires before the implicit accept on the next poll becomes
# available again and may be redelivered (with an incremented delivery_count). Keying on the
# payload means a redelivery cannot end the loop before every distinct record has been seen, nor
# fail the assertions below - it is only counted as an informational redelivery.
first_delivery = {}
redeliveries = 0

30.times do
  batch = consumer.poll(1_000)

  batch.each do |message|
    assert(message.is_a?(Rdkafka::ShareConsumer::Message), "expected ShareConsumer::Message, got #{message.class}")
    assert(!message.error?, "unexpected record-level error: #{message.error}")

    if first_delivery.key?(message.payload)
      redeliveries += 1
    else
      first_delivery[message.payload] = message
    end
  end

  break if first_delivery.size >= MESSAGES
end

assert(
  first_delivery.size == MESSAGES,
  "expected #{MESSAGES} distinct records on first delivery, got #{first_delivery.size}"
)
assert(
  first_delivery.keys.sort == MESSAGES.times.map { |i| "payload-#{i}" }.sort,
  "payload mismatch: #{first_delivery.keys.sort.inspect}"
)
assert(
  first_delivery.values.map(&:key).sort == MESSAGES.times.map { |i| "key-#{i}" }.sort,
  "key mismatch: #{first_delivery.values.map(&:key).sort.inspect}"
)
# The first delivery of a record in a brand-new group is always delivery_count 1 (there is no
# prior member that could have released it), independent of any later redelivery.
assert(
  first_delivery.values.all? { |m| m.delivery_count == 1 },
  "expected delivery_count 1 on first delivery, got #{first_delivery.values.map(&:delivery_count).tally.inspect}"
)
assert(first_delivery.values.all? { |m| m.timestamp.is_a?(Time) }, "expected every record to carry a Time timestamp")

# Flush the implicit accept of the last delivered batch to the broker, then close so this member
# leaves the group having acknowledged everything.
consumer.commit_sync
consumer.close

# Second member of the same group: everything the first member consumed was implicitly accepted,
# so nothing must be redelivered here. Poll for a few seconds to give the broker time to (not)
# deliver anything.
verifier = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": GROUP
).share_consumer

verifier.subscribe(TOPIC)

redelivered = []
10.times do
  verifier.poll(500).each do |message|
    assert(message.is_a?(Rdkafka::ShareConsumer::Message), "expected ShareConsumer::Message, got #{message.class}")
    redelivered << message.payload
  end
end

verifier.close

assert(
  redelivered.empty?,
  "expected no redelivery after implicit accept, got #{redelivered.inspect}"
)

note = redeliveries.positive? ? " (#{redeliveries} redelivery/-ies observed within the first member under at-least-once share semantics)" : ""
puts "share consumer implicit acknowledgement OK " \
  "(#{first_delivery.size} distinct records accepted, none redelivered to a second member)#{note}"
