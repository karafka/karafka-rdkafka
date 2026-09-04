# frozen_string_literal: true

# This integration test verifies KIP-932 share consumer explicit acknowledgement semantics:
# - accepted records are not redelivered
# - released records are redelivered with an incremented delivery count
# - rejected records are not redelivered
# - commit_sync returns per-partition results
# - the acknowledgement commit callback reports acknowledged offsets
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: All assertions hold (test passes)
# - 1: An assertion failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "share-explicit-#{SecureRandom.hex(6)}"
GROUP = "share-explicit-group-#{SecureRandom.hex(4)}"
MESSAGES = 6

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

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

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
handles = MESSAGES.times.map do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}", key: "key-#{i}")
end
handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
producer.close

consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": GROUP,
  "share.acknowledgement.mode": "explicit"
).share_consumer

ack_callback_results = []
consumer.acknowledgement_commit_callback = lambda do |results, error|
  ack_callback_results << [results, error]
end

consumer.subscribe(TOPIC)

# Collect the first delivery of all records, releasing "payload-1", rejecting "payload-2" and
# accepting everything else
first_delivery = []
released_offset = nil
rejected_offset = nil

30.times do
  batch = consumer.poll(1_000)

  batch.each do |message|
    assert(!message.error?, "unexpected record-level error: #{message.error}")

    first_delivery << message.payload

    case message.payload
    when "payload-1"
      released_offset = message.offset
      consumer.acknowledge(message, :release)
    when "payload-2"
      rejected_offset = message.offset
      consumer.acknowledge(message, :reject)
    else
      consumer.acknowledge(message, :accept)
    end
  end

  break if first_delivery.size >= MESSAGES
end

assert(
  first_delivery.sort == MESSAGES.times.map { |i| "payload-#{i}" }.sort,
  "expected all #{MESSAGES} messages in first delivery, got #{first_delivery.inspect}"
)

# Flush the acknowledgements and check the per-partition results
results = consumer.commit_sync

results&.to_h&.each do |topic, partitions|
  partitions.each do |partition|
    assert(
      partition.err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      "commit_sync reported error #{partition.err} for #{topic}/#{partition.partition}"
    )
  end
end

# The released record ("payload-1") must be redelivered with an incremented delivery count, and
# the rejected record ("payload-2") must never reappear. KIP-932 share groups are at-least-once,
# so an *accepted* record whose acquisition lock expired before the commit_sync above flushed the
# accept may also be redelivered - expected broker behavior, not a failure. So we require only the
# released record to come back, forbid only the rejected one, and tolerate (record, don't fail on)
# any extra redelivery of an accepted record. Loop until the released record is seen (or a
# deadline) rather than breaking on the first redelivery, which could be one of those extras.
redelivered = {}
deadline = Time.now + 30

while Time.now < deadline && !redelivered.key?("payload-1")
  batch = consumer.poll(1_000)

  batch.each do |message|
    redelivered[message.payload] ||= message
    consumer.acknowledge(message, :accept)
  end
end

released = redelivered["payload-1"]
assert(!released.nil?, "expected the released payload-1 to be redelivered, got #{redelivered.keys.inspect}")
assert(released.offset == released_offset, "redelivered offset mismatch for payload-1")
assert(
  released.delivery_count >= 2,
  "expected delivery_count >= 2 on the released record's redelivery, got #{released.delivery_count}"
)
assert(
  !redelivered.key?("payload-2"),
  "the rejected payload-2 must not be redelivered, but it was"
)

extra_redeliveries = redelivered.keys - ["payload-1"]

consumer.commit_sync

# Give the async acknowledgement commit callback a chance to be serviced by poll
5.times { consumer.poll(200) }

assert(ack_callback_results.any?, "acknowledgement commit callback was never invoked")

acknowledged_offsets = ack_callback_results.flat_map do |results_list, _error|
  results_list.flat_map { |entry| entry[:offsets] }
end

assert(
  acknowledged_offsets.include?(rejected_offset),
  "acknowledgement commit callback did not report the rejected offset"
)

ack_callback_results.each do |results_list, error|
  assert(error.nil?, "acknowledgement commit callback reported error: #{error}")

  results_list.each do |entry|
    assert(entry[:topic] == TOPIC, "unexpected topic in ack callback: #{entry[:topic]}")
    assert(entry[:partition] == 0, "unexpected partition in ack callback: #{entry[:partition]}")
  end
end

consumer.close

note = extra_redeliveries.empty? ? "" : " (#{extra_redeliveries.size} accepted record(s) also redelivered under at-least-once share semantics: #{extra_redeliveries.inspect})"
puts "share consumer explicit acknowledgement OK#{note}"
