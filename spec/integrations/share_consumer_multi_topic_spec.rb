# frozen_string_literal: true

# This integration test verifies that a single KIP-932 share consumer subscribed to more than one
# topic at once receives the records produced to every subscribed topic (a share subscription
# takes a list of topics, mirroring the regular consumer), and that records are attributed to the
# right topic.
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: Records from every subscribed topic were delivered (test passes)
# - 1: An assertion failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
SUFFIX = SecureRandom.hex(6)
TOPIC_A = "share-multi-topic-a-#{SUFFIX}"
TOPIC_B = "share-multi-topic-b-#{SUFFIX}"
GROUP = "share-multi-topic-group-#{SecureRandom.hex(4)}"
PER_TOPIC = 8

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
[TOPIC_A, TOPIC_B].each do |topic|
  admin.create_topic(topic, 1, 1).wait(max_wait_timeout_ms: 15_000)
end
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
handles = [TOPIC_A, TOPIC_B].flat_map do |topic|
  PER_TOPIC.times.map do |i|
    producer.produce(topic: topic, payload: "#{topic}-payload-#{i}", key: "key-#{i}")
  end
end
handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
producer.close

consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": GROUP
).share_consumer

# A single subscribe call listing both topics
consumer.subscribe(TOPIC_A, TOPIC_B)

subscribed = consumer.subscription.to_h.keys
assert(
  subscribed.sort == [TOPIC_A, TOPIC_B].sort,
  "expected subscription to both topics, got #{subscribed.inspect}"
)

total = PER_TOPIC * 2

# Track the first delivery of each distinct payload. Share groups are at-least-once (a record can
# be redelivered if its acquisition lock expires before it is accepted), so break on the number of
# distinct payloads seen rather than a gross received count that a redelivery could inflate before
# every record has actually arrived.
first_delivery = {}
redeliveries = 0

40.times do
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

  break if first_delivery.size >= total
end

consumer.close

by_topic = first_delivery.values.group_by(&:topic)

[TOPIC_A, TOPIC_B].each do |topic|
  payloads = (by_topic[topic] || []).map(&:payload).sort
  expected = PER_TOPIC.times.map { |i| "#{topic}-payload-#{i}" }.sort

  assert(
    payloads == expected,
    "expected all #{PER_TOPIC} distinct records from #{topic}, got #{payloads.inspect}"
  )
end

note = redeliveries.positive? ? " (#{redeliveries} redelivery/-ies observed under at-least-once share semantics)" : ""
puts "share consumer multi topic OK " \
  "(topic a: #{by_topic[TOPIC_A]&.size || 0} records, topic b: #{by_topic[TOPIC_B]&.size || 0} records)#{note}"
