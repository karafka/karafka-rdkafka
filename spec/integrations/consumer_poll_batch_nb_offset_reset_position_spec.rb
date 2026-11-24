# frozen_string_literal: true

# This integration test verifies that `Consumer#poll_batch_nb` moves the partition position back
# after an offset reset that goes back, so `#position` is right after the last message it returned.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios:
# 1. A consumer assigned to one partition reads 300 messages with `#poll` (position 300)
# 2. The topic is deleted and created again with 50 messages, so the next fetch resets the offset
#    to the earliest one
# 3. The consumer reads only with `#poll_batch_nb` until it gets offset 49
# 4. `#position` must be 50, not the old 300
#
# Exit codes:
# - 0: the position is right after the last message `#poll_batch_nb` returned, or pending (skipped)
# - 1: messages missing, or the position is not right after the last returned message
#
# Pending: librdkafka sets the position after a batch only when it moves forward, so
# `#poll_batch` and `#poll_batch_nb` leave it at the old offset (300) after this reset.
# https://github.com/confluentinc/librdkafka/blob/v2.16.0/src/rdkafka_queue.c#L828-L839
# (`rd_kafka_q_serve_rkmessages`). Set `RDKAFKA_RUN_PENDING=true` to run it anyway.

unless ENV["RDKAFKA_RUN_PENDING"] == "true"
  puts "SKIP: pending, librdkafka moves the batch poll position only forward " \
    "(set RDKAFKA_RUN_PENDING=true to run)"
  exit(0)
end

require "rdkafka"
require "securerandom"

TOPIC = "it-consumer-poll-batch-nb-offset-reset-position-#{SecureRandom.uuid}"
TIMEOUT = 30

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def topic_visible?(admin)
  admin.metadata(TOPIC).topics.first&.dig(:partitions)&.any?
rescue Rdkafka::RdkafkaError => e
  raise unless e.code == :unknown_topic_or_part

  false
end

def create_topic_with_messages(admin, producer, count)
  admin.create_topic(TOPIC, 1, 1).wait(max_wait_timeout_ms: 15_000)

  deadline = now + TIMEOUT
  until topic_visible?(admin)
    exit(1) if now > deadline

    sleep(0.1)
  end

  count.times { |i| producer.produce(topic: TOPIC, payload: "payload #{i}", partition: 0) }
  producer.flush
end

def wait_for_topic_deleted(admin)
  deadline = now + TIMEOUT
  while topic_visible?(admin)
    exit(1) if now > deadline

    sleep(0.1)
  end
end

def position(consumer)
  consumer.position.to_h.fetch(TOPIC).first.offset
end

config = Rdkafka::Config.new("bootstrap.servers": "localhost:9092")
admin = config.admin
producer = config.producer
consumer = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "group.id": "it-consumer-poll-batch-nb-offset-reset-position-#{SecureRandom.uuid}",
  "auto.offset.reset": "earliest",
  "enable.auto.commit": false
).consumer

begin
  create_topic_with_messages(admin, producer, 300)

  tpl = Rdkafka::Consumer::TopicPartitionList.new
  tpl.add_topic_and_partitions_with_offsets(TOPIC, 0 => 0)
  consumer.assign(tpl)

  last_offset = nil
  deadline = now + TIMEOUT

  while now < deadline && last_offset != 299
    message = consumer.poll(100)
    last_offset = message.offset if message
  end

  exit(1) unless last_offset == 299
  exit(1) unless position(consumer) == 300

  admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
  wait_for_topic_deleted(admin)
  create_topic_with_messages(admin, producer, 50)

  last_offset = nil
  deadline = now + TIMEOUT

  while now < deadline && last_offset != 49
    consumer.poll_batch_nb(100, max_items: 10).each do |item|
      last_offset = item.offset if item.is_a?(Rdkafka::Consumer::Message)
    end
  end

  exit(1) unless last_offset == 49
  exit(1) unless position(consumer) == 50
ensure
  consumer.close

  begin
    admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  producer.close
  admin.close
end
