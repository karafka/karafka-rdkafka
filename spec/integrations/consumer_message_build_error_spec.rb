# frozen_string_literal: true

# This integration test verifies that a message that cannot be built (its headers are corrupted)
# is not lost silently by `Consumer#poll`, `Consumer#poll_nb` and `Consumer#poll_nb_each`.
#
# Once librdkafka returns a message, it has advanced past it and (with the default
# `enable.auto.offset.store`) stored its offset. When the Ruby message cannot be built, the raised
# `RdkafkaError` holds the topic, partition and offset of the message, so the caller can seek back
# to it.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. Partition 1 of a 2-partition topic
# holds two valid messages and then a message whose headers cannot be parsed.
#
# Scenarios, for `#poll`, `#poll_nb` and then `#poll_nb_each`:
# 1. Polling reaches the corrupted message: the raised `RdkafkaError` has the topic, partition and
#    offset of that message in `details`.
# 2. The caller seeks back with these details: the next poll reads the same message and raises the
#    same error again.
#
# Exit codes:
# - 0: all methods raised the error with the message details, also after seeking back
# - 1: an error without the details, or the message was not read again

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

TOPIC = "it-consumer-message-build-error-#{SecureRandom.uuid}"
PARTITION = 1
TIMEOUT = 30

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

# Zigzag varint, as in Kafka records
def varint(value)
  value = (value << 1) ^ (value >> 63)
  bytes = []

  while value >= 0x80
    bytes << ((value & 0x7f) | 0x80)
    value >>= 7
  end

  (bytes << value).pack("C*")
end

# A record batch with one record whose headers claim one header with a 100 bytes long key but
# hold only one byte of it. The CRC stays 0: consumers check it only with `check.crcs`.
def corrupted_record_batch
  payload = "corrupted"
  headers = varint(1) + varint(100) + "k"
  record = [0].pack("c") + varint(0) + varint(0) + varint(-1) + varint(payload.bytesize) +
    payload + headers
  records = varint(record.bytesize) + record

  timestamp = (Time.now.to_f * 1_000).to_i
  # Attributes, last offset delta, first and max timestamp, producer id and epoch, base sequence
  # and record count
  after_crc = [0, 0, timestamp, timestamp, -1, -1, -1, 1].pack("s>l>q>q>q>s>l>l>") + records
  # Partition leader epoch, magic and CRC
  after_length = [-1, 2, 0].pack("l>cL>") + after_crc

  [0, after_length.bytesize].pack("q>l>") + after_length
end

def poll_once(consumer, poll_method)
  return consumer.public_send(poll_method, 100) unless poll_method == :poll_nb_each

  message = nil

  consumer.poll_nb_each do |polled|
    message = polled
    :stop
  end

  message
end

def wait_for_build_error(consumer, poll_method)
  deadline = now + TIMEOUT

  while now < deadline
    begin
      poll_once(consumer, poll_method)
    rescue Rdkafka::RdkafkaError => e
      return e
    end

    sleep(0.1)
  end

  nil
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 2)

  producer = cluster.config.producer
  2.times { |i| producer.produce(topic: TOPIC, payload: "payload #{i}", partition: PARTITION).wait }
  producer.close

  offset = cluster.produce_record_batch(TOPIC, PARTITION, corrupted_record_batch)
  details = { topic: TOPIC, partition: PARTITION, offset: offset }

  consumer = cluster.config(
    "group.id": "it-consumer-message-build-error-#{SecureRandom.uuid}",
    "auto.offset.reset": "earliest"
  ).consumer

  begin
    consumer.subscribe(TOPIC)

    [:poll, :poll_nb, :poll_nb_each].each do |poll_method|
      error = wait_for_build_error(consumer, poll_method)

      exit(1) unless error&.details == details

      consumer.seek_by(error.details[:topic], error.details[:partition], error.details[:offset])

      exit(1) unless wait_for_build_error(consumer, poll_method)&.details == details

      consumer.seek_by(TOPIC, PARTITION, offset)
    end
  ensure
    consumer.close
  end
end
