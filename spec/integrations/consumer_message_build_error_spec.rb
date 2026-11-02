# frozen_string_literal: true

# This integration test verifies that a message that cannot be built (e.g. its headers cannot be
# read) is not lost silently by `Consumer#poll`, `Consumer#poll_nb` and `Consumer#poll_nb_each`.
#
# Once librdkafka returns a message, it has advanced past it and (with the default
# `enable.auto.offset.store`) stored its offset. When the Ruby message cannot be built, the raised
# `RdkafkaError` holds the topic, partition and offset of the message, so the caller can seek back
# to it.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios, for `#poll`, `#poll_nb` and then `#poll_nb_each`, on partition 1 of a 2-partition
# topic:
# 1. Reading the headers of the next message fails once: the raised `RdkafkaError` has the
#    topic, partition and offset of that message in `details`.
# 2. The caller seeks back with these details: the next poll returns the same message.
#
# Exit codes:
# - 0: all methods raised the error with the message details and the message was read again
# - 1: an error without the details, or the message was not read again

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-consumer-message-build-error-#{SecureRandom.uuid}"
TIMEOUT = 30

# Fails the header read of the next message once
module FailingHeaders
  class << self
    attr_accessor :fail_next
  end

  self.fail_next = false

  def from_native(*)
    if FailingHeaders.fail_next
      FailingHeaders.fail_next = false
      raise Rdkafka::RdkafkaError.new(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, "Error reading message headers")
    end

    super
  end
end

Rdkafka::Consumer::Headers.singleton_class.prepend(FailingHeaders)

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 2, 1).wait

# The topic must be in the metadata before we produce to it
deadline = now + TIMEOUT
begin
  admin.metadata(TOPIC)
rescue Rdkafka::RdkafkaError
  raise if now > deadline

  sleep(0.1)
  retry
end

admin.close

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
reports = Array.new(3) { |i| producer.produce(topic: TOPIC, payload: "payload #{i}", partition: 1).wait }
producer.close

consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "it-consumer-message-build-error-#{SecureRandom.uuid}",
  "auto.offset.reset": "earliest"
).consumer

def poll_once(consumer, poll_method)
  return consumer.public_send(poll_method, 100) unless poll_method == :poll_nb_each

  message = nil

  consumer.poll_nb_each do |polled|
    message = polled
    :stop
  end

  message
end

begin
  consumer.subscribe(TOPIC)

  [:poll, :poll_nb, :poll_nb_each].zip(reports).each do |poll_method, report|
    FailingHeaders.fail_next = true

    error = nil
    deadline = now + TIMEOUT

    while error.nil? && now < deadline
      begin
        poll_once(consumer, poll_method)
      rescue Rdkafka::RdkafkaError => e
        error = e
      end

      sleep(0.1) unless error
    end

    exit(1) unless error
    exit(1) unless error.details == { topic: TOPIC, partition: 1, offset: report.offset }

    consumer.seek_by(error.details[:topic], error.details[:partition], error.details[:offset])

    message = nil
    deadline = now + TIMEOUT

    while message.nil? && now < deadline
      message = poll_once(consumer, poll_method)
      sleep(0.1) unless message
    end

    exit(1) unless message&.offset == report.offset
  end
ensure
  consumer.close
end
