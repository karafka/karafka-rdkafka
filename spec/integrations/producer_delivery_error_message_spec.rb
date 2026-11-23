# frozen_string_literal: true

# This integration test verifies that a failed delivery carries the error text that librdkafka
# got for the message, not only the error code.
#
# A compacted topic rejects a record without a key with `invalid_record`, and the broker names
# the cause in the Produce response (KIP-467). The text must reach:
#   - `DeliveryReport#broker_message` of the delivery callback report
#   - `DeliveryReport#broker_message` and `#error.broker_message` from
#     `wait(raise_response_error: false)`
#   - `RdkafkaError#broker_message` raised by `DeliveryHandle#wait`
# A delivered message has no text.
#
# In one batch with two key-less records and a keyed one, each key-less record gets
# `invalid_record` with the broker text, and the keyed one gets `invalid_different_record` with
# librdkafka's text for the other records of a rejected batch.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the error text reached the delivery report and the raised error
# - 1: the text was missing or wrong

require "rdkafka"
require "securerandom"

BOOTSTRAP = "localhost:9092"
TOPIC = "it-delivery-error-message-#{SecureRandom.hex(6)}"

def topic_visible?(admin, topic)
  admin.metadata(topic).topics.first&.dig(:partitions)&.any?
rescue Rdkafka::RdkafkaError => e
  raise unless e.code == :unknown_topic_or_part

  false
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1, "cleanup.policy" => "compact").wait(max_wait_timeout_ms: 15_000)

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
until topic_visible?(admin, TOPIC)
  exit(1) if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

  sleep(0.1)
end

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
batch_producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP, "linger.ms": 500).producer
callback_reports = []
producer.delivery_callback = ->(report) { callback_reports << report }

begin
  delivered = producer.produce(topic: TOPIC, payload: "payload", key: "key").wait(max_wait_timeout_ms: 15_000)

  exit(1) unless delivered.error.nil? && delivered.broker_message.nil?

  report = producer
    .produce(topic: TOPIC, payload: "payload")
    .wait(max_wait_timeout_ms: 15_000, raise_response_error: false)

  exit(1) unless report.error.code == :invalid_record
  exit(1) unless report.broker_message.is_a?(String) && report.broker_message.start_with?("Compacted topic cannot accept message without key")
  exit(1) unless report.error.broker_message == report.broker_message

  raised = begin
    producer.produce(topic: TOPIC, payload: "payload").wait(max_wait_timeout_ms: 15_000)
    nil
  rescue Rdkafka::RdkafkaError => e
    e
  end

  exit(1) unless raised&.code == :invalid_record
  exit(1) unless raised.broker_message == report.broker_message

  producer.flush

  failed_callback_reports = callback_reports.reject { |r| r.error.zero? }

  exit(1) unless failed_callback_reports.size == 2
  exit(1) unless failed_callback_reports.all? { |r| r.broker_message == report.broker_message }
  exit(1) unless callback_reports.find { |r| r.error.zero? }&.broker_message.nil?

  batch_reports = [nil, "key", nil]
    .map { |key| batch_producer.produce(topic: TOPIC, payload: "payload", key: key, partition: 0) }
    .map { |handle| handle.wait(max_wait_timeout_ms: 15_000, raise_response_error: false) }

  invalid = batch_reports.values_at(0, 2)
  different = batch_reports[1]

  exit(1) unless invalid.all? { |r| r.error.code == :invalid_record }
  exit(1) unless invalid.all? { |r| r.broker_message&.start_with?("Compacted topic cannot accept message without key") }
  exit(1) unless different.error.code == :invalid_different_record
  exit(1) unless different.broker_message == "Failed to append record because it was part of a batch which had one more more invalid records"
  exit(1) unless different.error.broker_message == different.broker_message
ensure
  producer.close
  batch_producer.close

  begin
    admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end
