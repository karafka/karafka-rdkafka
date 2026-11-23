# frozen_string_literal: true

module Rdkafka
  class Producer
    # Delivery report for a message that failed to be delivered.
    #
    # A separate class keeps the error text off successful reports, so they stay small.
    class FailedDeliveryReport < DeliveryReport
      # @return [String, nil] error text of the failed delivery, `nil` when librdkafka has none
      attr_reader :broker_message

      # @private
      # @param partition [Integer] partition number
      # @param offset [Integer] message offset
      # @param topic_name [String, nil] topic name
      # @param error [Integer, RdkafkaError] error code or error
      # @param label [Object, nil] user-defined label
      # @param status [Integer, nil] `Bindings::RD_KAFKA_MSG_STATUS_*` persistence status
      # @param latency [Integer, nil] produce latency in microseconds, negative when not available
      # @param broker_id [Integer, nil] broker id, negative when not known
      # @param broker_message [String, nil] error text of the failed delivery
      def initialize(
        partition,
        offset,
        topic_name,
        error,
        label = nil,
        status: nil,
        latency: nil,
        broker_id: nil,
        broker_message: nil
      )
        super(partition, offset, topic_name, error, label, status: status, latency: latency, broker_id: broker_id)
        @broker_message = broker_message
      end
    end
  end
end
