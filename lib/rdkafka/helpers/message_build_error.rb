# frozen_string_literal: true

module Rdkafka
  module Helpers
    # Shared message build error handling for Consumer and ShareConsumer.
    module MessageBuildError
      private

      # Rebuilds an error raised while building a message with that message's topic, partition
      # and offset. librdkafka has already returned the message, so the caller needs them to act
      # on it: a consumer seeks back to it, a share consumer acknowledges it.
      # @param native_message [Rdkafka::Bindings::Message] the native message that failed to build
      # @param error [RdkafkaError] the error raised while building it
      # @return [RdkafkaError]
      def message_build_error(native_message, error)
        Rdkafka::RdkafkaError.new(
          error.rdkafka_response,
          error.message_prefix,
          broker_message: error.broker_message,
          details: {
            topic: Rdkafka::Bindings.rd_kafka_topic_name(native_message[:rkt]),
            partition: native_message[:partition],
            offset: native_message[:offset]
          }.freeze,
          instance_name: error.instance_name
        )
      end
    end
  end
end
