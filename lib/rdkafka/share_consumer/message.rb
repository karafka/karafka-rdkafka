# frozen_string_literal: true

module Rdkafka
  class ShareConsumer
    # A message that was consumed from a topic through a share group.
    #
    # On top of the regular consumer message data it carries the delivery count maintained by
    # the broker and, for record-level errors delivered inside a batch, the error itself.
    class Message < Rdkafka::Consumer::Message
      # Number of times the broker has delivered this record (1 for the first delivery). A
      # record released explicitly or via an expired acquisition lock is redelivered with an
      # incremented delivery count until the broker's delivery count limit archives it.
      # @return [Integer]
      attr_reader :delivery_count

      # Record-level error for this batch entry, if any. Topic, partition and offset are valid
      # on error entries while the payload is not a consumed record. Error entries do not need
      # to be acknowledged.
      # @return [RdkafkaError, nil]
      attr_reader :error

      # @private
      # @param native_message [Rdkafka::Bindings::Message] native message struct from librdkafka
      # @param delivery_count [Integer] broker-maintained delivery count
      # @param error [RdkafkaError, nil] record-level error
      def initialize(native_message, delivery_count:, error: nil)
        super(native_message)

        @delivery_count = delivery_count
        @error = error
      end

      # Whether this batch entry is a record-level error rather than a consumed record
      # @return [Boolean]
      def error?
        !@error.nil?
      end
    end
  end
end
