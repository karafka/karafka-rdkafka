# frozen_string_literal: true

module Rdkafka
  # Testing utilities for Producer and Consumer instances.
  # This module is NOT included by default and should only be used in test environments.
  #
  # This module provides librdkafka native testing utilities that are needed to trigger certain
  # behaviours that are hard to reproduce in stable environments.
  #
  # To use in tests for producers:
  #   producer.singleton_class.include(Rdkafka::Testing)
  #
  # To use in tests for consumers:
  #   consumer.singleton_class.include(Rdkafka::Testing)
  #
  # IMPORTANT: After triggering a fatal error, you MUST call mark_for_cleanup to prevent
  # segfaults during garbage collection. Fatal errors leave the client in an unusable state,
  # and attempting to close it (either explicitly or via finalizer) will hang or crash.
  module Testing
    # Triggers a test fatal error using rd_kafka_test_fatal_error.
    # This is useful for testing fatal error handling without needing actual broker issues.
    #
    # @param error_code [Integer] The error code to trigger (e.g., 47 for invalid_producer_epoch)
    # @param reason [String] Descriptive reason for the error
    # @return [Integer] Result code from rd_kafka_test_fatal_error (0 on success)
    #
    # @example
    #   producer.trigger_test_fatal_error(47, "Test producer fencing")
    def trigger_test_fatal_error(error_code, reason)
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_test_fatal_error(inner, error_code, reason)
      end
    end

    # Checks if a fatal error has occurred and retrieves error details.
    # Calls rd_kafka_fatal_error to get the actual fatal error code and message.
    #
    # @return [Hash, nil] Hash with :error_code and :error_string if fatal error occurred, nil otherwise
    #
    # @example
    #   if fatal_error = producer.fatal_error
    #     puts "Fatal error #{fatal_error[:error_code]}: #{fatal_error[:error_string]}"
    #   end
    def fatal_error
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.extract_fatal_error(inner)
      end
    end

    # Marks this producer/consumer for cleanup without calling close.
    # This MUST be called after triggering a fatal error to prevent segfaults during GC.
    #
    # After a fatal error is triggered via trigger_test_fatal_error, the librdkafka client
    # is in an unrecoverable state. Calling close() or allowing the finalizer to run will
    # cause rd_kafka_destroy() to hang indefinitely or segfault.
    #
    # This method:
    # 1. Undefines the finalizer to prevent GC from trying to destroy the client
    # 2. Marks the native_kafka as closed to prevent further operations
    # 3. Does NOT call rd_kafka_destroy() - the resources will leak but the process won't crash
    #
    # @return [nil]
    #
    # @example
    #   producer.trigger_test_fatal_error(47, "Test error")
    #   producer.mark_for_cleanup  # Prevent segfault during GC
    def mark_for_cleanup
      # Undefine the finalizer to prevent GC from calling rd_kafka_destroy
      ObjectSpace.undefine_finalizer(self)

      # Mark the native kafka as closing to prevent further operations
      # We access the instance variable directly to avoid triggering any operations
      @native_kafka.instance_variable_set(:@closing, true)

      nil
    end
  end
end
