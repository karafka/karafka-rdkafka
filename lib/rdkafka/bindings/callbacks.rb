# frozen_string_literal: true

module Rdkafka
  module Bindings
    LogCallback = FFI::Function.new(
      :void, [:pointer, :int, :string, :string]
    ) do |_client_ptr, level, _level_string, line|
      severity = case level
      when 0, 1, 2
        Logger::FATAL
      when 3
        Logger::ERROR
      when 4
        Logger::WARN
      when 5, 6
        Logger::INFO
      when 7
        Logger::DEBUG
      else
        Logger::UNKNOWN
      end

      Rdkafka::Config.ensure_log_thread
      Rdkafka::Config.log_queue << [severity, "rdkafka: #{line}"]
    end

    StatsCallback = FFI::Function.new(
      :int, [:pointer, :string, :int, :pointer]
    ) do |_client_ptr, json, _json_len, _opaque|
      if Rdkafka::Config.statistics_callback
        stats = JSON.parse(json)

        # If user requested statistics callbacks, we can use the statistics data to get the
        # partitions count for each topic when this data is published. That way we do not have
        # to query this information when user is using `partition_key`. This takes around 0.02ms
        # every statistics interval period (most likely every 5 seconds) and saves us from making
        # any queries to the cluster for the partition count.
        #
        # One edge case is if user would set the `statistics.interval.ms` much higher than the
        # default current partition count refresh (30 seconds). This is taken care of as the lack
        # of reporting to the partitions cache will cause cache expire and blocking refresh.
        #
        # If user sets `topic.metadata.refresh.interval.ms` too high this is on the user.
        #
        # Since this cache is shared, having few consumers and/or producers in one process will
        # automatically improve the querying times even with low refresh times.
        (stats["topics"] || EMPTY_HASH).each do |topic_name, details|
          partitions_count = details["partitions"].keys.count { |k| !(k == RD_KAFKA_PARTITION_UA_STR) }

          next unless partitions_count.positive?

          Rdkafka::Producer.partitions_count_cache.set(topic_name, partitions_count)
        end

        Rdkafka::Config.statistics_callback.call(stats)
      end

      # Return 0 so librdkafka frees the json string
      RD_KAFKA_RESP_ERR_NO_ERROR
    end

    # Retrieves fatal error details from a kafka client handle.
    # This is a helper method to extract fatal error information consistently
    # across different parts of the codebase (callbacks, testing utilities, etc.).
    #
    # @param client_ptr [FFI::Pointer] Native kafka client pointer
    # @return [Hash, nil] Hash with :error_code and :error_string if fatal error occurred,
    #   nil otherwise
    #
    # @example
    #   details = Rdkafka::Bindings.extract_fatal_error(client_ptr)
    #   if details
    #     puts "Fatal error #{details[:error_code]}: #{details[:error_string]}"
    #   end
    def self.extract_fatal_error(client_ptr)
      error_buffer = FFI::MemoryPointer.new(:char, FATAL_ERROR_BUFFER_SIZE)

      error_code = rd_kafka_fatal_error(client_ptr, error_buffer, FATAL_ERROR_BUFFER_SIZE)

      return nil if error_code == RD_KAFKA_RESP_ERR_NO_ERROR

      {
        error_code: error_code,
        error_string: error_buffer.read_string
      }
    end

    ErrorCallback = FFI::Function.new(
      :void, [:pointer, :int, :string, :pointer]
    ) do |client_ptr, err_code, reason, _opaque|
      if Rdkafka::Config.error_callback
        instance_name = client_ptr.null? ? nil : Rdkafka::Bindings.rd_kafka_name(client_ptr)

        # Handle fatal errors according to librdkafka documentation:
        # When ERR__FATAL is received, we must call rd_kafka_fatal_error()
        # to get the actual underlying fatal error code and description.
        error = if err_code == RD_KAFKA_RESP_ERR__FATAL
          Rdkafka::RdkafkaError.build_fatal(
            client_ptr,
            fallback_error_code: err_code,
            fallback_message: reason,
            instance_name: instance_name
          )
        else
          Rdkafka::RdkafkaError.build(err_code, broker_message: reason, instance_name: instance_name)
        end

        error.set_backtrace(caller)
        Rdkafka::Config.error_callback.call(error)
      end
    end

    # The OAuth callback is currently global and contextless. This means that the callback will be
    # called for all instances, and the callback must be able to determine to which instance it is
    # associated. The instance name will be provided in the callback, allowing the callback to
    # reference the correct instance.
    #
    # An example of how to use the instance name in the callback is given below.
    # The `refresh_token` is configured as the `oauthbearer_token_refresh_callback`.
    # `instances` is a map of client names to client instances, maintained by the user.
    #
    # ```
    #   def refresh_token(config, client_name)
    #     client = instances[client_name]
    #     client.oauthbearer_set_token(
    #       token: 'new-token-value',
    #       lifetime_ms: token-lifetime-ms,
    #       principal_name: 'principal-name'
    #     )
    #   end
    # ```
    OAuthbearerTokenRefreshCallback = FFI::Function.new(
      :void, [:pointer, :string, :pointer]
    ) do |client_ptr, config, opaque_ptr|
      if Rdkafka::Config.oauthbearer_token_refresh_callback && !client_ptr.null?
        client_name = Rdkafka::Bindings.rd_kafka_name(client_ptr)

        # Share consumers have no way to query their librdkafka client name (the share handle
        # exposes no name accessor), so hand it over here - where librdkafka provides it -
        # letting applications correlate this callback's client name with a ShareConsumer via
        # ShareConsumer#name.
        Rdkafka::Config.opaques[opaque_ptr.to_i]&.capture_client_name(client_name)

        Rdkafka::Config.oauthbearer_token_refresh_callback.call(config, client_name)
      end
    end
  end
end
