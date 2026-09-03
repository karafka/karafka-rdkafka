# frozen_string_literal: true

module Rdkafka
  # A KIP-932 share group consumer of Kafka messages.
  #
  # Share groups bring queue-like semantics to Kafka: multiple members of the same group consume
  # from the same partitions cooperatively. Partition assignment is entirely broker-driven: there
  # is no rebalance callback and no `assign` step.
  #
  # This is a **deliberately minimal** binding, matching the equally minimal share consumer in
  # confluent-kafka-python ({https://github.com/confluentinc/confluent-kafka-python/pull/2217
  # confluent-kafka-python#2217}). It exposes only {#subscribe}/{#unsubscribe}/{#subscription},
  # a batch {#poll}, {#commit_sync} and {#close}. Acknowledgement is **implicit**: every record
  # delivered by a poll is acknowledged as accepted by the following poll (or by {#commit_sync}).
  # There is intentionally no per-record accept/release/reject, no asynchronous commit and no
  # acknowledgement-commit callback. Anything richer - a managed poll loop, explicit per-record
  # disposition, offset or lifecycle orchestration - belongs in a higher layer such as
  # Karafka/WaterDrop, not in this thin librdkafka wrapper.
  #
  # To create a share consumer set up a {Config} with `:"group.id"` and call
  # {Config#share_consumer share_consumer} on it.
  #
  # @note The share consumer is a **preview** feature of librdkafka and requires a broker with
  #   share groups enabled (Apache Kafka 4.2.0+). Its API may change before general availability
  #   and it is not recommended for production use.
  #
  # @note The share consumer handle is **not thread-safe by design** (matching KIP-932): use one
  #   share consumer per thread. librdkafka enforces this and calls from a second thread raise
  #   an `RdkafkaError` with code `conflict`. This class is deliberately not built on
  #   {NativeKafka}: there is no background polling thread (statistics, error and log callbacks
  #   are all serviced from within {#poll}) and the native handle must never be used from a
  #   polling thread, so only the {#close} interaction is synchronized here. {#close} may be
  #   called from any thread and waits for in-flight calls to finish before tearing down.
  #
  # Preview limitations inherited from librdkafka worth knowing at this layer:
  # - `max.poll.records` (default 500) is a soft bound
  # - a blocking poll can only be ended by its timeout (no wakeup API)
  # - {#close} takes no timeout and is bounded internally by `socket.timeout.ms`
  class ShareConsumer
    include Helpers::OAuth

    # State shared with the GC finalizer, holding just the native handle so the finalizer can
    # destroy it without capturing `self` (which would pin the consumer and prevent collection).
    State = Struct.new(:native)

    private_constant :State

    # The client name librdkafka reports for this consumer (e.g. "rdkafka#consumer-1"), used to
    # correlate global callbacks (such as the OAuthBearer token refresh callback) with this
    # instance. librdkafka exposes no name accessor on the share handle, so this is captured
    # from the first callback that carries it and is nil until then.
    # @return [String, nil]
    attr_reader :name

    # @private
    # Captures the librdkafka client name once known (from callback context)
    attr_writer :name

    # @private
    # @param native [FFI::Pointer] pointer to the native rd_kafka_share_t handle
    # @param opaque [Rdkafka::Opaque, nil] opaque used for callback context. Held here so it is
    #   not garbage collected while the native client can still reference it
    #   (Config.opaques is a WeakMap).
    def initialize(native, opaque: nil)
      @opaque = opaque
      # State shared with the GC finalizer so it can destroy the native handle without capturing
      # `self` (which would pin the consumer and prevent collection).
      @state = State.new(native)
      # Guards handle teardown against in-flight calls: increments happen under this mutex and
      # close holds it while draining and destroying (mirrors NativeKafka's approach)
      @access_mutex = Mutex.new
      # Separate mutex for decrements so a blocked close (holding @access_mutex) cannot
      # deadlock with an operation trying to finish
      @decrement_mutex = Mutex.new
      # Number of calls currently using the native handle
      @operations_in_progress = 0

      ObjectSpace.define_finalizer(self, self.class.finalizer(@state))
    end

    # Builds the GC finalizer for a share consumer. `rd_kafka_share_destroy` internally closes
    # the consumer (flushing the implicit acknowledgements and leaving the group) when it was not
    # closed explicitly.
    #
    # @private
    # @param state [State] holder carrying the native handle
    # @return [Proc] finalizer proc that must not reference the consumer instance
    def self.finalizer(state)
      proc do
        native = state.native

        if native
          state.native = nil
          error = Rdkafka::Bindings.rd_kafka_share_destroy(native)
          Rdkafka::Bindings.rd_kafka_error_destroy(error) unless error.null?
        end
      end
    end

    # Subscribes to one or more topics letting the broker assign partitions to the members of
    # the share group.
    #
    # Wildcard (regex) subscriptions are not supported by share consumers.
    #
    # @param topics [Array<String>] One or more topic names
    # @return [nil]
    # @raise [ArgumentError] When no topics are given. librdkafka would treat an empty
    #   subscribe as an unsubscribe (unlike the regular consumer, which rejects it), which
    #   silently drops the subscription - use {#unsubscribe} for that.
    # @raise [RdkafkaError] When subscribing fails
    def subscribe(*topics)
      raise ArgumentError, "at least one topic is required (an empty subscribe would unsubscribe)" if topics.empty?

      with_native(__method__) do |native|
        tpl = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(topics.length)

        begin
          topics.each do |topic|
            Rdkafka::Bindings.rd_kafka_topic_partition_list_add(tpl, topic, Rdkafka::Bindings::RD_KAFKA_PARTITION_UA)
          end

          response = Rdkafka::Bindings.rd_kafka_share_subscribe(native, tpl)
          Rdkafka::RdkafkaError.validate!(response, "Error subscribing to '#{topics.join(", ")}'")
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(tpl)
        end
      end

      nil
    end

    # Unsubscribes from all subscribed topics and leaves the share group assignment.
    #
    # @return [nil]
    # @raise [RdkafkaError] When unsubscribing fails
    def unsubscribe
      with_native(__method__) do |native|
        response = Rdkafka::Bindings.rd_kafka_share_unsubscribe(native)
        Rdkafka::RdkafkaError.validate!(response)
      end

      nil
    end

    # Returns the current subscription to topics.
    #
    # @return [Rdkafka::Consumer::TopicPartitionList]
    # @raise [RdkafkaError] When getting the subscription fails
    def subscription
      with_native(__method__) do |native|
        ptr = FFI::MemoryPointer.new(:pointer)
        response = Rdkafka::Bindings.rd_kafka_share_subscription(native, ptr)
        Rdkafka::RdkafkaError.validate!(response)

        native_tpl = ptr.read_pointer

        begin
          Rdkafka::Consumer::TopicPartitionList.from_native_tpl(native_tpl)
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native_tpl)
        end
      end
    end

    # Polls for a batch of messages.
    #
    # Unlike {Consumer#poll} a single call returns a whole batch (up to `max.poll.records`,
    # which is a soft bound in the preview). Every record delivered by the previous poll is
    # acknowledged as accepted by this call (implicit acknowledgement).
    #
    # Record-level errors (for example an unauthorized topic) are delivered as messages with
    # {ShareConsumer::Message#error} set rather than raised, since a batch can mix valid records
    # and errors. Callers should check {ShareConsumer::Message#error?} before using the payload.
    #
    # @param timeout_ms [Integer] Timeout of this poll
    # @return [Array<ShareConsumer::Message, RdkafkaError>] polled messages, empty when the
    #   timeout expired without records. A message that fails to build (see below) is an
    #   `RdkafkaError` rather than a `ShareConsumer::Message`.
    # @raise [RdkafkaError] When polling fails at the batch level
    def poll(timeout_ms = Defaults::SHARE_CONSUMER_POLL_TIMEOUT_MS)
      with_native(__method__) do |native|
        messages_ptr = FFI::MemoryPointer.new(:pointer)
        error_ptr = Rdkafka::Bindings.rd_kafka_share_poll(native, timeout_ms, messages_ptr)

        # Destroys the native error and raises when it carries an actual error
        Rdkafka::RdkafkaError.validate!(error_ptr)

        batch = messages_ptr.read_pointer

        next [] if batch.null?

        begin
          count = Rdkafka::Bindings.rd_kafka_messages_count(batch)

          Array.new(count) do |i|
            message_ptr = Rdkafka::Bindings.rd_kafka_messages_get(batch, i)
            native_message = Rdkafka::Bindings::Message.new(message_ptr)

            # Only touch the error machinery for the rare record-level error entries
            error = if native_message[:err] == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
              nil
            else
              Rdkafka::RdkafkaError.build(native_message)
            end

            begin
              Message.new(
                native_message,
                delivery_count: Rdkafka::Bindings.rd_kafka_message_delivery_count(message_ptr),
                error: error
              )
            rescue Rdkafka::RdkafkaError => e
              # A message that fails to build (e.g. a header read error) is surfaced inline as an
              # error rather than discarding the whole batch - including the messages already
              # built - and raising, which would silently lose them. Mirrors how
              # Consumer#poll_batch handles the same failure mode.
              e
            end
          end
        ensure
          # Destroys the batch together with all the messages it contains. All message content
          # has been copied into Ruby objects at this point.
          Rdkafka::Bindings.rd_kafka_messages_destroy(batch)
        end
      end
    end

    # Sends the pending implicit acknowledgements to the broker and waits for the responses.
    #
    # @param timeout_ms [Integer] Maximum time to wait for the broker replies
    # @return [Rdkafka::Consumer::TopicPartitionList, nil] per-partition results where each
    #   partition carries the acknowledgement outcome in its `err` field, or nil when there was
    #   nothing to commit
    # @raise [RdkafkaError] When the commit fails as a whole
    def commit_sync(timeout_ms = Defaults::SHARE_CONSUMER_COMMIT_SYNC_TIMEOUT_MS)
      with_native(__method__) do |native|
        tpl_ptr = FFI::MemoryPointer.new(:pointer)
        error_ptr = Rdkafka::Bindings.rd_kafka_share_commit_sync(native, timeout_ms, tpl_ptr)

        Rdkafka::RdkafkaError.validate!(error_ptr)

        native_tpl = tpl_ptr.read_pointer

        next nil if native_tpl.null?

        begin
          Rdkafka::Consumer::TopicPartitionList.from_native_tpl(native_tpl)
        ensure
          Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native_tpl)
        end
      end
    end

    # Marks failed oauth token acquire in librdkafka for this share consumer
    #
    # @param reason [String] human readable error reason for failing to acquire token
    def oauthbearer_set_token_failure(reason)
      with_native(__method__) do |native|
        Rdkafka::Bindings.rd_kafka_share_oauthbearer_set_token_failure(native, reason)
      end
    end

    # Whether this share consumer has closed
    # @return [Boolean]
    def closed?
      @state.native.nil?
    end

    # Closes this share consumer: sends any pending acknowledgements, leaves the share group and
    # frees the native handle. Waits for in-flight calls from other threads to finish first.
    # Close is bounded internally by `socket.timeout.ms` (there is no timeout argument in the
    # librdkafka preview).
    #
    # @return [nil]
    # @raise [RdkafkaError] When closing or destroying reported an error. librdkafka leaves the
    #   instance intact in that case, so the consumer keeps its native handle and `close` can be
    #   retried.
    def close
      return if closed?

      @access_mutex.synchronize do
        # Wait for in-flight calls on other threads to finish using the handle. New calls block
        # on the mutex above while we hold it.
        sleep(Defaults::NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS / 1_000.0) until @operations_in_progress.zero?

        native = @state.native

        # Lost the race against a concurrent close that already finished
        return if native.nil?

        # librdkafka leaves the instance intact when close/destroy fail, and validate! raises
        # before any reference (or the finalizer) is dropped, so the caller can retry close
        # and the handle cannot leak silently.
        #
        # The close step is skipped when librdkafka already considers the consumer closed
        # (e.g. a prior close attempt succeeded but its destroy failed and was retried).
        if Rdkafka::Bindings.rd_kafka_share_consumer_closed(native).zero?
          Rdkafka::RdkafkaError.validate!(Rdkafka::Bindings.rd_kafka_share_consumer_close(native))
        end

        Rdkafka::RdkafkaError.validate!(Rdkafka::Bindings.rd_kafka_share_destroy(native))

        # The native handle is gone only now; drop all references and the finalizer
        ObjectSpace.undefine_finalizer(self)
        @state.native = nil
      end

      nil
    end

    private

    # Executes a block with the native share handle while tracking the call so {#close} cannot
    # tear the handle down mid-call from another thread. Increments happen under the access
    # mutex that close holds for the whole teardown, so once close is draining no new call can
    # slip in; librdkafka's own single-owner gate still rejects genuinely concurrent API calls.
    #
    # @param method [Symbol] public method name, used for the ClosedConsumerError
    # @yield [FFI::Pointer] the native share handle
    # @raise [ClosedConsumerError] when the consumer is closed or closing
    def with_native(method)
      # Owning the mutex here means re-entry from a callback fired during #close's teardown
      # (the only internal holder) - the handle is going away, so reject before tracking
      raise Rdkafka::ClosedConsumerError.new(method) if @access_mutex.owned?

      @access_mutex.synchronize { @operations_in_progress += 1 }

      begin
        native = @state.native

        raise Rdkafka::ClosedConsumerError.new(method) if native.nil?

        yield(native)
      ensure
        @decrement_mutex.synchronize { @operations_in_progress -= 1 }
      end
    end

    # Performs the share-specific native token set call, reusing the buffer and extension
    # plumbing from {Helpers::OAuth}
    #
    # @param token [String] the token value
    # @param lifetime_ms [Integer] token expiry, in milliseconds since the epoch
    # @param principal_name [String] the Kafka principal name associated with the token
    # @param extensions_ptr [FFI::Pointer, nil] `const char **` built by
    #   {Helpers::OAuth#map_extensions}
    # @param extensions_size [Integer] number of key-value pairs pointed to by `extensions_ptr`
    # @param error_buffer [FFI::MemoryPointer] 256-byte buffer for the error string
    # @return [Integer] 0 on success
    def oauthbearer_native_set_token(token, lifetime_ms, principal_name, extensions_ptr, extensions_size, error_buffer)
      with_native(:oauthbearer_set_token) do |native|
        Rdkafka::Bindings.rd_kafka_share_oauthbearer_set_token(
          native, token, lifetime_ms, principal_name,
          extensions_ptr, extensions_size, error_buffer, 256
        )
      end
    end
  end
end
