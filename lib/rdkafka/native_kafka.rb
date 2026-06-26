# frozen_string_literal: true

module Rdkafka
  # @private
  # A wrapper around a native kafka that polls and cleanly exits
  class NativeKafka
    # Creates a new NativeKafka wrapper
    # @param inner [FFI::Pointer] pointer to the native Kafka handle
    # @param run_polling_thread [Boolean] whether to run a background polling thread
    # @param opaque [Rdkafka::Opaque] opaque object for callback context
    # @param auto_start [Boolean] whether to start the polling thread automatically
    # @param timeout_ms [Integer] poll timeout in milliseconds
    def initialize(inner, run_polling_thread:, opaque:, auto_start: true, timeout_ms: Defaults::NATIVE_KAFKA_POLL_TIMEOUT_MS)
      @inner = inner
      @opaque = opaque

      # Single mutex guarding all access bookkeeping: the in-progress operations counter and the
      # exclusive-access flag. A condition variable lets `#synchronize` wait for in-flight
      # operations to drain WITHOUT holding the mutex while it waits, so the operations themselves
      # can take the mutex to decrement and signal back. Using one mutex (rather than separate
      # increment/decrement mutexes) keeps the counter updates atomic - mutating it under two
      # different mutexes allowed lost updates, which could either let `#synchronize` destroy the
      # handle while an operation was still using it (crash) or leave the counter stuck above zero
      # (hung close).
      @mutex = Mutex.new
      @resource = ConditionVariable.new
      # Lock around internal polling
      @poll_mutex = Mutex.new
      # Counter for operations in progress using inner
      @operations_in_progress = 0
      # Set while a `#synchronize` block runs, with the owning thread so that thread can re-enter
      # `#with_inner` (which `#synchronize` itself uses). Other threads' `#with_inner` calls wait.
      @exclusive = false
      @exclusive_owner = nil

      @run_polling_thread = run_polling_thread

      @timeout_ms = timeout_ms

      start if auto_start

      @closing = false
    end

    # Starts the polling thread if configured
    # @return [nil]
    def start
      synchronize do
        return if @started

        @started = true

        # Trigger initial poll to make sure oauthbearer cb and other initial cb are handled
        Rdkafka::Bindings.rd_kafka_poll(@inner, 0)

        if @run_polling_thread
          # Start thread to poll client for delivery callbacks,
          # not used in consumer.
          @polling_thread = Thread.new do
            loop do
              @poll_mutex.synchronize do
                Rdkafka::Bindings.rd_kafka_poll(@inner, @timeout_ms)
              end

              # Exit thread if closing and the poll queue is empty
              if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(@inner) == 0
                break
              end
            end
          end

          @polling_thread.name = "rdkafka.native_kafka##{Rdkafka::Bindings.rd_kafka_name(@inner).gsub("rdkafka", "")}"
          @polling_thread.abort_on_exception = true
          @polling_thread[:closing] = false
        end
      end
    end

    # Executes a block with the inner native Kafka handle
    # @yield [FFI::Pointer] the inner native Kafka handle
    # @return [Object] the result of the block
    # @raise [ClosedInnerError] when the inner handle is nil
    def with_inner
      incremented = false

      @mutex.synchronize do
        # Wait while another thread holds exclusive access (e.g. close). The exclusive owner itself
        # is allowed through so `#synchronize` can use `#with_inner` re-entrantly.
        @resource.wait(@mutex) while @exclusive && @exclusive_owner != Thread.current
        @operations_in_progress += 1
        incremented = true
      end

      @inner.nil? ? raise(ClosedInnerError) : yield(@inner)
    ensure
      if incremented
        @mutex.synchronize do
          @operations_in_progress -= 1
          # Wake `#synchronize` (and any other waiter) once the last in-flight operation is done
          @resource.broadcast if @operations_in_progress.zero?
        end
      end
    end

    # Executes a block while holding exclusive access to the native Kafka handle
    # @param block [Proc] block to execute with the native handle
    # @yield [FFI::Pointer] the inner native Kafka handle
    # @return [Object] the result of the block
    def synchronize(&block)
      acquire_exclusive

      begin
        with_inner(&block)
      ensure
        release_exclusive
      end
    end

    # Returns a finalizer proc for closing this native Kafka handle
    # @return [Proc] finalizer proc
    def finalizer
      ->(_) { close }
    end

    # Returns whether this native Kafka handle is closed or closing
    # @return [Boolean] true if closed or closing
    def closed?
      @closing || @inner.nil?
    end

    # Enable IO event notifications on the main queue
    # Librdkafka will write to your FD when the queue transitions from empty to non-empty
    #
    # @note This method is incompatible with background polling threads.
    #   If background polling is enabled, use manual polling instead (e.g., consumer.poll)
    #
    # @param fd [Integer] your file descriptor (from IO.pipe or eventfd)
    # @param payload [String] data to write to fd when queue has data (default: "\x01")
    # @return [nil]
    # @raise [ClosedInnerError] when the handle is closed
    # @raise [RuntimeError] when background polling thread is active
    #
    # @example
    #   # Create your own signaling FD
    #   signal_r, signal_w = IO.pipe
    #   native_kafka.enable_main_queue_io_events(signal_w.fileno)
    #
    #   # Monitor it with select
    #   readable, = IO.select([signal_r], nil, nil, timeout)
    #   if readable
    #     consumer.poll(0)  # Get messages
    #   end
    def enable_main_queue_io_events(fd, payload = "\x01")
      if @run_polling_thread
        raise "Cannot enable IO events while background polling thread is active. " \
          "Either disable background polling by setting run_polling_thread: false, " \
          "or use manual polling with consumer.poll() instead of the FD API."
      end

      with_inner do |inner|
        queue_ptr = Bindings.rd_kafka_queue_get_main(inner)
        Bindings.rd_kafka_queue_io_event_enable(queue_ptr, fd, payload, payload.bytesize)
        Bindings.rd_kafka_queue_destroy(queue_ptr)
      end
    end

    # Enable IO event notifications on the background queue
    # Librdkafka will write to your FD when the background queue transitions from empty to non-empty
    #
    # @note This method is incompatible with background polling threads.
    #   If background polling is enabled, use manual polling instead (e.g., consumer.poll)
    #
    # @param fd [Integer] your file descriptor (from IO.pipe or eventfd)
    # @param payload [String] data to write to fd when queue has data (default: "\x01")
    # @return [nil]
    # @raise [ClosedInnerError] when the handle is closed
    # @raise [RuntimeError] when background polling thread is active
    def enable_background_queue_io_events(fd, payload = "\x01")
      if @run_polling_thread
        raise "Cannot enable IO events while background polling thread is active. " \
          "Either disable background polling by setting run_polling_thread: false, " \
          "or use manual polling with consumer.poll() instead of the FD API."
      end

      with_inner do |inner|
        queue_ptr = Bindings.rd_kafka_queue_get_background(inner)
        Bindings.rd_kafka_queue_io_event_enable(queue_ptr, fd, payload, payload.bytesize)
        Bindings.rd_kafka_queue_destroy(queue_ptr)
      end
    end

    # Closes the native Kafka handle and cleans up resources
    # @param object_id [Integer, nil] optional object ID (unused, for finalizer compatibility)
    # @yield optional block to execute before destroying the handle
    # @return [nil]
    def close(object_id = nil)
      return if closed?

      @closing = true

      # Stop and join the polling thread BEFORE taking exclusive access. The polling thread may run
      # callbacks (e.g. the oauthbearer token refresh) that call back into `#with_inner`; if we
      # held exclusive access here those callbacks would block waiting for us while we block
      # waiting for the thread to finish - a deadlock. Joining first lets any in-flight callback
      # complete and the poll loop exit cleanly.
      if @polling_thread
        # Indicate to polling thread that we're closing
        @polling_thread[:closing] = true
        # Wait for the polling thread to finish up. This can be aborted in practice if this code
        # runs from a finalizer.
        @polling_thread.join
      end

      synchronize do
        # This check prevents a race condition where two threads both pass the `closed?` guard and
        # enter close; the first destroys the handle (and nils @poll_mutex), so the second must
        # bail out before touching either.
        next unless @inner

        # Destroy the client after locking the poll mutex as well
        @poll_mutex.lock

        yield if block_given?

        Rdkafka::Bindings.rd_kafka_destroy(@inner)
        @inner = nil
        @opaque = nil
        @poll_mutex.unlock
        @poll_mutex = nil
      end
    end

    private

    # Acquires exclusive access: blocks new (non-re-entrant) operations and waits for all in-flight
    # operations to drain. Must be paired with {#release_exclusive}.
    # @return [nil]
    def acquire_exclusive
      @mutex.synchronize do
        # Wait for any other exclusive holder to finish first
        @resource.wait(@mutex) while @exclusive
        @exclusive = true
        @exclusive_owner = Thread.current

        # Wait for in-flight operations to finish. `@resource.wait` releases @mutex while waiting,
        # so those operations can take it to decrement and signal back. New operations cannot start
        # because `@exclusive` is set.
        @resource.wait(@mutex) until @operations_in_progress.zero?
      end
    end

    # Releases exclusive access acquired via {#acquire_exclusive} and wakes any waiters.
    # @return [nil]
    def release_exclusive
      @mutex.synchronize do
        @exclusive = false
        @exclusive_owner = nil
        @resource.broadcast
      end
    end
  end
end
