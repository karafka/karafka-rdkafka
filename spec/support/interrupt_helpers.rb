# frozen_string_literal: true

# Helpers that deliver an interrupt (`Thread#raise`, `Thread#kill` or a raise from a signal trap
# handler) at an exact point of a client call, found by TracePoint, so specs can check that the
# client still closes afterwards.
module InterruptHelpers
  # A worker thread parked by TracePoint at a point of a client call
  ParkedWorker = Struct.new(:thread, :trace, :release)

  # Runs the client call in a new thread and parks it when the given `Mutex#synchronize` call it
  # makes starts (`:call`) or ends (`:return`). `Mutex#synchronize` is a C method before Ruby 4.0,
  # so both kinds of events are traced.
  #
  # @param event [Symbol] `:call` or `:return`
  # @param call [Integer] which `Mutex#synchronize` call of the worker to park at, from 1
  # @param client_call [Proc] the client call to run in the worker
  # @return [ParkedWorker] the parked worker
  def park_worker(event, call, &client_call)
    park_worker_on(event, :"c_#{event}", call: call, client_call: client_call) do |tp|
      tp.defined_class == Thread::Mutex && tp.method_id == :synchronize
    end
  end

  # Runs the client call in a new thread and parks it at the first run of the line of the source
  # file of the given method that has the given code
  #
  # @param method [UnboundMethod] method whose source file has the line
  # @param code [String] the stripped code of the line
  # @param client_call [Proc] the client call to run in the worker
  # @return [ParkedWorker] the parked worker
  def park_worker_at_line(method, code, &client_call)
    path = method.source_location.first
    lineno = File.readlines(path).index { |line| line.strip == code } + 1

    park_worker_on(:line, call: 1, client_call: client_call) do |tp|
      tp.path == path && tp.lineno == lineno
    end
  end

  # Runs the client call in a new thread and parks it at the given matching event
  #
  # @param events [Array<Symbol>] TracePoint events to trace
  # @param call [Integer] which matching event of the worker to park at, from 1
  # @param client_call [Proc] the client call to run in the worker
  # @param matches [Proc] tells if a TracePoint event matches
  # @return [ParkedWorker] the parked worker
  def park_worker_on(*events, call:, client_call:, &matches)
    calls = 0
    worker = nil
    release = Queue.new
    parked = Queue.new

    trace = TracePoint.new(*events) do |tp|
      next unless Thread.current == worker
      next unless matches.call(tp)

      calls += 1
      next unless calls == call

      parked << true
      release.pop
    end

    worker = Thread.new do
      Thread.stop
      client_call.call
    rescue
      nil
    end

    sleep(0.01) until worker.status == "sleep"
    trace.enable
    worker.run

    expect(parked.pop(timeout: 5)).to be(true)

    ParkedWorker.new(worker, trace, release)
  end

  # Lets a parked worker go on and waits for it to end
  #
  # @param parked [ParkedWorker] the parked worker
  def finish_parked_worker(parked)
    parked.release << true
    parked.thread.join(5)
    parked.trace.disable
  end

  # Runs the block and raises from a `USR1` trap handler at each of the given matching events
  #
  # @param events [Array<Symbol>] TracePoint events to trace
  # @param calls [Array<Integer>] which matching events to raise at, from 1
  # @param matches [Proc] tells if a TracePoint event matches
  # @param before_signal [Proc, nil] runs before each signal
  # @param block [Proc] the code to run
  def trapped_at(events, calls, matches, before_signal: nil, &block)
    error = Class.new(StandardError)
    previous = trap("USR1") { raise error }
    seen = 0

    trace = TracePoint.new(*events) do |tp|
      next unless matches.call(tp)

      seen += 1
      next unless calls.include?(seen)

      before_signal&.call
      Process.kill("USR1", Process.pid)
      sleep(5)
    end

    expect { trace.enable(&block) }.to raise_error(error)
  ensure
    trap("USR1", previous)
  end

  # Runs the block and raises from a `USR1` trap handler at each of the given calls of a method
  #
  # @param klass [Module] class that defines the method
  # @param method_id [Symbol] name of the method
  # @param event [Symbol] `:call` or `:return`, C method events are traced too
  # @param calls [Array<Integer>] which calls to raise at, from 1
  # @param before_signal [Proc, nil] runs before each signal
  # @param block [Proc] the code to run
  def method_trapped_at(klass, method_id, event, calls, before_signal: nil, &block)
    matches = ->(tp) { tp.defined_class == klass && tp.method_id == method_id }

    trapped_at([event, :"c_#{event}"], calls, matches, before_signal: before_signal, &block)
  end

  # Runs the block and raises from a `USR1` trap handler at the first run of each of the given
  # lines of the source file of the given method, in order
  #
  # @param method [UnboundMethod] method whose source file has the lines
  # @param lines [Array<Array(String, Integer)>] `[stripped line, occurrence]` pairs
  # @param block [Proc] the code to run
  def lines_trapped(method, *lines, &block)
    path = method.source_location.first
    source = File.readlines(path).map(&:strip)

    linenos = lines.map do |text, occurrence|
      source.each_index.select { |index| source[index] == text }.fetch(occurrence) + 1
    end

    matches = ->(tp) { tp.path == path && linenos.include?(tp.lineno) }

    trapped_at([:line], (1..linenos.size).to_a, matches, &block)
  end

  # Expects `close` of the client, run in another thread, to return
  #
  # @param client [#close] the client to close
  def expect_close_to_finish(client)
    closer = Thread.new { client.close }

    expect(closer.join(10)).not_to be_nil
  ensure
    closer&.kill
  end
end
