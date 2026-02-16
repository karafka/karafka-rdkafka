# frozen_string_literal: true

module Rdkafka
  # Namespace for some small utilities used in multiple components
  module Helpers
    # Time related methods used across Karafka
    module Time
      # @return [Float] current monotonic time in seconds with microsecond precision
      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # @return [Integer] current monotonic time in milliseconds
      def monotonic_now_ms
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC, :millisecond)
      end
    end
  end
end
