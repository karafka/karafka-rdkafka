# frozen_string_literal: true

# Fails the integration spec unless the condition holds
#
# @param condition [Object] value that must be truthy
# @param message [String] what was expected, printed when the condition does not hold
def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end
