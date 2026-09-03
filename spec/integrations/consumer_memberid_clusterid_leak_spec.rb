# frozen_string_literal: true

# This integration test verifies that Consumer#cluster_id (and #member_id) do not leak the
# librdkafka-allocated string on every call.
#
# `rd_kafka_clusterid` / `rd_kafka_memberid` each return a newly allocated C string that the
# caller must release with `rd_kafka_mem_free`. They were bound with a `:string` return type,
# which makes FFI copy the bytes into a Ruby String but never free the underlying buffer, so
# every call leaked it. (`rd_kafka_clusterid` was additionally bound with the wrong arity,
# missing its `timeout_ms` argument.) They are now bound as `:pointer`, copied into a Ruby
# string, and freed.
#
# We hammer cluster_id and assert RSS stays flat. RSS is read from /proc on Linux; on platforms
# without /proc the calls are still exercised (no crash) but growth is not asserted.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: no significant memory growth (or growth not measurable on this platform)
# - 1: the cluster id could not be fetched, or RSS grew (leak still present)

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
GROUP = "memberid-clusterid-leak-#{SecureRandom.hex(6)}"
ITERATIONS = 200_000

consumer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP, "group.id": GROUP).consumer

# A cluster_id call with a timeout establishes the broker connection and caches the metadata, so
# we retry until the broker answers.
cluster_id = nil
20.times do
  cluster_id = consumer.cluster_id(5_000)
  break if cluster_id && !cluster_id.empty?

  sleep 0.3
end

if cluster_id.nil? || cluster_id.empty?
  warn "FAIL: could not fetch the cluster id from the broker"
  consumer.close
  exit(1)
end

def rss_kb
  File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
end

measurable = File.exist?("/proc/self/status")

# Settle the Ruby heap before sampling RSS. `GC.compact` defragments the heap so freed pages can be
# released, which keeps the baseline and final samples from drifting purely due to fragmentation.
# A plain `GC.start` leaves that drift in, which is what made this spec trip on Ruby 4.0.
def settle_heap
  GC.start
  GC.compact if GC.respond_to?(:compact)
end

# Warm up so the malloc arena / Ruby heap settle before we measure.
20_000.times { consumer.cluster_id(5_000) }
settle_heap
before = measurable ? rss_kb : 0

ITERATIONS.times { consumer.cluster_id(5_000) }

settle_heap
after = measurable ? rss_kb : 0
consumer.close

if measurable
  delta = after - before
  puts "RSS delta after #{ITERATIONS} cluster_id calls: #{delta} KB"

  # When fixed this is a few dozen KB (64 KB observed locally on Ruby 4.0). Leaking the ~30-byte
  # string on every call would be ~5.7 MB at this iteration count. RSS is noisy on a loaded runner
  # though - Ruby 4.0 CI measured 2.06 MB with a leak-free build, i.e. ~10 bytes/call, a third of
  # what a real leak costs - so the ceiling sits at 3 MB: clear of the observed noise floor and
  # still well under the ~5.7 MB a genuine leak produces.
  if delta > 3_000
    warn "FAIL: RSS grew #{delta} KB over #{ITERATIONS} calls - cluster_id still leaks"
    exit(1)
  end
else
  puts "RSS not measurable on this platform; exercised #{ITERATIONS} cluster_id calls without crashing"
end

puts "PASS: cluster_id did not leak (cluster_id=#{cluster_id.inspect})"
exit(0)
