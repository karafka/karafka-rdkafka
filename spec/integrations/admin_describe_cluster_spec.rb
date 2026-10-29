# frozen_string_literal: true

# This integration test shows how to describe a cluster with `Admin#describe_cluster` and verifies
# the result end to end.
#
# `describe_cluster` returns the cluster id, the current controller and every broker node (id,
# host, port and rack) without fetching any topic metadata. With
# `include_authorized_operations: true` it also returns the operations this client may perform on
# the cluster, as `Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_*` codes.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the cluster was described correctly
# - 1: a described value was missing or wrong

require "rdkafka"

$stdout.sync = true

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

begin
  report = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000)

  puts "Cluster id: #{report.cluster_id}"
  puts "Controller: #{report.controller.inspect}"
  report.nodes.each { |node| puts "Node: #{node[:id]} #{node[:host]}:#{node[:port]} rack=#{node[:rack].inspect}" }

  if report.cluster_id.to_s.empty?
    warn "FAIL: cluster_id is empty"
    exit(1)
  end
  if report.nodes.empty?
    warn "FAIL: no nodes described"
    exit(1)
  end

  report.nodes.each do |node|
    unless node[:id].is_a?(Integer)
      warn "FAIL: node has no id: #{node.inspect}"
      exit(1)
    end
    if node[:host].to_s.empty?
      warn "FAIL: node has no host: #{node.inspect}"
      exit(1)
    end
    unless node[:port].is_a?(Integer) && node[:port].positive?
      warn "FAIL: node has no port: #{node.inspect}"
      exit(1)
    end
  end

  unless report.nodes.include?(report.controller)
    warn "FAIL: controller #{report.controller.inspect} is not one of the nodes"
    exit(1)
  end
  unless report.authorized_operations.nil?
    warn "FAIL: authorized operations returned without being requested"
    exit(1)
  end

  report = admin.describe_cluster(include_authorized_operations: true).wait(max_wait_timeout_ms: 15_000)

  puts "Authorized operations: #{report.authorized_operations.inspect}"

  unless report.authorized_operations.is_a?(Array) && !report.authorized_operations.empty?
    warn "FAIL: authorized operations missing: #{report.authorized_operations.inspect}"
    exit(1)
  end
ensure
  admin.close
end

puts "PASS: the cluster was described correctly"
exit(0)
