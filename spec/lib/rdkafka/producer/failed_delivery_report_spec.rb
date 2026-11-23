# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::FailedDeliveryReport do
  let(:report) do
    described_class.new(
      2,
      -1,
      topic_name,
      Rdkafka::Bindings::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH,
      "label",
      status: Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_NOT_PERSISTED,
      latency: 1_000,
      broker_id: 1,
      broker_message: "invalid record"
    )
  end

  let(:topic_name) { TestTopics.unique }

  it { expect(report).to be_a(Rdkafka::Producer::DeliveryReport) }

  it "gets the error text" do
    expect(report.broker_message).to eq("invalid record")
  end

  it "gets the delivery report fields" do
    expect(report.partition).to eq(2)
    expect(report.offset).to eq(-1)
    expect(report.topic).to eq(topic_name)
    expect(report.error).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH)
    expect(report.label).to eq("label")
    expect(report).to be_not_persisted
    expect(report.latency).to eq(1_000)
    expect(report.broker_id).to eq(1)
  end

  context "without an error text" do
    let(:report) { described_class.new(2, -1, topic_name, Rdkafka::Bindings::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH) }

    it { expect(report.broker_message).to be_nil }
  end
end
