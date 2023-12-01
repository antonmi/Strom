defmodule Strom.MixerTest do
  use ExUnit.Case, async: false

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.Mixer

  def orders_and_parcels do
    orders =
      "test/data/orders.csv"
      |> File.read!()
      |> String.split("\n")

    parcels =
      "test/data/parcels.csv"
      |> File.read!()
      |> String.split("\n")

    {orders, parcels}
  end

  setup do
    source1 = Source.start(%ReadLines{path: "test/data/orders.csv"})
    source2 = Source.start(%ReadLines{path: "test/data/parcels.csv"})

    stream1 = Source.stream(source1)
    stream2 = Source.stream(source2)

    %{stream1: stream1, stream2: stream2}
  end

  test "stream", %{stream1: stream1, stream2: stream2} do
    lines =
      [stream1, stream2]
      |> Mixer.start()
      |> Mixer.stream()
      |> Enum.into([])

    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []
  end

  test "stop", %{stream1: stream1} do
    mixer = Mixer.start([stream1])
    assert Process.alive?(mixer.pid)
    :ok = Mixer.stop(mixer)
    refute Process.alive?(mixer.pid)
  end

  test "stream two identical streams", %{stream1: stream1} do
    lines =
      [stream1, stream1]
      |> Mixer.start()
      |> Mixer.stream()
      |> Enum.into([])

    {orders, _parcels} = orders_and_parcels()
    assert lines -- orders == []
    assert orders -- lines == []
  end

  test "stream two identical files", %{stream1: stream1} do
    source2 = Source.start(%ReadLines{path: "test/data/orders.csv"})
    stream2 = Source.stream(source2)

    lines =
      [stream1, stream2]
      |> Mixer.start()
      |> Mixer.stream()
      |> Enum.into([])

    {orders, _parcels} = orders_and_parcels()
    assert lines -- (orders ++ orders) == []
    assert (orders ++ orders) -- lines == []
  end

  test "add streams", %{stream1: stream1, stream2: stream2} do
    lines =
      [stream1]
      |> Mixer.start()
      |> Mixer.add(stream2)
      |> Mixer.stream()
      |> Enum.into([])

    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []
  end

  test "dynamically add streams", %{stream1: stream1, stream2: stream2} do
    mixer = Mixer.start([stream1])

    Task.async(fn ->
      Mixer.add(mixer, stream2)
    end)

    lines =
      mixer
      |> Mixer.stream()
      |> Enum.into([])

    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []
  end

  #  @tag timeout: 300_000
  #  test "memory" do
  #    # add :observer, :runtime_tools, :wx to extra_applications
  #    :observer.start()
  #
  #    source1 = Source.start(%ReadLines{path: "test_data/input.csv"})
  #    source2 = Source.start(%ReadLines{path: "test_data/input.csv"})
  #
  #    stream1 = Source.stream(source1)
  #    stream2 = Source.stream(source2)
  #    sink = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/output.csv"})
  #
  #    [stream1]
  #    |> Mixer.start()
  #    |> Mixer.add(stream2)
  #    |> Mixer.stream()
  #    |> Strom.Sink.stream(sink)
  #    |> Stream.run()
  #  end
end
