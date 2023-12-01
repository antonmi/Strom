defmodule Strom.SplitterTest do
  use ExUnit.Case, async: false

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.{Mixer, Splitter}

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

  setup %{stream1: stream1, stream2: stream2} do
    stream =
      [stream1, stream2]
      |> Mixer.start()
      |> Mixer.stream()

    %{stream: stream}
  end

  test "splitter", %{stream: stream} do
    {original_orders, original_parcels} = orders_and_parcels()

    partitions = [
      fn el -> String.starts_with?(el, "PARCEL_SHIPPED") end,
      fn el -> String.starts_with?(el, "ORDER_CREATED") end
    ]

    splitter = Splitter.start(stream, partitions)
    [stream1, stream2] = Splitter.stream(splitter)

    parcels = Enum.to_list(stream1)
    assert parcels -- original_parcels == []
    assert original_parcels -- parcels == []

    orders = Enum.to_list(stream2)
    assert orders -- original_orders == []
    assert original_orders -- orders == []
  end

  test "stop", %{stream: stream} do
    splitter = Splitter.start(stream, [])
    assert Process.alive?(splitter.pid)
    :ok = Splitter.stop(splitter)
    refute Process.alive?(splitter.pid)
  end

  #  @tag timeout: 300_000
  #  test "memory" do
  #    # add :observer, :runtime_tools, :wx to extra_applications
  #    :observer.start()
  #
  #    source = Source.start(%ReadLines{path: "test_data/input.csv"})
  #    stream = Source.stream(source)
  #
  #    partitions = [
  #      fn el -> String.contains?(el, "Z,111,") end,
  #      fn el -> String.contains?(el, "Z,222,") end,
  #      fn el -> String.contains?(el, "Z,333,") end
  #    ]
  #
  #    splitter = Splitter.start(stream, partitions)
  #
  #    sink1 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/output111.csv"})
  #    sink2 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/output222.csv"})
  #    sink3 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/output333.csv"})
  #
  #    splitter
  #    |> Splitter.stream()
  #    |> Enum.zip([sink1, sink2, sink3])
  #    |> Enum.map(fn {stream, sink} ->
  #      Task.async(fn ->
  #        stream
  #        |> Strom.Sink.stream(sink)
  #        |> Stream.run()
  #      end)
  #    end)
  #    |> Enum.map(fn task -> Task.await(task, :infinity) end)
  #  end
end
