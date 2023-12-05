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
    source3 = Source.start(%ReadLines{path: "test/data/parcels.csv"})

    flow =
      %{}
      |> Source.call(source1, :source1)
      |> Source.call(source2, :source2)
      |> Source.call(source3, :source3)

    %{flow: flow}
  end

  test "call with map", %{flow: flow} do
    mixer = Mixer.start()

    partitions = %{
      source1: fn el -> String.contains?(el, "111") end,
      source2: fn el -> String.contains?(el, "111") end
    }

    %{mixed: mixed, source3: source3} = Mixer.call(flow, mixer, partitions, :mixed)

    lines = Enum.to_list(mixed)
    assert length(lines) == 99
    {_orders, parcels} = orders_and_parcels()

    source3_lines = Enum.to_list(source3)
    assert length(source3_lines) == length(parcels)
  end

  test "call with list of streams", %{flow: flow} do
    mixer = Mixer.start()

    %{mixed: mixed, source3: source3} = Mixer.call(flow, mixer, [:source1, :source2], :mixed)

    lines = Enum.to_list(mixed)
    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []

    source3_lines = Enum.to_list(source3)
    assert length(source3_lines) == length(parcels)
  end

  test "stop" do
    mixer = Mixer.start()
    assert Process.alive?(mixer.pid)
    :ok = Mixer.stop(mixer)
    refute Process.alive?(mixer.pid)
  end

  test "stream two identical sources" do
    source = Source.start(%ReadLines{path: "test/data/orders.csv"})
    mixer = Mixer.start()

    %{stream: stream} =
      %{}
      |> Source.call(source, :s1)
      |> Source.call(source, :s2)
      |> Mixer.call(mixer, [:s1, :s2], :stream)

    lines = Enum.to_list(stream)

    {orders, _parcels} = orders_and_parcels()
    assert length(lines) == length(orders)
  end
end
