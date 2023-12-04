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
      |> Source.stream(source1, :source1)
      |> Source.stream(source2, :source2)
      |> Source.stream(source3, :source3)

    %{flow: flow}
  end

  test "stream", %{flow: flow} do
    mixer = Mixer.start()

    %{mixed: mixed, source3: source3} = Mixer.stream(flow, mixer, [:source1, :source2], :mixed)

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

  test "stream two identical streams" do
    source = Source.start(%ReadLines{path: "test/data/orders.csv"})
    mixer = Mixer.start()

    %{stream: stream} =
      %{}
      |> Source.stream(source, :s1)
      |> Source.stream(source, :s2)
      |> Mixer.stream(mixer, [:s1, :s2], :stream)

    lines = Enum.to_list(stream)

    {orders, _parcels} = orders_and_parcels()
    assert lines -- orders == []
    assert orders -- lines == []
  end
end
