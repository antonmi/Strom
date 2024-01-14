defmodule Strom.MixerTest do
  use ExUnit.Case, async: false

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.Mixer

  setup do
    mixer =
      [:stream1, :stream2]
      |> Mixer.new(:stream)
      |> Mixer.start(buffer: 1)

    %{mixer: mixer}
  end

  test "start and stop", %{mixer: mixer} do
    assert Process.alive?(mixer.pid)
    Mixer.stop(mixer)
    refute Process.alive?(mixer.pid)
  end

  test "do mix", %{mixer: mixer} do
    flow = %{stream1: [1, 2, 3], stream2: [4, 5, 6]}
    %{stream: stream} = Mixer.call(flow, mixer)
    assert Enum.sort(Enum.to_list(stream)) == [1, 2, 3, 4, 5, 6]
  end

  describe "complex cases" do
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
      source1 =
        :source1
        |> Source.new(ReadLines.new("test/data/orders.csv"))
        |> Source.start()

      source2 =
        :source2
        |> Source.new(ReadLines.new("test/data/parcels.csv"))
        |> Source.start()

      source3 =
        :source3
        |> Source.new(ReadLines.new("test/data/parcels.csv"))
        |> Source.start()

      flow =
        %{}
        |> Source.call(source1)
        |> Source.call(source2)
        |> Source.call(source3)

      %{flow: flow}
    end

    test "call with map", %{flow: flow} do
      partitions = %{
        source1: fn el -> String.contains?(el, "111") end,
        source2: fn el -> String.contains?(el, "111") end
      }

      mixer =
        partitions
        |> Mixer.new(:mixed)
        |> Mixer.start()

      %{mixed: mixed, source3: source3} = Mixer.call(flow, mixer)

      lines = Enum.to_list(mixed)
      assert length(lines) == 99
      {_orders, parcels} = orders_and_parcels()

      source3_lines = Enum.to_list(source3)
      assert length(source3_lines) == length(parcels)
    end

    test "call with list of streams", %{flow: flow} do
      mixer =
        [:source1, :source2]
        |> Mixer.new(:mixed)
        |> Mixer.start()

      %{mixed: mixed, source3: source3} = Mixer.call(flow, mixer)

      lines = Enum.to_list(mixed)
      {orders, parcels} = orders_and_parcels()
      assert lines -- (orders ++ parcels) == []
      assert (orders ++ parcels) -- lines == []

      source3_lines = Enum.to_list(source3)
      assert length(source3_lines) == length(parcels)
    end

    test "stream one file into two streams" do
      source1 =
        :s1
        |> Source.new(ReadLines.new("test/data/orders.csv"))
        |> Source.start()

      source2 =
        :s2
        |> Source.new(ReadLines.new("test/data/orders.csv"))
        |> Source.start()

      mixer =
        [:s1, :s2]
        |> Mixer.new(:stream)
        |> Mixer.start()

      %{stream: stream} =
        %{}
        |> Source.call(source1)
        |> Source.call(source2)
        |> Mixer.call(mixer)

      lines = Enum.to_list(stream)

      {orders, _parcels} = orders_and_parcels()
      assert length(lines) == length(orders) * 2
    end

    test "mixer as simple filter" do
      source1 =
        :stream
        |> Source.new(ReadLines.new("test/data/orders.csv"))
        |> Source.start()

      mixer =
        %{
          stream: fn el -> String.contains?(el, "111,3") end
        }
        |> Mixer.new(:stream)
        |> Mixer.start()

      %{stream: stream} =
        %{}
        |> Source.call(source1)
        |> Mixer.call(mixer)

      stream
      |> Enum.to_list()
      |> Enum.each(fn el -> assert String.contains?(el, "111,3") end)
    end
  end
end
