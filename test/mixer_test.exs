defmodule Strom.MixerTest do
  use ExUnit.Case, async: true
  doctest Strom.Mixer

  alias Strom.{Composite, Mixer, Source, Transformer}
  alias Strom.Source.ReadLines

  setup do
    mixer =
      [:stream1, :stream2]
      |> Mixer.new(:stream, chunk: 1)
      |> Mixer.start()

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

  test "output stream has the same name as one of the input streams" do
    mixer =
      [:stream1, :stream2]
      |> Mixer.new(:stream1, chunk: 1)
      |> Mixer.start()

    flow = %{stream1: [1, 2, 3], stream2: [4, 5, 6]}
    %{stream1: stream} = Mixer.call(flow, mixer)
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
  end

  describe "halt mix stream if one of the stream ends" do
    setup do
      source_finite = Source.new(:finite, [1, 2, 3, 4, 5])
      source_infinite = Source.new(:infinite, Stream.cycle([9, 8, 7]))

      mixer = Mixer.new([:finite, :infinite], :stream, no_wait: true)

      composite =
        [source_finite, source_infinite, mixer]
        |> Composite.new()
        |> Composite.start()

      %{composite: composite}
    end

    test "halt when one stream halts", %{composite: composite} do
      %{stream: stream} = Composite.call(%{}, composite)
      results = Enum.to_list(stream)
      Enum.each([1, 2, 3, 4, 5], fn num -> assert Enum.member?(results, num) end)
      Composite.stop(composite)
    end
  end

  test "two mixers mix into the same stream" do
    mixer1 = Mixer.new([:s1, :s2], :stream, chunk: 1)
    mixer2 = Mixer.new([:s3, :s4], :stream, chunk: 1)
    transformer = Transformer.new(:stream, & &1)

    composite =
      [mixer1, mixer2, transformer]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{s1: [1], s2: [2], s3: [3], s4: [4]}, composite)

    assert Enum.sort(Enum.to_list(stream)) == [1, 2, 3, 4]
  end
end
