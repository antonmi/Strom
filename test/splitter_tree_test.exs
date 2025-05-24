defmodule Strom.SplitterTreeTest do
  use ExUnit.Case, async: true

  alias Strom.{Composite, SplitterTree}

  test "splits 1 stream into 5 with copying" do
    count = 5
    output_streams = Enum.map(1..count, &String.to_atom("s#{&1}"))
    parts = 2
    splitter_tree = SplitterTree.new(:stream, output_streams, parts: parts)

    composite =
      [splitter_tree]
      |> Composite.new()
      |> Composite.start()

    max = 10
    flow = Composite.call(%{stream: Enum.to_list(1..max)}, composite)

    Enum.each(output_streams, fn output_stream ->
      assert Enum.to_list(flow[output_stream]) == Enum.to_list(1..max)
    end)

    Composite.stop(composite)
  end

  test "splits 1 stream into 5 with random distribution (hash mode)" do
    count = 5
    output_streams = Enum.map(1..count, &String.to_atom("s#{&1}"))
    parts = 2
    splitter_tree = SplitterTree.new(:stream, output_streams, parts: parts, mode: :hash)

    composite =
      [splitter_tree]
      |> Composite.new()
      |> Composite.start()

    max = 10
    flow = Composite.call(%{stream: Enum.to_list(1..max)}, composite)

    numbers =
      Enum.reduce(output_streams, [], fn output_stream, numbers ->
        numbers ++ Enum.to_list(flow[output_stream])
      end)

    assert Enum.sort(numbers) == Enum.to_list(1..max)

    Composite.stop(composite)
  end

  test "splits a stream into multiple streams and copies data to each stream" do
    count = :rand.uniform(100)
    output_streams = Enum.map(1..count, &String.to_atom("s#{&1}"))
    parts = 5 + :rand.uniform(5)
    splitter_tree = SplitterTree.new(:stream, output_streams, parts: parts)

    composite =
      [splitter_tree]
      |> Composite.new()
      |> Composite.start()

    max = 10
    flow = Composite.call(%{stream: Enum.to_list(1..max)}, composite)

    Enum.each(output_streams, fn output_stream ->
      assert Enum.to_list(flow[output_stream]) == Enum.to_list(1..max)
    end)

    Composite.stop(composite)
  end

  test "splits a stream into multiple streams with the hash mode" do
    count = :rand.uniform(100)
    output_streams = Enum.map(1..count, &String.to_atom("s#{&1}"))
    parts = 5 + :rand.uniform(5)
    splitter_tree = SplitterTree.new(:stream, output_streams, parts: parts, mode: :hash)

    composite =
      [splitter_tree]
      |> Composite.new()
      |> Composite.start()

    max = 10
    flow = Composite.call(%{stream: Enum.to_list(1..max)}, composite)

    numbers =
      Enum.reduce(output_streams, [], fn output_stream, numbers ->
        numbers ++ Enum.to_list(flow[output_stream])
      end)

    assert Enum.sort(numbers) == Enum.to_list(1..max)

    Composite.stop(composite)
  end
end
