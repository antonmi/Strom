defmodule Strom.SourceTest do
  use ExUnit.Case, async: true

  alias Strom.Source
  alias Strom.Source.ReadLines

  setup do
    source =
      :my_stream
      |> Source.new(%ReadLines{path: "test/data/orders.csv"})
      |> Source.start()

    %{source: source}
  end

  test "source init", %{source: source} do
    assert Process.alive?(source.pid)
    assert source.origin.path == "test/data/orders.csv"
    Source.stop(source)
    refute Process.alive?(source.pid)
  end

  test "call", %{source: source} do
    assert {["ORDER_CREATED,2017-04-18T20:00:00.000Z,111,3"], %Source{}} = Source.call(source)
  end

  test "stream lines", %{source: source} do
    %{my_stream: stream} = Source.call(%{}, source)
    lines = Enum.to_list(stream)
    assert Enum.join(lines, "\n") == File.read!("test/data/orders.csv")
  end

  test "several sources", %{source: source} do
    another_source =
      :another_stream
      |> Source.new(%ReadLines{path: "test/data/orders.csv"})
      |> Source.start()

    %{my_stream: stream, another_stream: another_stream} =
      %{}
      |> Source.call(source)
      |> Source.call(another_source)

    list = Enum.to_list(stream)
    another_list = Enum.to_list(another_stream)

    assert Enum.join(list, "\n") == File.read!("test/data/orders.csv")
    assert Enum.join(another_list, "\n") == File.read!("test/data/orders.csv")
  end

  test "several sources for one stream" do
    source =
      :my_stream
      |> Source.new([4, 5, 6])
      |> Source.start()

    %{my_stream: stream} = Source.call(%{my_stream: [1, 2, 3]}, source)

    numbers = Enum.to_list(stream)
    assert Enum.sort(numbers) == [1, 2, 3, 4, 5, 6]
  end
end
