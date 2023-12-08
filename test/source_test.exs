defmodule Strom.SourceTest do
  use ExUnit.Case, async: true

  alias Strom.Source
  alias Strom.Source.ReadLines

  setup do
    path = "test/data/orders.csv"
    source = Source.start(%ReadLines{path: path})
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
    %{my_stream: stream} = Source.call(%{}, source, :my_stream)
    lines = Enum.to_list(stream)
    assert Enum.join(lines, "\n") == File.read!("test/data/orders.csv")
  end

  test "several sources", %{source: source} do
    another_source = Source.start(%ReadLines{path: "test/data/orders.csv"})

    %{my_stream: stream, another_stream: another_stream} =
      %{}
      |> Source.call(source, :my_stream)
      |> Source.call(another_source, :another_stream)

    list = Enum.to_list(stream)
    another_list = Enum.to_list(another_stream)

    assert Enum.join(list, "\n") == File.read!("test/data/orders.csv")
    assert Enum.join(another_list, "\n") == File.read!("test/data/orders.csv")
  end

  test "several sources for one stream", %{source: source} do
    %{my_stream: stream} =
      Source.call(%{my_stream: [1, 2, 3]}, source, :my_stream)

    lines = Enum.to_list(stream)
    assert Enum.member?(lines, 1)
    assert Enum.member?(lines, 2)
    assert Enum.member?(lines, 3)
  end

  test "stop", %{source: source} do
    assert Source.stop(source) == :ok
    refute Process.alive?(source.pid)
  end
end
