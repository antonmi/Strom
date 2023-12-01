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
  end

  test "call", %{source: source} do
    assert {["ORDER_CREATED,2017-04-18T20:00:00.000Z,111,3"], %Source{}} = Source.call(source)
  end

  test "stream lines", %{source: source} do
    lines =
      source
      |> Source.stream()
      |> Enum.to_list()

    assert Enum.join(lines, "\n") == File.read!("test/data/orders.csv")
  end

  test "stop", %{source: source} do
    assert Source.stop(source) == :ok
    refute Process.alive?(source.pid)
  end
end
