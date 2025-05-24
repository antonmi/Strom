defmodule Strom.MixerTreeTest do
  use ExUnit.Case, async: true

  alias Strom.{Composite, MixerTree}

  test "mixes 5 streams with 2 parts" do
    count = 5
    parts = 2
    names = Enum.map(1..count, &String.to_atom("s#{&1}"))

    mixer_tree =
      names
      |> MixerTree.new(:stream, parts: parts)
      |> Composite.start()

    assert length(Composite.components(mixer_tree)) == 6

    flow =
      names
      |> Enum.reduce(%{}, fn name, acc -> Map.put(acc, name, Enum.to_list(1..10)) end)
      |> Composite.call(mixer_tree)

    assert length(Enum.to_list(flow[:stream])) == count * 10
    Composite.stop(mixer_tree)
  end

  test "mixes random number of streams" do
    count = :rand.uniform(100)

    names = Enum.map(1..count, &String.to_atom("s#{&1}"))

    parts = 5 + :rand.uniform(5)

    mixer_tree =
      names
      |> MixerTree.new(:stream, parts: parts)
      |> Composite.start()

    flow =
      names
      |> Enum.reduce(%{}, fn name, acc -> Map.put(acc, name, Enum.to_list(1..10)) end)
      |> Composite.call(mixer_tree)

    assert length(Enum.to_list(flow[:stream])) == count * 10
    Composite.stop(mixer_tree)
  end
end
