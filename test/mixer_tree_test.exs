defmodule Strom.MixerTreeTest do
  use ExUnit.Case, async: true

  alias Strom.{Composite, MixerTree, Source}

  @tag timeout: :infinity
  test "messages" do
    count = :rand.uniform(100)

    names = Enum.map(1..count, &String.to_atom("tick#{&1}"))

    sources =
      Enum.map(names, fn name ->
        Source.new(name, [:tick])
      end)

    mixer = MixerTree.new(names, :stream, parts: 5 + :rand.uniform(5))

    composite =
      [sources, mixer]
      |> Composite.new()
      |> Composite.start()

    flow = Composite.call(%{}, composite)

    assert length(Enum.to_list(flow[:stream])) == count
    Composite.stop(composite)
  end
end
