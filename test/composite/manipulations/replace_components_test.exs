defmodule Strom.Composite.Manipulations.ReplaceComponentsTest do
  use ExUnit.Case, async: false
  import Strom.TestHelper
  alias Strom.{Mixer, Splitter, Transformer}
  alias Strom.Composite

  @moduletag timeout: 10_000

  test "insert several parallel components insted of on transofrmer" do
    stream = build_stream(Enum.to_list(1..30), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 100), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)

    task = Task.async(fn -> Enum.to_list(stream) end)

    Process.sleep(10)

    splitter =
      Splitter.new(:stream, %{
        stream1: fn n -> n <= 10 end,
        stream2: fn n -> n > 10 and n <= 20 end,
        stream3: fn n -> n > 20 end
      })

    transformers =
      Enum.map(1..3, fn i ->
        Transformer.new(:"stream#{i}", fn n -> n + 100 end, nil, chunk: 1)
      end)

    mixer = Mixer.new([:stream1, :stream2, :stream3], :stream)

    {composite, %{}} = Composite.replace(composite, 0, [splitter] ++ transformers ++ [mixer])
    components = Composite.components(composite)
    assert length(components) == 6

    list = Task.await(task)
    assert length(list) == 30
    assert Enum.all?(list, &(&1 <= 130))
    assert Enum.all?(list, &(&1 > 100))
  end

  test "replace with invalid indicies" do
    stream = build_stream(Enum.to_list(1..30), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 100), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)

    task = Task.async(fn -> Enum.to_list(stream) end)

    assert {:error, :indicies_not_in_range} = Composite.replace(composite, -1, [])
    assert {:error, :indicies_not_in_range} = Composite.replace(composite, {-1, -2}, [])
    assert {:error, :indicies_not_in_range} = Composite.replace(composite, {1, 0}, [])
    assert {:error, :indicies_not_in_range} = Composite.replace(composite, {0, 5}, [])
    assert {:error, :cannot_replace_last_component} = Composite.replace(composite, 1, [])

    list = Task.await(task)
    assert length(list) == 30
  end
end
