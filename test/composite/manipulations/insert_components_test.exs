defmodule Strom.Composite.Manipulations.InsertComponentsTest do
  use ExUnit.Case, async: false
  import Strom.TestHelper
  alias Strom.{Splitter, Transformer}
  alias Strom.Composite
  alias Strom.GenMix

  @moduletag timeout: 10_000

  test "insert three components between with two others" do
    stream = build_stream(Enum.to_list(1..20), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)

    [_transformer1, transformer2] = Composite.components(composite)
    old_task_pid = hd(Map.keys(GenMix.state(transformer2.pid).tasks))

    task = Task.async(fn -> Enum.to_list(stream) end)

    new_transformer1 = Transformer.new(:stream, &(&1 + 100), nil, chunk: 1)
    new_transformer2 = Transformer.new(:stream, &(&1 + 200), nil, chunk: 1)
    new_transformer3 = Transformer.new(:stream, &(&1 + 300), nil, chunk: 1)

    Process.sleep(10)

    {composite, %{}} =
      Composite.insert(composite, 1, [new_transformer1, new_transformer2, new_transformer3])

    components = Composite.components(composite)

    assert length(components) == 5
    assert wait_for_dying(old_task_pid)

    list = Task.await(task)

    Composite.stop(composite)
    assert length(list) == 20
    assert [11 | _] = list
    assert hd(Enum.reverse(list)) == 630
  end

  test "when inserted subflow leaves extra strem" do
    stream = build_stream(Enum.to_list(1..20), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)
    task = Task.async(fn -> Enum.to_list(stream) end)

    Process.sleep(10)
    splitter = Splitter.new(:stream, %{stream: fn _ -> true end, stream2: fn _ -> true end})
    {composite, subflow} = Composite.insert(composite, 1, [splitter])
    components = Composite.components(composite)
    assert length(components) == 3

    list = Task.await(task)
    assert length(list) == 20
    assert Enum.count(subflow[:stream2]) > 0
  end

  test "insert with invalid indicies  " do
    stream = build_stream(Enum.to_list(1..20), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)
    task = Task.async(fn -> Enum.to_list(stream) end)

    assert {:error, :cannot_replace_last_component} = Composite.insert(composite, 2, [])
    assert {:error, :indicies_not_in_range} = Composite.insert(composite, -1, [])
    assert {:error, :indicies_not_in_range} = Composite.insert(composite, 5, [])

    list = Task.await(task)
    assert length(list) == 20
  end

  def func(event, position) do
    if event < 3 do
      composite = %Composite{name: :composite}

      {_, _} =
        Composite.insert(composite, position + 2, [
          Transformer.new(:stream, &func/2, position + 2, chunk: 1),
          Transformer.new(:stream, & &1, nil, chunk: 1)
        ])

      {[event + 1, event + 1], position}
    else
      {[event], position}
    end
  end

  test "insert on the fly" do
    transformer1 = Transformer.new(:stream, &func/2, 0, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)
    transformer3 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2, transformer3]
      |> Composite.new(:composite)
      |> Composite.start()

    stream = build_stream(Enum.to_list(1..1), 1)
    %{stream: stream} = Composite.call(%{stream: stream}, composite)

    assert Enum.to_list(stream) == [2, 3, 3]
    assert length(Composite.components(composite)) == 7
  end
end
