defmodule Strom.DeleteComponentsTest do
  use ExUnit.Case, async: false
  import Strom.TestHelper
  alias Strom.Transformer
  alias Strom.Composite

  @moduletag timeout: 10_000

  test "delete one transformer" do
    stream = build_stream(Enum.to_list(1001..1020), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)
    task = Task.async(fn -> Enum.to_list(stream) end)

    Process.sleep(10)
    composite = Composite.delete(composite, 0)
    components = Composite.components(composite)
    assert length(components) == 1

    list = Task.await(task)

    Composite.stop(composite)
    assert length(list) == 20
    assert [1011 | _] = list
    assert [1020 | _] = Enum.reverse(list)
  end

  test "delete one transformer with sleep 0" do
    stream = build_stream(Enum.to_list(10_001..20_000), 0)
    transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)

    task = Task.async(fn -> Enum.to_list(stream) end)

    Process.sleep(1)
    composite = Composite.delete(composite, 0)
    components = Composite.components(composite)
    assert length(components) == 1

    list = Task.await(task)

    Composite.stop(composite)
    assert length(list) == 10_000
    assert [10_011 | _] = list
    assert [20_000 | _] = Enum.reverse(list)
  end

  test "delete one transformer with sleep 0 when processing several streams" do
    stream1 = build_stream(Enum.to_list(10_001..20_000), 0)
    stream2 = build_stream(Enum.to_list(20_001..30_000), 0)
    stream3 = build_stream(Enum.to_list(30_001..40_000), 0)

    transformer1 = Transformer.new([:stream1, :stream2, :stream3], &(&1 + 10), nil, chunk: 100)
    transformer2 = Transformer.new([:stream1, :stream2, :stream3], & &1, nil, chunk: 100)

    composite =
      [transformer1, transformer2]
      |> Composite.new()
      |> Composite.start()

    %{stream1: stream1, stream2: stream2, stream3: stream3} =
      Composite.call(%{stream1: stream1, stream2: stream2, stream3: stream3}, composite)

    task1 = Task.async(fn -> Enum.to_list(stream1) end)
    task2 = Task.async(fn -> Enum.to_list(stream2) end)
    task3 = Task.async(fn -> Enum.to_list(stream3) end)

    Process.sleep(1)
    composite = Composite.delete(composite, 0)

    components = Composite.components(composite)
    assert length(components) == 1

    list1 = Task.await(task1)
    list2 = Task.await(task2)
    list3 = Task.await(task3)

    Composite.stop(composite)
    assert length(list1) == 10_000
    # assert [10_011 | _] = list1
    # assert [20_000 | _] = Enum.reverse(list1)
    assert length(list2) == 10_000
    # assert [20_011 | _] = list2
    # assert [30_000 | _] = Enum.reverse(list2)
    assert length(list3) == 10_000
    # assert [30_011 | _] = list3
    # assert [40_000 | _] = Enum.reverse(list3)
  end

  test "delete two transformers" do
    stream = build_stream(Enum.to_list(1..10), 1)
    transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    transformer2 = Transformer.new(:stream, &(&1 + 20), nil, chunk: 1)
    transformer3 = Transformer.new(:stream, &(&1 + 1000), nil, chunk: 1)

    composite =
      [transformer1, transformer2, transformer3]
      |> Composite.new()
      |> Composite.start()

    %{stream: stream} = Composite.call(%{stream: stream}, composite)

    task = Task.async(fn -> Enum.to_list(stream) end)

    Process.sleep(7)
    composite = Composite.delete(composite, 0, 1)
    components = Composite.components(composite)
    assert length(components) == 1

    list = Task.await(task)
    Composite.stop(composite)
    assert length(list) == 10
    assert [1031 | _] = list
    assert [1010 | _] = Enum.reverse(list)
  end

  test "delete several transformer with several streams" do
    max = Enum.random(10..100)
    streams_count = Enum.random(5..5)
    transformer_count = Enum.random(2..2)
    stream_names = Enum.map(1..streams_count, &:"stream#{&1}")

    flow =
      Enum.reduce(stream_names, %{}, fn name, ac ->
        Map.put(ac, name, build_stream(Enum.to_list(1..max), 0))
      end)

    transformer = Transformer.new(stream_names, &(&1 + 1_000), nil, chunk: 1, buffer: 1)

    transformers =
      Enum.reduce(1..transformer_count, [], fn _n, acc ->
        [Transformer.new(stream_names, & &1, nil, chunk: 1, buffer: 10) | acc]
      end)

    composite =
      [transformer | transformers]
      |> Enum.reverse()
      |> Composite.new()
      |> Composite.start()

    initial_components = Composite.components(composite)
    flow = Composite.call(flow, composite)

    tasks =
      Enum.map(stream_names, fn name ->
        Task.async(fn -> Enum.to_list(flow[name]) end)
      end)

    Process.sleep(10)

    composite = Composite.delete(composite, 0, transformer_count - 1)
    components_after = Composite.components(composite)
    assert length(components_after) == 1

    try do
      numbers =
        tasks
        |> Task.await_many(1000)
        |> List.flatten()

      assert length(numbers) == max * streams_count
    catch
      :exit, error ->
        IO.inspect(error)
        IO.inspect({streams_count, transformer_count, max})
    end

    stopped_components = Enum.slice(initial_components, 0, transformer_count - 1)

    Enum.each(stopped_components, fn component ->
      assert wait_for_dying(component.pid)
    end)

    Composite.stop(composite)
  end
end
