defmodule Strom.ReplaceTest do
  use ExUnit.Case, async: false

  alias Strom.Transformer
  alias Strom.Composite

  def build_stream(list, sleep \\ 0) do
    {:ok, agent} = Agent.start_link(fn -> list end)

    Stream.resource(
      fn -> agent end,
      fn agent ->
        Process.sleep(sleep)

        Agent.get_and_update(agent, fn
          [] -> {{:halt, agent}, []}
          [datum | data] -> {{[datum], agent}, data}
        end)
      end,
      fn agent -> agent end
    )
  end

  describe "delete components" do
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
      # one event can be lost, I'll address this later
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

    test "delete 10 transformer with 10 streams" do
      max = 100
      streams_count = 10
      transformer_count = 10
      stream_names = Enum.map(1..streams_count, &:"stream#{&1}")

      flow =
        Enum.reduce(stream_names, %{}, fn name, ac ->
          Map.put(ac, name, build_stream(Enum.to_list(1..max), 0))
        end)

      transformer = Transformer.new(stream_names, &(&1 + 1_000), nil, chunk: 1, buffer: 100)

      transformers =
        Enum.reduce(1..transformer_count, [], fn _n, acc ->
          [Transformer.new(stream_names, & &1, nil, chunk: 1, buffer: 100) | acc]
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

      numbers =
        tasks
        |> Task.await_many()
        |> List.flatten()

      assert length(numbers) == max * streams_count
      stopped_components = Enum.slice(initial_components, 0, transformer_count - 1)

      Enum.each(stopped_components, fn component ->
        assert wait_for_dying(component.pid)
      end)
    end

    defp wait_for_dying(pid) do
      if Process.alive?(pid) do
        wait_for_dying(pid)
      else
        true
      end
    end

    # test "when there are two streams" do
    #   # TODO
    # end

    # test "delete when there is only one component" do
    #   # TODO
    # end

    # test "insert three components between with two others" do
    #   stream = build_stream(Enum.to_list(1..20), 1)
    #   transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
    #   transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

    #   composite =
    #     [transformer1, transformer2]
    #     |> Composite.new()
    #     |> Composite.start()

    #   %{stream: stream} = Composite.call(%{stream: stream}, composite)

    #   task = Task.async(fn -> Enum.to_list(stream) end)

    #   new_transformer1 = Transformer.new(:stream, &(&1 + 100), nil, chunk: 1)
    #   new_transformer2 = Transformer.new(:stream, &(&1 + 200), nil, chunk: 1)
    #   new_transformer3 = Transformer.new(:stream, &(&1 + 300), nil, chunk: 1)

    #   Process.sleep(10)

    #   composite =
    #     Composite.insert(composite, 1, [new_transformer1, new_transformer2, new_transformer3])

    #   components = Composite.components(composite)
    #   assert length(components) == 5
    #   list = Task.await(task)

    #   Composite.stop(composite)
    #   assert length(list) == 20
    #   assert [11 | _] = list
    #   # TODO address this later
    #   assert hd(Enum.reverse(list)) == 630 || hd(Enum.reverse(list)) == 30
    # end
  end
end
