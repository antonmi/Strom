defmodule Strom.ReplaceTest do
  use ExUnit.Case, async: true

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
      stream = build_stream(Enum.to_list(1..10), 2)
      transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
      transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

      composite =
        [transformer1, transformer2]
        |> Composite.new()
        |> Composite.start()

      %{stream: stream} = Composite.call(%{stream: stream}, composite)

      task = Task.async(fn -> Enum.to_list(stream) end)

      Process.sleep(13)
      composite = Composite.delete(composite, 0)
      components = Composite.components(composite)
      assert length(components) == 1

      list = Task.await(task)
      # one event can be lost, I'll address this later
      assert length(list) >= 9
      assert [11 | _] = list
      assert [10 | _] = Enum.reverse(list)

      Composite.stop(composite)
    end

    test "delete two transformers" do
      stream = build_stream(Enum.to_list(1..10), 2)
      transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
      transformer2 = Transformer.new(:stream, &(&1 + 20), nil, chunk: 1)
      transformer3 = Transformer.new(:stream, &(&1 + 1000), nil, chunk: 1)

      composite =
        [transformer1, transformer2, transformer3]
        |> Composite.new()
        |> Composite.start()

      %{stream: stream} = Composite.call(%{stream: stream}, composite)

      task = Task.async(fn -> Enum.to_list(stream) end)

      Process.sleep(13)
      composite = Composite.delete(composite, 0, 1)
      components = Composite.components(composite)
      assert length(components) == 1

      list = Task.await(task)
      # one event can be lost, I'll address this later
      assert length(list) >= 9
      assert [1031 | _] = list
      assert [1010 | _] = Enum.reverse(list)
    end

    test "when there are two streams" do
      # TODO
    end

    test "delete when there is only one component" do
      # TODO
    end

    test "insert three components between with two others" do
      stream = build_stream(Enum.to_list(1..10), 2)
      transformer1 = Transformer.new(:stream, &(&1 + 10), nil, chunk: 1)
      transformer2 = Transformer.new(:stream, & &1, nil, chunk: 1)

      composite =
        [transformer1, transformer2]
        |> Composite.new()
        |> Composite.start()

      %{stream: stream} = Composite.call(%{stream: stream}, composite)

      task = Task.async(fn -> Enum.to_list(stream) end)

      new_transformer1 = Transformer.new(:stream, &(&1 + 100), nil, chunk: 1)
      new_transformer2 = Transformer.new(:stream, &(&1 + 200), nil, chunk: 1)
      new_transformer3 = Transformer.new(:stream, &(&1 + 300), nil, chunk: 1)

      Process.sleep(15)

      composite =
        Composite.insert(composite, 1, [new_transformer1, new_transformer2, new_transformer3])

      components = Composite.components(composite)
      assert length(components) == 5

      list = Task.await(task)
      assert length(list) == 10
      assert [11 | _] = list
      assert hd(Enum.reverse(list)) == 620
    end
  end
end
