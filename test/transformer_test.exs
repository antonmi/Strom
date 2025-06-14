defmodule Strom.TransformerTest do
  use ExUnit.Case, async: true
  import Strom.TestHelper
  doctest Strom.Transformer

  alias Strom.Transformer

  test "start and stop" do
    transformer =
      :stream
      |> Transformer.new(&(&1 + 1))
      |> Transformer.start()

    assert Process.alive?(transformer.pid)
    :ok = Transformer.stop(transformer)
    refute Process.alive?(transformer.pid)
  end

  test "call with one stream" do
    transformer =
      :numbers
      |> Transformer.new(&(&1 * &1))
      |> Transformer.start()

    flow = %{numbers: [1, 2, 3, 4, 5]}
    flow = Transformer.call(flow, transformer)

    assert Enum.sort(Enum.to_list(flow[:numbers])) == [1, 4, 9, 16, 25]
  end

  test "call several transformers with one stream" do
    transformer1 =
      :numbers
      |> Transformer.new(&(&1 + 1))
      |> Transformer.start()

    transformer2 =
      :numbers
      |> Transformer.new(&(&1 + 1))
      |> Transformer.start()

    transformer3 =
      :numbers
      |> Transformer.new(&(&1 + 1))
      |> Transformer.start()

    flow =
      %{numbers: [1, 2, 3, 4, 5]}
      |> Transformer.call(transformer1)
      |> Transformer.call(transformer2)
      |> Transformer.call(transformer3)

    assert Enum.sort(Enum.to_list(flow[:numbers])) == [4, 5, 6, 7, 8]
  end

  test "with chunk and buffer" do
    chunk = Enum.random(1..5)
    buffer = Enum.random(1..5)

    transformer =
      :numbers
      |> Transformer.new(&(&1 * &1), nil, chunk: chunk, buffer: buffer)
      |> Transformer.start()

    flow = %{numbers: [1, 2, 3, 4, 5]}
    flow = Transformer.call(flow, transformer)
    assert Enum.sort(Enum.to_list(flow[:numbers])) == [1, 4, 9, 16, 25]
  end

  test "call with several streams" do
    transformer =
      [:numbers1, :numbers2]
      |> Transformer.new(&(&1 * &1))
      |> Transformer.start()

    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}
    flow = Transformer.call(flow, transformer)

    assert Enum.sort(Enum.to_list(flow[:numbers1])) == [1, 4, 9, 16, 25]
    assert Enum.sort(Enum.to_list(flow[:numbers2])) == [36, 49, 64, 81, 100]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]
  end

  test "call with accumulator" do
    fun = fn el, acc ->
      {[el, acc], acc + 1}
    end

    transformer =
      [:numbers1, :numbers2]
      |> Transformer.new(fun, 100, chunk: 2)
      |> Transformer.start()

    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}

    flow = Transformer.call(flow, transformer)

    assert Enum.sort(Enum.to_list(flow[:numbers1])) == [1, 2, 3, 4, 5, 100, 101, 102, 103, 104]
    assert Enum.sort(Enum.to_list(flow[:numbers2])) == [6, 7, 8, 9, 10, 100, 101, 102, 103, 104]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]

    Transformer.stop(transformer)
  end

  test "two transforemers with different buffer size" do
    max = 1000
    stream1 = build_stream(Enum.to_list(1..max), 0)
    stream2 = build_stream(Enum.to_list(1..max), 0)

    transformer1 =
      [:stream1, :stream2]
      |> Transformer.new(& &1, nil, chunk: 1, buffer: 100)
      |> Transformer.start()

    transformer2 =
      [:stream1, :stream2]
      |> Transformer.new(& &1, nil, chunk: 1, buffer: 1)
      |> Transformer.start()

    flow =
      %{stream1: stream1, stream2: stream2}
      |> Transformer.call(transformer1)
      |> Transformer.call(transformer2)

    task1 = Task.async(fn -> Enum.to_list(flow[:stream1]) end)
    task2 = Task.async(fn -> Enum.to_list(flow[:stream2]) end)

    assert length(Task.await(task1)) == max
    assert length(Task.await(task2)) == max
  end

  test "two transforemers when data fits to buffer" do
    max = 10
    stream1 = build_stream(Enum.to_list(1..max), 0)
    stream2 = build_stream(Enum.to_list(1..max), 0)

    transformer1 =
      [:stream1, :stream2]
      |> Transformer.new(& &1, nil, chunk: 1)
      |> Transformer.start()

    transformer2 =
      [:stream1, :stream2]
      |> Transformer.new(& &1, nil, chunk: 1)
      |> Transformer.start()

    flow =
      %{stream1: stream1, stream2: stream2}
      |> Transformer.call(transformer1)
      |> Transformer.call(transformer2)

    task1 = Task.async(fn -> Enum.to_list(flow[:stream1]) end)
    task2 = Task.async(fn -> Enum.to_list(flow[:stream2]) end)

    assert length(Task.await(task1)) == max
    assert length(Task.await(task2)) == max
  end
end
