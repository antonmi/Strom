defmodule Strom.TransformerTest do
  use ExUnit.Case, async: false

  alias Strom.Transformer

  test "start and stop" do
    transformer = Transformer.start()
    assert Process.alive?(transformer.pid)
    :ok = Transformer.stop(transformer)
    refute Process.alive?(transformer.pid)
  end

  test "call" do
    transformer = Transformer.start()
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}
    fun = &(&1 * &1)
    flow = Transformer.call(flow, transformer, [:numbers1, :numbers2], fun)

    assert Enum.sort(Enum.to_list(flow[:numbers1])) == [1, 4, 9, 16, 25]
    assert Enum.sort(Enum.to_list(flow[:numbers2])) == [36, 49, 64, 81, 100]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]
  end

  test "call with one stream" do
    transformer = Transformer.start()
    flow = %{numbers1: [1, 2, 3, 4, 5]}
    flow = Transformer.call(flow, transformer, :numbers1, &(&1 * &1))
    assert Enum.sort(Enum.to_list(flow[:numbers1])) == [1, 4, 9, 16, 25]
  end

  test "call with accumulator" do
    transformer = Transformer.start()

    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}

    fun = fn el, acc ->
      {[el, acc], acc + 1}
    end

    flow = Transformer.call(flow, transformer, [:numbers1, :numbers2], {fun, 100})

    assert Enum.sort(Enum.to_list(flow[:numbers1])) == [1, 2, 3, 4, 5, 100, 101, 102, 103, 104]
    assert Enum.sort(Enum.to_list(flow[:numbers2])) == [6, 7, 8, 9, 10, 100, 101, 102, 103, 104]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]
  end

  test "call with opts and accumulator" do
    transformer = Transformer.start(opts: %{add: 1})

    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}

    fun = fn el, acc, opts ->
      {[el, acc], acc + opts[:add]}
    end

    flow = Transformer.call(flow, transformer, [:numbers1, :numbers2], {fun, 100})

    assert Enum.sort(Enum.to_list(flow[:numbers1])) == [1, 2, 3, 4, 5, 100, 101, 102, 103, 104]
    assert Enum.sort(Enum.to_list(flow[:numbers2])) == [6, 7, 8, 9, 10, 100, 101, 102, 103, 104]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]
  end
end
