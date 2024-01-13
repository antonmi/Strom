defmodule Strom.GenMixTest do
  use ExUnit.Case, async: false

  alias Strom.GenMix

  test "start and stop" do
    {:ok, pid} = GenMix.start(%GenMix{})
    assert Process.alive?(pid)
    :ok = GenMix.stop(pid)
    refute Process.alive?(pid)
  end

  test "call" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}

    inputs = %{
      numbers1: fn el -> el < 5 end,
      numbers2: fn el -> el > 6 end
    }

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix = %GenMix{inputs: inputs, outputs: outputs}

    {:ok, pid} = GenMix.start(gen_mix)

    flow = GenMix.call(flow, pid)

    assert Enum.sort(Enum.to_list(flow[:odd])) == [1, 3, 7, 9]
    assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 8, 10]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]
  end

  test "massive call" do
    #    :observer.start()
    flow = %{
      numbers1: Enum.to_list(1..100_000),
      numbers2: Enum.to_list(1..100_000),
      numbers3: Enum.to_list(1..100_000)
    }

    inputs = %{
      numbers1: fn el -> rem(el, 3) == 0 end,
      numbers2: fn el -> rem(el, 4) == 0 end,
      numbers3: fn el -> rem(el, 5) == 0 end
    }

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix = %GenMix{inputs: inputs, outputs: outputs}
    {:ok, pid} = GenMix.start(gen_mix)

    flow = GenMix.call(flow, pid)

    task1 =
      Task.async(fn ->
        list = Enum.to_list(flow[:odd])
        assert length(list) == 26667
      end)

    task2 =
      Task.async(fn ->
        list = Enum.to_list(flow[:even])
        assert length(list) == 51666
      end)

    Task.await(task1, :infinity)
    Task.await(task2, :infinity)
  end

  test "huge files" do
    #      :observer.start()
    #    source1 =
    #      :source1
    #      |> Strom.Source.new(%Strom.Source.ReadLines{path: "test_data/orders.csv"})
    #      |> Strom.Source.start()
    #
    #    source2 =
    #      :source2
    #      |> Strom.Source.new(%Strom.Source.ReadLines{path: "test_data/parcels.csv"})
    #      |> Strom.Source.start()
    #
    #    sink1 =
    #      :odd
    #      |> Strom.Sink.new(%Strom.Sink.WriteLines{path: "test_data/odd.csv"})
    #      |> Strom.Sink.start()
    #
    #    sink2 =
    #      :even
    #      |> Strom.Sink.new(%Strom.Sink.WriteLines{path: "test_data/even.csv"}, true)
    #      |> Strom.Sink.start()
    #
    #    flow =
    #      %{}
    #      |> Strom.Source.call(source1)
    #      |> Strom.Source.call(source2)
    #
    #    inputs = %{
    #      source1: fn el -> el end,
    #      source2: fn el -> el end
    #    }
    #
    #    outputs = %{
    #      odd: fn el -> rem(el, 2) == 1 end,
    #      even: fn el -> rem(el, 2) == 0 end
    #    }
    #
    #    {:ok, mix1} = GenMix.start(%GenMix{inputs: inputs, outputs: inputs})
    #    {:ok, mix2} = GenMix.start(%GenMix{inputs: inputs, outputs: outputs})
    #
    #    transformer1 =
    #      [:source1, :source2]
    #      |> Strom.Transformer.new(fn el -> String.length(el) end)
    #      |> Strom.Transformer.start()
    #
    #    transformer2 =
    #      [:odd, :even]
    #      |> Strom.Transformer.new(fn el -> "#{el}" end)
    #      |> Strom.Transformer.start()
    #
    #    flow
    #    |> GenMix.call(mix1)
    #    |> Strom.Transformer.call(transformer1)
    #    |> GenMix.call(mix2)
    #    |> Strom.Transformer.call(transformer2)
    #    |> Strom.Sink.call(sink1)
    #    |> Strom.Sink.call(sink2)
  end
end
