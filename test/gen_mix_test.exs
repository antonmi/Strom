defmodule Strom.GenMixTest do
  use ExUnit.Case, async: true

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
end
