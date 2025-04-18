defmodule Strom.GenMixTest do
  use ExUnit.Case, async: true

  alias Strom.GenMix

  test "start and stop" do
    gen_mix = GenMix.start(%GenMix{})
    assert Process.alive?(gen_mix.pid)
    :ok = GenMix.stop(gen_mix)
    refute Process.alive?(gen_mix.pid)
  end

  test "call one by one" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10], numbers3: [0, 0, 0, 0, 0]}

    inputs = %{
      numbers1: fn el -> el < 5 end,
      numbers2: fn el -> el > 6 end
    }

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix = GenMix.start(%GenMix{inputs: inputs, outputs: outputs})

    flow = GenMix.call(flow, gen_mix)

    assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 8, 10]
    assert Enum.sort(Enum.to_list(flow[:odd])) == [1, 3, 7, 9]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]

    GenMix.stop(gen_mix)
  end

  test "call in a tasks" do
    flow = %{numbers1: Enum.to_list(1..100), numbers2: Enum.to_list(101..200)}
    inputs = %{numbers1: fn _el -> true end, numbers2: fn _el -> true end}

    outputs = %{
      odd: fn el ->
        rem(el, 2) == 1
      end,
      even: fn el ->
        rem(el, 2) == 0
      end
    }

    gen_mix = GenMix.start(%GenMix{inputs: inputs, outputs: outputs})

    flow = GenMix.call(flow, gen_mix)
    task_even = Task.async(fn -> Enum.count(flow[:even]) end)
    task_odd = Task.async(fn -> Enum.count(flow[:odd]) end)
    assert Task.await(task_even) == 100
    assert Task.await(task_odd) == 100
  end

  test "when buffer limit has been reached" do
    flow = %{numbers1: Enum.to_list(1..1_000), numbers2: Enum.to_list(1..1_000)}
    inputs = %{numbers1: fn _el -> true end, numbers2: fn _el -> true end}

    outputs = %{
      odd: fn el ->
        rem(el, 2) == 1
      end,
      even: fn el ->
        rem(el, 2) == 0
      end
    }

    gen_mix =
      GenMix.start(%GenMix{inputs: inputs, outputs: outputs, opts: [chunk: 10, buffer: 100]})

    flow = GenMix.call(flow, gen_mix)

    task_even = Task.async(fn -> Enum.count(flow[:even]) end)
    Process.sleep(50)
    assert length(:sys.get_state(gen_mix.pid).data[:even]) == 0
    assert length(:sys.get_state(gen_mix.pid).data[:odd]) >= 100
    task_odd = Task.async(fn -> Enum.count(flow[:odd]) end)
    assert Task.await(task_even) == 1000
    assert Task.await(task_odd) == 1000
  end

  test "call with one infinite stream" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: Stream.cycle([1, 2, 3])}

    inputs = %{numbers1: fn _el -> true end, numbers2: fn _el -> true end}

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix = GenMix.start(%GenMix{inputs: inputs, outputs: outputs, opts: [no_wait: true]})
    flow = GenMix.call(flow, gen_mix)

    assert Enum.count(flow[:even]) >= 2
    assert Enum.count(flow[:odd]) >= 3
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

    gen_mix = GenMix.start(%GenMix{inputs: inputs, outputs: outputs, opts: [chunk: 1000]})

    flow = GenMix.call(flow, gen_mix)

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

    GenMix.stop(gen_mix)
  end
end
