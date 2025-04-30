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

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix = GenMix.start(%GenMix{inputs: [:numbers1, :numbers2], outputs: outputs})

    flow = GenMix.call(flow, gen_mix)

    assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 6, 8, 10]
    assert Enum.sort(Enum.to_list(flow[:odd])) == [1, 3, 5, 7, 9]
    assert Enum.sort(Enum.to_list(flow[:numbers3])) == [0, 0, 0, 0, 0]

    GenMix.stop(gen_mix)
  end

  test "call in a tasks" do
    flow = %{numbers1: Enum.to_list(1..100), numbers2: Enum.to_list(101..200)}

    outputs = %{
      odd: fn el ->
        rem(el, 2) == 1
      end,
      even: fn el ->
        rem(el, 2) == 0
      end
    }

    gen_mix = GenMix.start(%GenMix{inputs: [:numbers1, :numbers2], outputs: outputs})

    flow = GenMix.call(flow, gen_mix)
    task_even = Task.async(fn -> Enum.count(flow[:even]) end)
    task_odd = Task.async(fn -> Enum.count(flow[:odd]) end)
    assert Task.await(task_even) == 100
    assert Task.await(task_odd) == 100
  end

  test "when buffer limit has been reached" do
    flow = %{numbers1: Enum.to_list(1..1_000), numbers2: Enum.to_list(1..1_000)}

    outputs = %{
      odd: fn el ->
        rem(el, 2) == 1
      end,
      even: fn el ->
        rem(el, 2) == 0
      end
    }

    gen_mix =
      GenMix.start(%GenMix{
        inputs: [:numbers1, :numbers2],
        outputs: outputs,
        opts: [chunk: 10, buffer: 100]
      })

    flow = GenMix.call(flow, gen_mix)

    task_even = Task.async(fn -> Enum.count(flow[:even]) end)
    Process.sleep(50)
    assert :sys.get_state(gen_mix.pid).data[:even] == []
    assert length(:sys.get_state(gen_mix.pid).data[:odd]) >= 100
    task_odd = Task.async(fn -> Enum.count(flow[:odd]) end)
    assert Task.await(task_even) == 1000
    assert Task.await(task_odd) == 1000
  end

  test "call with one infinite stream" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: Stream.cycle([1, 2, 3])}

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix =
      GenMix.start(%GenMix{
        inputs: [:numbers1, :numbers2],
        outputs: outputs,
        opts: [no_wait: true]
      })

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

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    gen_mix =
      GenMix.start(%GenMix{
        inputs: [:numbers1, :numbers2, :numbers3],
        outputs: outputs,
        opts: [chunk: 1000]
      })

    flow = GenMix.call(flow, gen_mix)

    task1 =
      Task.async(fn ->
        list = Enum.to_list(flow[:odd])
        assert length(list) == 150_000
      end)

    task2 =
      Task.async(fn ->
        list = Enum.to_list(flow[:even])
        assert length(list) == 150_000
      end)

    Task.await(task1, :infinity)
    Task.await(task2, :infinity)

    GenMix.stop(gen_mix)
  end

  def factorial(n) do
    Enum.reduce(1..n, 1, &(&1 * &2))
  end

  def build_stream(list, sleep \\ 0) do
    Stream.resource(
      fn -> list end,
      fn
        [] ->
          {:halt, []}

        [hd | tl] ->
          Process.sleep(sleep)
          {[hd], tl}
      end,
      fn [] -> [] end
    )
  end

  test "two streams with different rates, be sure that quick stream doesn't block the slow one" do
    quick = Stream.cycle([11])
    #    quick = build_stream(List.duplicate([101], 10000), 1)
    slow = build_stream(Enum.to_list(1..10), 1)

    outputs = %{
      quick: fn n -> n > 10 end,
      slow: fn n -> n <= 10 end
    }

    flow = %{quick: quick, slow: slow}

    gen_mix =
      GenMix.start(%GenMix{
        inputs: [:quick, :slow],
        outputs: outputs,
        opts: [chunk: 2, buffer: 3, no_wait: true]
      })

    flow = GenMix.call(flow, gen_mix)

    Task.async(fn ->
      Enum.to_list(flow[:quick])
    end)

    slow_list = Enum.to_list(flow[:slow])
    assert length(slow_list) == 10
  end

  test "call to the gen_mix that was already called" do
    gen_mix = GenMix.start(%GenMix{inputs: [:stream], outputs: %{stream: & &1}})
    GenMix.call(%{stream: [1, 2, 3]}, gen_mix)

    assert_raise RuntimeError, "Compoment has been already called", fn ->
      GenMix.call(%{stream: [4, 5, 6]}, gen_mix)
    end
  end
end
