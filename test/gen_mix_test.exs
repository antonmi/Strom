defmodule Strom.GenMixTest do
  use ExUnit.Case, async: true
  import Strom.TestHelper
  alias Strom.GenMix

  @moduletag timeout: 5000

  def build_list_stream(list, sleep \\ 0) do
    Stream.resource(
      fn -> list end,
      fn
        [] ->
          {:halt, []}

        [hd | tl] ->
          Process.sleep(sleep)
          {[hd], tl}
      end,
      fn list -> list end
    )
  end

  test "start and stop" do
    gen_mix = GenMix.start(%GenMix{})
    assert Process.alive?(gen_mix.pid)
    :ok = GenMix.stop(gen_mix)
    refute Process.alive?(gen_mix.pid)
  end

  test "start and stop with simple stream" do
    stream = build_list_stream(Enum.to_list(1..10), 0)
    gen_mix = GenMix.start(%GenMix{inputs: [:stream], outputs: %{stream: & &1}, opts: [chunk: 1]})
    %{stream: stream} = GenMix.call(%{stream: stream}, gen_mix)
    task = Task.async(fn -> Enum.to_list(stream) end)
    :ok = GenMix.stop(gen_mix)
    Task.await(task)
    assert wait_for_dying(gen_mix.pid)
  end

  test "start and stop with many streams when buffer is full" do
    max = 100
    stream_names = Enum.map(1..10, &:"stream#{&1}")

    flow =
      Enum.reduce(stream_names, %{}, fn name, acc ->
        Map.put(acc, name, build_list_stream(Enum.to_list(1..max), 0))
      end)

    outputs =
      Enum.reduce(stream_names, %{}, fn name, acc ->
        Map.put(acc, name, fn _ -> true end)
      end)

    gen_mix =
      GenMix.start(%GenMix{inputs: stream_names, outputs: outputs, opts: [chunk: 1, buffer: 10]})

    flow = GenMix.call(flow, gen_mix)

    numbers =
      stream_names
      |> Enum.map(fn name ->
        Task.async(fn -> Enum.to_list(flow[name]) end)
      end)
      |> Task.await_many()
      |> List.flatten()

    assert length(numbers) == 10_000

    :ok = GenMix.stop(gen_mix)
    assert wait_for_dying(gen_mix.pid)
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

  test "call first one by one and then in a tasks" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    outputs = %{
      odd: fn el ->
        rem(el, 2) == 1
      end,
      even: fn el ->
        rem(el, 2) == 0
      end
    }

    gen_mix = GenMix.start(%GenMix{inputs: [:numbers1, :numbers2], outputs: outputs})

    %{odd: odd, even: even} = GenMix.call(flow, gen_mix)
    assert Enum.sort(Enum.to_list(even)) == [2, 4, 6, 8, 10]
    assert Enum.sort(Enum.to_list(odd)) == [1, 3, 5, 7, 9]

    %{odd: odd, even: even} = GenMix.call(flow, gen_mix)
    task_even = Task.async(fn -> Enum.sort(Enum.to_list(even)) end)
    task_odd = Task.async(fn -> Enum.sort(Enum.to_list(odd)) end)
    assert Task.await(task_even) == [2, 4, 6, 8, 10]
    assert Task.await(task_odd) == [1, 3, 5, 7, 9]
  end

  test "when buffer limit has been reached" do
    flow = %{numbers1: Enum.to_list(1..10), numbers2: Enum.to_list(1..10)}

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
        opts: [chunk: 5, buffer: 3]
      })

    flow = GenMix.call(flow, gen_mix)

    task_even = Task.async(fn -> Enum.count(flow[:even]) end)
    task_odd = Task.async(fn -> Enum.count(flow[:odd]) end)
    assert Task.await(task_even) == 10
    assert Task.await(task_odd) == 10

    assert Process.alive?(gen_mix.pid)

    :ok = GenMix.stop(gen_mix)
    Process.sleep(100)
    refute Process.alive?(gen_mix.pid)
  end

  test "call with one infinite stream and no_wait option" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: Stream.cycle([1, 3, 5])}

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

    %{odd: odd, even: even} = GenMix.call(flow, gen_mix)

    assert Enum.to_list(even) == [2, 4]
    assert Enum.count(odd) >= 0

    # when run in tasks
    %{odd: odd, even: even} = GenMix.call(flow, gen_mix)
    task1 = Task.async(fn -> Enum.to_list(even) end)
    task2 = Task.async(fn -> Enum.count(odd) end)
    assert Task.await(task1) == [2, 4]
    assert Task.await(task2) >= 0
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

  test "two streams with different rates, be sure that quick stream doesn't block the slow one" do
    quick = build_list_stream(List.duplicate([101], 100_000), 0)
    slow = build_list_stream(Enum.to_list(1..10), 1)

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

  describe "call gen_mix several times" do
    test "call to the gen_mix that was already called" do
      max = 1000
      stream1 = build_stream(Enum.to_list(1..max), 0)
      gen_mix = GenMix.start(%GenMix{inputs: [:stream], outputs: %{stream: & &1}})
      %{stream: stream} = GenMix.call(%{stream: stream1}, gen_mix)
      task1 = Task.async(fn -> Enum.to_list(stream) end)

      stream2 = build_stream(Enum.to_list(1..max), 0)
      %{stream: stream} = GenMix.call(%{stream: stream2}, gen_mix)
      task2 = Task.async(fn -> Enum.to_list(stream) end)

      all_events = Task.await_many([task1, task2], 4000) |> List.flatten() |> Enum.sort()

      assert all_events ==
               [Enum.to_list(1..max), Enum.to_list(1..max)] |> List.flatten() |> Enum.sort()
    end
  end
end
