defmodule Strom.Examples.SimpleNumbersTest do
  use ExUnit.Case

  alias Strom.{Mixer, Splitter, Transformer}

  test "simple numbers" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    mixer = Mixer.start()
    splitter = Splitter.start()

    partitions = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    transformer = Transformer.start()

    %{odd: odd, even: even} =
      flow
      |> Mixer.call(mixer, [:numbers1, :numbers2], :number)
      |> Transformer.call(transformer, :number, &(&1 + 1))
      |> Splitter.call(splitter, :number, partitions)

    assert Enum.sort(Enum.to_list(odd)) == [3, 5, 7, 9, 11]
    assert Enum.sort(Enum.to_list(even)) == [2, 4, 6, 8, 10]
  end

  describe "round robin mixer" do
    defmodule RoundRobin do
      use Strom.DSL

      def add_label(event, label) do
        {[{event, label}], label}
      end

      def call({number, label}, acc) do
        [another] = Enum.reject(Map.keys(acc), &(&1 == label))

        case Map.fetch!(acc, another) do
          [hd | tl] ->
            {[hd, number], Map.put(acc, another, tl)}

          [] ->
            numbers = Map.fetch!(acc, label)
            {[], Map.put(acc, label, numbers ++ [number])}
        end
      end

      def topology(_opts) do
        [
          transform(:first, &__MODULE__.add_label/2, :first),
          transform(:second, &__MODULE__.add_label/2, :second),
          mix([:first, :second], :mixed),
          transform(:mixed, &__MODULE__.call/2, %{first: [], second: []})
        ]
      end
    end

    test "test the order of numbers" do
      RoundRobin.start()

      %{mixed: mixed} =
        %{first: [1, 2, 3], second: [10, 20, 30]}
        |> RoundRobin.call()

      case Enum.to_list(mixed) do
        [1 | rest] ->
          assert rest == [10, 2, 20, 3, 30]

        [10 | rest] ->
          assert rest == [1, 20, 2, 30, 3]
      end
    end
  end

  describe "round robin mixer with many streams" do
    defmodule RoundRobinMany do
      use Strom.DSL

      def add_label(event, label) do
        {[{event, label}], label}
      end

      def call({number, label}, acc) do
        others = Enum.reject(Map.keys(acc), &(&1 == label))

        if Enum.all?(others, &(length(Map.fetch!(acc, &1)) > 0)) do
          Enum.reduce(others, {[number], acc}, fn other, {nums, acc} ->
            [hd | tl] = Map.fetch!(acc, other)
            {[hd | nums], Map.put(acc, other, tl)}
          end)
        else
          numbers = Map.fetch!(acc, label)
          {[], Map.put(acc, label, numbers ++ [number])}
        end
      end

      def topology(names) do
        Enum.map(names, fn name ->
          transform(name, &__MODULE__.add_label/2, name)
        end) ++
          [
            mix(names, :mixed),
            transform(:mixed, &__MODULE__.call/2, Enum.reduce(names, %{}, &Map.put(&2, &1, [])))
          ]
      end
    end

    test "test the order of numbers" do
      RoundRobinMany.start([:first, :second, :third])

      %{mixed: mixed} =
        %{first: [1, 2, 3], second: [10, 20, 30], third: [100, 200, 300]}
        |> RoundRobinMany.call()

      mixed = Enum.to_list(mixed)
      assert length(mixed) == 9

      first = Enum.take(mixed, 3)
      assert Enum.member?(first, 1)
      assert Enum.member?(first, 10)
      assert Enum.member?(first, 100)

      last = Enum.take(Enum.reverse(mixed), 3)
      assert Enum.member?(last, 3)
      assert Enum.member?(last, 30)
      assert Enum.member?(last, 300)
    end
  end
end
