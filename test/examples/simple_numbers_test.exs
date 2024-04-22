defmodule Strom.Examples.SimpleNumbersTest do
  use ExUnit.Case

  alias Strom.{Composite, Mixer, Splitter, Transformer}

  test "simple numbers" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    mixer =
      [:numbers1, :numbers2]
      |> Mixer.new(:number)
      |> Mixer.start()

    splitter =
      :number
      |> Splitter.new(%{
        odd: fn el -> rem(el, 2) == 1 end,
        even: fn el -> rem(el, 2) == 0 end
      })
      |> Splitter.start()

    transformer =
      :number
      |> Transformer.new(&(&1 + 1))
      |> Transformer.start()

    %{odd: odd, even: even} =
      flow
      |> Mixer.call(mixer)
      |> Transformer.call(transformer)
      |> Splitter.call(splitter)

    assert Enum.sort(Enum.to_list(odd)) == [3, 5, 7, 9, 11]
    assert Enum.sort(Enum.to_list(even)) == [2, 4, 6, 8, 10]
  end

  describe "round robin mixer" do
    defmodule RoundRobin do
      import Strom.DSL

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

      def components() do
        [
          transform(:first, &__MODULE__.add_label/2, :first),
          transform(:second, &__MODULE__.add_label/2, :second),
          mix([:first, :second], :mixed),
          transform(:mixed, &__MODULE__.call/2, %{first: [], second: []})
        ]
      end
    end

    test "test the order of numbers" do
      round_robin =
        RoundRobin.components()
        |> Composite.new()
        |> Composite.start()

      %{mixed: mixed} =
        %{first: [1, 2, 3], second: [10, 20, 30]}
        |> Composite.call(round_robin)

      case Enum.to_list(mixed) do
        [1 | rest] ->
          assert rest == [10, 2, 20, 3, 30]

        [10 | rest] ->
          assert rest == [1, 20, 2, 30, 3]
      end

      Composite.stop(round_robin)
    end
  end

  describe "round robin mixer with many streams" do
    defmodule RoundRobinMany do
      import Strom.DSL

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

      def components(names) do
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
      round_robin_many =
        RoundRobinMany.components([:first, :second, :third])
        |> Composite.new()
        |> Composite.start()

      %{mixed: mixed} =
        %{first: [1, 2, 3], second: [10, 20, 30], third: [100, 200, 300]}
        |> Composite.call(round_robin_many)

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

      Composite.stop(round_robin_many)
    end
  end
end
