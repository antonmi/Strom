defmodule Strom.Examples.SimpleNumbersTest do
  use ExUnit.Case

  alias Strom.{Mixer, Splitter, Function}

  test "simple numbers" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    mixer = Mixer.start()
    splitter = Splitter.start()

    partitions = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    function = Function.start(&(&1 + 1))

    %{odd: odd, even: even} =
      flow
      |> Mixer.call(mixer, [:numbers1, :numbers2], :number)
      |> Function.call(function, :number)
      |> Splitter.call(splitter, :number, partitions)

    assert Enum.sort(Enum.to_list(odd)) == [3, 5, 7, 9, 11]
    assert Enum.sort(Enum.to_list(even)) == [2, 4, 6, 8, 10]
  end

  describe "round robin mixer" do
    defmodule RoundRobin do
      use Strom.DSL

      def add_label(event, label), do: {event, label}

      defmodule DoMix do
        def start(names) do
          Enum.reduce(names, %{}, &Map.put(&2, &1, []))
        end

        def call({number, label}, acc, names) do
          [another] = Enum.reject(names, &(&1 == label))

          case Map.fetch!(acc, another) do
            [hd | tl] ->
              {[hd, number], Map.put(acc, another, tl)}

            [] ->
              numbers = Map.fetch!(acc, label)
              {[], Map.put(acc, label, numbers ++ [number])}
          end
        end

        def stop(_acc, _opts), do: :ok
      end

      @topology [
        function(:first, &__MODULE__.add_label/2, :first),
        function(:second, &__MODULE__.add_label/2, :second),
        mixer([:first, :second], :mixed),
        module(:mixed, DoMix, [:first, :second])
      ]
    end

    test "test the order of numbers" do
      RoundRobin.start()

      %{mixed: mixed} =
        %{first: [1, 2, 3], second: [10, 20, 30]}
        |> RoundRobin.call()

      assert Enum.to_list(mixed) == [1, 10, 2, 20, 3, 30]
    end
  end
end
