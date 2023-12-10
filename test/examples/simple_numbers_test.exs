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
        def start(_opts), do: %{first: [], second: []}

        def call({number, :first}, acc, _opts) do
          case acc[:second] do
            [hd | tl] ->
              {[hd, number], Map.put(acc, :second, tl)}

            [] ->
              firsts = Map.fetch!(acc, :first)
              {[], Map.put(acc, :first, firsts ++ [number])}
          end
        end

        def call({number, :second}, acc, _opts) do
          case acc[:first] do
            [hd | tl] ->
              {[hd, number], Map.put(acc, :first, tl)}

            [] ->
              seconds = Map.fetch!(acc, :second)
              {[], Map.put(acc, :second, seconds ++ [number])}
          end
        end

        def stop(_acc, _opts), do: :ok
      end

      @topology [
        function(:first, &__MODULE__.add_label/2, :first),
        function(:second, &__MODULE__.add_label/2, :second),
        mixer([:first, :second], :mixed),
        module(:mixed, DoMix)
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
