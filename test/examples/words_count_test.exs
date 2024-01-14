defmodule Strom.Examples.WordsCountTest do
  use ExUnit.Case

  alias Strom.Composite

  defmodule WordsCount do
    import Strom.DSL

    alias Strom.Source.ReadLines

    defmodule DoCount do
      def call(:done, acc), do: {[acc], %{}}

      def call(string, acc) do
        acc =
          string
          |> String.downcase()
          |> String.split(~r/[\W]/)
          |> Enum.reduce(acc, fn word, acc ->
            prev = Map.get(acc, word, 0)
            Map.put(acc, word, prev + 1)
          end)

        {[], acc}
      end
    end

    defmodule SumAll do
      def call(:done, acc), do: {[acc], %{}}

      def call(sums, acc) do
        acc =
          sums
          |> Enum.reduce(acc, fn {word, count}, acc ->
            prev = Map.get(acc, word, 0)
            Map.put(acc, word, prev + count)
          end)

        {[], acc}
      end
    end

    def components({file_name, count}) do
      all_names = Enum.map(1..count, &:"lines-#{&1}")

      partitions =
        Enum.reduce(all_names, %{}, fn name, acc ->
          Map.put(acc, name, fn string -> :erlang.phash2(string, count) end)
        end)

      dones =
        Enum.map(all_names, fn name ->
          source(name, [:done])
        end)

      [
        source(:file, ReadLines.new(file_name)),
        split(:file, partitions)
      ] ++
        dones ++
        [
          transform(all_names, &DoCount.call/2, %{}),
          mix(all_names, :mixed),
          source(:mixed, [:done]),
          transform(:mixed, &SumAll.call/2, %{})
        ]
    end
  end

  test "count" do
    words_count = Composite.start(WordsCount.components({"test/data/orders.csv", 1}))

    %{mixed: counts} = Composite.call(%{}, words_count)
    [counts] = Enum.to_list(counts)
    assert counts["00"] == 214
    assert counts["order_created"] == 107

    Composite.stop(words_count)
  end
end
