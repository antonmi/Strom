defmodule Strom.Examples.WordsCountTest do
  use ExUnit.Case

  defmodule WordsFlow do
    use Strom.DSL

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

    def topology({file_name, count}) do
      all_names = Enum.map(1..count, &:"lines-#{&1}")

      dones =
        Enum.map(all_names, fn name ->
          source(name, [:done])
        end)

      [
        source(all_names, %ReadLines{path: file_name})
      ] ++
        dones ++
        [
          transform(all_names, &DoCount.call/2, %{}),
          mixer(all_names, :mixed),
          source(:mixed, [:done]),
          transform(:mixed, &SumAll.call/2, %{})
        ]
    end
  end

  test "count" do
    WordsFlow.start({"test/data/orders.csv", 10})

    %{mixed: counts} = WordsFlow.call(%{})
    [counts] = Enum.to_list(counts)
    assert counts["00"] == 214
    assert counts["order_created"] == 107
  end
end
