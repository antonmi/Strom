defmodule Strom.Examples.WordsCountTest do
  use ExUnit.Case

  defmodule WordsFlow do
    use Strom.DSL

    alias Strom.Source.{ReadLines, Events}

    defmodule DoCount do
      def start(_opts), do: %{}

      def call(:done, acc, _), do: {[acc], %{}}

      def call(string, acc, _) do
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

      def stop(_acc, _opts), do: :ok
    end

    defmodule SumAll do
      def start(_opts), do: %{}

      def call(:done, acc, _), do: {[acc], %{}}

      def call(sums, acc, _) do
        acc =
          sums
          |> Enum.reduce(acc, fn {word, count}, acc ->
            prev = Map.get(acc, word, 0)
            Map.put(acc, word, prev + count)
          end)

        {[], acc}
      end

      def stop(_acc, _opts), do: :ok
    end

    def topology({file_name, count}) do
      all_names = Enum.map(1..count, &:"lines-#{&1}")

      dones =
        Enum.map(all_names, fn name ->
          source(name, %Events{events: [:done]})
        end)

      [
        source(all_names, %ReadLines{path: file_name})
      ] ++
        dones ++
        [
          module(all_names, DoCount),
          mixer(all_names, :mixed),
          source(:mixed, %Events{events: [:done]}),
          module(:mixed, SumAll)
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
