defmodule Strom.Integration.TelegramTest do
  use ExUnit.Case

  defmodule TelegramFlow do
    use Strom.DSL

    alias Strom.Source.ReadLines
    alias Strom.Sink.WriteLines

    defmodule Decompose do
      def start([]), do: nil

      def call(event, nil, []) do
        {String.split(event, ","), nil}
      end

      def stop(nil, []), do: :ok
    end

    defmodule Recompose do
      @length 100

      def start([]), do: []

      def call(event, words, []) do
        line = Enum.join(words, " ")
        new_line = line <> " " <> event

        if String.length(new_line) > @length do
          {[new_line], [event]}
        else
          {[], words ++ [event]}
        end
      end

      def stop(_acc, []), do: :ok
    end

    @topology [
      source(:input, %ReadLines{path: "test/data/orders.csv"}),
      module(:input, Decompose),
      module(:input, Recompose),
      sink(:input, %WriteLines{path: "test_data/telegram.txt"}, true)
    ]
  end

  test "run flow" do
    TelegramFlow.start()
    TelegramFlow.call(%{})
    TelegramFlow.stop()
  end
end
