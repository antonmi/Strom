defmodule Strom.Integration.TelegramTest do
  use ExUnit.Case

  alias Strom.Composite

  defmodule Telegram do
    import Strom.DSL

    alias Strom.Source.ReadLines
    alias Strom.Sink.WriteLines

    defmodule Decompose do
      def call(event, nil) do
        {String.split(event, ","), nil}
      end
    end

    defmodule Recompose do
      @length 100

      def call(event, words) do
        line = Enum.join(words, " ")
        new_line = line <> " " <> event

        if String.length(new_line) > @length do
          {[new_line], [event]}
        else
          {[], words ++ [event]}
        end
      end
    end

    def components do
      [
        source(:input, ReadLines.new("test/data/orders.csv")),
        transform(:input, &Decompose.call/2, nil),
        transform(:input, &Recompose.call/2, []),
        sink(:input, WriteLines.new("test/data/telegram.txt"), sync: true)
      ]
    end
  end

  test "run flow" do
    telegram =
      Telegram.components()
      |> Composite.new()
      |> Composite.start()

    Composite.call(%{}, telegram)
    Composite.stop(telegram)
  end
end
