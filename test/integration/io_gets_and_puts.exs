defmodule Strom.Integration.IOGetsAndPutsTest do
  use ExUnit.Case

  defmodule SimpleFlow do
    use Strom.DSL

    def hello(stream), do: Stream.map(stream, &"Hello, #{&1}!")

    def topology(_opts) do
      [
        source(%Strom.Source.IOGets{}),
        function(&__MODULE__.hello/1),
        sink(%Strom.Sink.IOPuts{}),
        run()
      ]
    end
  end
end
