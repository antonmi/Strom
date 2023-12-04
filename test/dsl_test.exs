defmodule Strom.DSLTest do
  use ExUnit.Case

  alias Strom.Source.ReadLines
  alias Strom.Sink.WriteLines

  defmodule Pipeline do
    use ALF.DSL

    @components [
      stage(:to_integer),
      stage(:add_one)
    ]

    def to_integer(event, _), do: String.to_integer(event)
    def add_one(event, _), do: event + 1
  end

  defmodule ToString do
    use ALF.DSL

    @components [
      stage(:to_string)
    ]

    def to_string(event, _), do: "#{event}"
  end

  defmodule MyFlow do
    use Strom.DSL

    source1 = %ReadLines{path: "test/data/numbers1.txt"}
    source2 = %ReadLines{path: "test/data/numbers2.txt"}
    sink_odd = %WriteLines{path: "test/data/odd.txt"}
    sink_even = %WriteLines{path: "test/data/even.txt"}

    def odd_fun(event), do: rem(event, 2) == 1
    def even_fun(event), do: rem(event, 2) == 0

    partitions = %{
      odd: &__MODULE__.odd_fun/1,
      even: &__MODULE__.even_fun/1
    }

    def to_string(stream), do: Stream.map(stream, &"#{&1}")

    defmodule ToStringModule do
      def start(:opts), do: :state

      def stream(stream, :state), do: Stream.map(stream, &"#{&1}")

      def stop(:state), do: :ok
    end

    @topology [
      source(source1, :numbers1),
      source(source2, :numbers2),
      mixer([:numbers1, :numbers2], :mixed),
      module(Pipeline, :mixed),
      splitter(:mixed, partitions),
      function(&__MODULE__.to_string/1, [:odd, :even]),
      sink(sink_odd, :odd),
      sink(sink_even, :even, true)
    ]
  end

  def check_output() do
    even =
      "test/data/even.txt"
      |> File.read!()
      |> String.split()
      |> Enum.sort()

    assert even == ["2", "4", "6"]

    odd =
      "test/data/odd.txt"
      |> File.read!()
      |> String.split()
      |> Enum.sort()

    assert odd == ["11", "21", "3", "31", "41", "5", "51"]
  end

  test "start and run" do
    MyFlow.start()
    MyFlow.run(%{})
    check_output()
    MyFlow.stop()
  end
end
