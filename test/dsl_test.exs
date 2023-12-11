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

    def odd_fun(event), do: rem(event, 2) == 1
    def even_fun(event), do: rem(event, 2) == 0

    def to_string(el), do: "#{el}"

    defmodule ToStringModule do
      def start(:opts), do: :state

      def call(event, :state, :opts), do: "#{event}"

      def stop(:state, :opts), do: :ok
    end

    def topology(opts) do
      source1 = %ReadLines{path: "test/data/numbers1.txt"}
      source2 = %ReadLines{path: "test/data/numbers2.txt"}
      sink_odd = %WriteLines{path: "test/data/odd.txt"}
      sink_even = %WriteLines{path: "test/data/even.txt"}

      partitions = %{
        odd: &__MODULE__.odd_fun/1,
        even: &__MODULE__.even_fun/1
      }

      [
        source(:numbers1, source1),
        source(:numbers2, source2),
        mixer([:numbers1, :numbers2], :mixed),
        module(:mixed, Pipeline, sync: true),
        splitter(:mixed, partitions),
        function([:odd, :even], opts[:to_string_fun]),
        sink(:odd, sink_odd, true),
        sink(:even, sink_even, true)
      ]
    end
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
    MyFlow.start(%{to_string_fun: &MyFlow.to_string/1})
    MyFlow.call(%{})
    check_output()
    MyFlow.stop()
  end
end
