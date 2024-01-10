defmodule Strom.DSLTest do
  use ExUnit.Case

  alias Strom.Source.ReadLines
  alias Strom.Sink.WriteLines

  defmodule MyFlow do
    use Strom.DSL

    def topology(opts) do
      partitions = %{
        odd: &__MODULE__.odd_fun/1,
        even: &__MODULE__.even_fun/1
      }

      [
        source(:numbers1, %ReadLines{path: "test/data/numbers1.txt"}),
        source(:numbers2, %ReadLines{path: "test/data/numbers2.txt"}),
        mix([:numbers1, :numbers2], :mixed),
        transform(:mixed, &__MODULE__.to_integer/1),
        transform(:mixed, &__MODULE__.add_one/1),
        split(:mixed, partitions),
        transform([:odd, :even], opts[:to_string_fun]),
        sink(:odd, %WriteLines{path: "test/data/odd.txt"}, true),
        sink(:even, %WriteLines{path: "test/data/even.txt"}, true)
      ]
    end

    def odd_fun(event), do: rem(event, 2) == 1

    def even_fun(event), do: rem(event, 2) == 0

    def to_string(el), do: "#{el}"

    def to_integer(event), do: String.to_integer(event)

    def add_one(event), do: event + 1
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

  test "start, run, and stop" do
    MyFlow.start(%{to_string_fun: &MyFlow.to_string/1})
    MyFlow.call(%{})
    check_output()
    MyFlow.stop()
  end

  describe "transform with options" do
    defmodule FlowTransform do
      use Strom.DSL

      def fun(event, acc, opts) do
        {[event + acc + opts[:add]], acc + opts[:add]}
      end

      def topology(_opts) do
        [
          source(:s1, [1, 2, 3]),
          transform(:s1, &__MODULE__.fun/3, 1000, opts: %{add: 1})
        ]
      end
    end

    test "transform with options" do
      FlowTransform.start()
      %{s1: stream} = FlowTransform.call(%{})
      assert Enum.to_list(stream) == [1002, 1004, 1006]
    end
  end

  describe "combining several flows" do
    defmodule Flow1 do
      use Strom.DSL

      def topology(mixed_name) do
        [
          source(:s1, [1, 2, 3]),
          source(:s2, [10, 20, 30]),
          source(:s3, [100, 200, 300]),
          mix([:s1, :s2], mixed_name)
        ]
      end
    end

    defmodule Flow2 do
      use Strom.DSL

      def add_one(el), do: el + 1
      def to_string(el), do: "#{el}"

      def topology(_) do
        [
          transform(:stream1, &__MODULE__.add_one/1),
          transform(:stream2, &__MODULE__.add_one/1)
        ]
      end
    end

    defmodule Flow3 do
      use Strom.DSL

      def add_one(el), do: el + 1
      def to_string(el), do: "#{el}"

      def topology(name) do
        [
          mix([:str1, :str2], name)
        ]
      end
    end

    defmodule ComposedFlow do
      use Strom.DSL

      def topology(name) do
        [
          from(Flow1, :s12),
          rename(%{s12: :stream1, s3: :stream2}),
          from(Flow2),
          rename(%{stream1: :str1, stream2: :str2}),
          from(Flow3, name)
        ]
      end
    end

    test "ComposedFlow" do
      ComposedFlow.start(:mixed)
      %{mixed: stream} = ComposedFlow.call(%{})
      results = Enum.to_list(stream)
      assert length(results) == 9
      assert Enum.sort(results) == [2, 3, 4, 11, 21, 31, 101, 201, 301]
    end
  end
end
