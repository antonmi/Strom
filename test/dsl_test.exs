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

    def to_string(stream), do: Stream.map(stream, &"#{&1}")

    defmodule ToStringModule do
      def start(:opts), do: :state

      def stream(stream, :state), do: Stream.map(stream, &"#{&1}")

      def stop(:state), do: :ok
    end

    @topology [
      mixer([source(source1), source(source2)]),
      module(Pipeline),
      splitter(%{
        &__MODULE__.odd_fun/1 => [
          function(&__MODULE__.to_string/1),
          sink(sink_odd),
          run()
        ],
        &__MODULE__.even_fun/1 => [
          module(ToStringModule, :opts),
          sink(sink_even),
          run()
        ]
      })
    ]
  end

  test "topology" do
    assert [
             %Strom.DSL.Mixer{
               sources: [
                 %Strom.DSL.Source{
                   origin: %Strom.Source.ReadLines{
                     path: "test/data/numbers1.txt",
                     file: nil,
                     infinite: false
                   }
                 },
                 %Strom.DSL.Source{
                   origin: %Strom.Source.ReadLines{
                     path: "test/data/numbers2.txt",
                     file: nil,
                     infinite: false
                   }
                 }
               ]
             },
             %Strom.DSL.Module{module: Pipeline, opts: []},
             %Strom.DSL.Splitter{
               branches: branches
             }
           ] = MyFlow.topology()

    assert branches[&Strom.DSLTest.MyFlow.even_fun/1] == [
             %Strom.DSL.Module{module: Strom.DSLTest.MyFlow.ToStringModule, opts: :opts},
             %Strom.DSL.Sink{
               origin: %Strom.Sink.WriteLines{path: "test/data/even.txt", file: nil}
             },
             %Strom.DSL.Run{run: nil}
           ]

    assert [
             %Strom.DSL.Function{function: function},
             %Strom.DSL.Sink{
               origin: %Strom.Sink.WriteLines{path: "test/data/odd.txt", file: nil}
             },
             %Strom.DSL.Run{run: nil}
           ] = branches[&Strom.DSLTest.MyFlow.odd_fun/1]

    assert is_function(function)
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
    MyFlow.run()
    check_output()
    MyFlow.stop()
  end

  test "start and run stream separately" do
    flow = MyFlow.start()

    lists =
      flow.streams
      |> Enum.map(fn stream ->
        Task.async(fn -> Enum.to_list(stream) end)
      end)
      |> Enum.map(&Task.await/1)
      |> Enum.map(&Enum.sort/1)

    assert Enum.member?(lists, ["2", "4", "6"])
    assert Enum.member?(lists, ["11", "21", "3", "31", "41", "5", "51"])

    check_output()
  end

  describe "connect 2 flows" do
    defmodule AnotherFlow do
      use Strom.DSL

      sink_mixed = %WriteLines{path: "test/data/mixed.txt"}

      @topology [
        flow_source(MyFlow),
        sink(sink_mixed),
        run()
      ]
    end

    test "run mixed" do
      AnotherFlow.start()
      AnotherFlow.run()

      check_output()

      mixed =
        "test/data/mixed.txt"
        |> File.read!()
        |> String.split()
        |> Enum.sort()

      assert mixed == ["11", "2", "21", "3", "31", "4", "41", "5", "51", "6"]
    end
  end
end
