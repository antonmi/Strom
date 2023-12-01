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

    @topology [
      mixer([[source(source1)], [source(source2)]]),
      pipeline(Pipeline),
      splitter(%{
        &__MODULE__.odd_fun/1 => [
          pipeline(ToString),
          sink(sink_odd),
          run()
        ],
        &__MODULE__.even_fun/1 => [
          pipeline(ToString),
          transform(&__MODULE__.to_string/1),
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
                 [
                   %Strom.DSL.Source{
                     origin: %Strom.Source.ReadLines{
                       path: "test/data/numbers1.txt",
                       file: nil,
                       infinite: false
                     }
                   }
                 ],
                 [
                   %Strom.DSL.Source{
                     origin: %Strom.Source.ReadLines{
                       path: "test/data/numbers2.txt",
                       file: nil,
                       infinite: false
                     }
                   }
                 ]
               ]
             },
             %Strom.DSL.Pipeline{pipeline: Pipeline},
             %Strom.DSL.Splitter{
               branches: branches
             }
           ] = MyFlow.topology()

    assert branches[&Strom.DSLTest.MyFlow.even_fun/1] == [
             %Strom.DSL.Pipeline{pipeline: ToString},
             %Strom.DSL.Transform{function: &Strom.DSLTest.MyFlow.to_string/1, module: nil},
             %Strom.DSL.Sink{
               origin: %Strom.Sink.WriteLines{path: "test/data/even.txt", file: nil}
             },
             %Strom.DSL.Run{run: nil}
           ]

    assert branches[&Strom.DSLTest.MyFlow.odd_fun/1] == [
             %Strom.DSL.Pipeline{pipeline: ToString},
             %Strom.DSL.Sink{
               origin: %Strom.Sink.WriteLines{path: "test/data/odd.txt", file: nil}
             },
             %Strom.DSL.Run{run: nil}
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
    MyFlow.run()
    check_output()
    MyFlow.stop()
  end
end
