defmodule Strom.FlowTest do
  use ExUnit.Case

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.Sink
  alias Strom.Sink.WriteLines
  alias Strom.{Mixer, Splitter}

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

  describe "check logic" do
    setup do
      source1 = Source.start(%ReadLines{path: "test/data/numbers1.txt"})
      source2 = Source.start(%ReadLines{path: "test/data/numbers2.txt"})
      sink_odd = Sink.start(%WriteLines{path: "test/data/odd.txt"})
      sink_even = Sink.start(%WriteLines{path: "test/data/even.txt"})

      partitions = [&(rem(&1, 2) == 1), &(rem(&1, 2) == 0)]
      Pipeline.start()
      ToString.start()

      on_exit(fn ->
        Pipeline.stop()
        ToString.stop()
      end)

      %{
        source1: source1,
        source2: source2,
        sink_odd: sink_odd,
        sink_even: sink_even,
        partitions: partitions
      }
    end

    test "run pipeline and split events", %{
      source1: source1,
      source2: source2,
      sink_odd: sink_odd,
      sink_even: sink_even,
      partitions: partitions
    } do
      [Source.stream(source1), Source.stream(source2)]
      |> Mixer.start()
      |> Mixer.stream()
      |> Pipeline.stream()
      |> Splitter.start(partitions)
      |> Splitter.stream()
      |> Enum.zip([sink_odd, sink_even])
      |> Enum.map(fn {stream, sink} ->
        Task.async(fn ->
          stream
          |> Stream.map(&"#{&1}")
          |> Sink.stream(sink)
          |> Stream.run()
        end)
      end)
      |> Enum.map(&Task.await/1)

      check_output()
    end
  end

  defmodule MyFlow do
    alias Strom.DSL

    defmodule MyModule do
      defstruct state: nil

      def start(_opts), do: %__MODULE__{}

      def stream(stream, %__MODULE__{}), do: stream

      def stop(%__MODULE__{}), do: :ok
    end

    defmodule ToString do
      defstruct state: nil

      def start(_opts), do: %__MODULE__{}

      def stream(stream, %__MODULE__{}), do: stream

      def stop(%__MODULE__{}), do: :ok
    end

    defmodule Function do
      def call(stream), do: stream
    end

    def topology() do
      source_origin1 = %ReadLines{path: "test/data/numbers1.txt"}
      source_origin2 = %ReadLines{path: "test/data/numbers2.txt"}
      sink_origin_odd = %WriteLines{path: "test/data/odd.txt"}
      sink_origin_even = %WriteLines{path: "test/data/even.txt"}

      odd_fun = &(rem(&1, 2) == 1)
      even_fun = &(rem(&1, 2) == 0)

      [
        %DSL.Mixer{
          sources: [
            [%DSL.Source{origin: source_origin1}, %DSL.Module{module: MyModule}],
            [%DSL.Source{origin: source_origin2}, %DSL.Function{function: &Function.call/1}]
          ]
        },
        %DSL.Module{module: Pipeline},
        %DSL.Splitter{
          branches: %{
            odd_fun => [
              %DSL.Module{module: ToString},
              %DSL.Sink{origin: sink_origin_odd},
              %DSL.Run{}
            ],
            even_fun => [
              %DSL.Function{function: fn stream -> Stream.map(stream, &"#{&1}") end},
              %DSL.Sink{origin: sink_origin_even},
              %DSL.Run{}
            ]
          }
        }
      ]
    end
  end

  test "start and stop" do
    flow = Strom.Flow.start(MyFlow)

    assert %Strom.Flow{
             pid: flow_pid,
             name: Strom.FlowTest.MyFlow,
             streams: [stream1, _stream2],
             modules: [
               "Elixir.Strom.FlowTest.MyFlow.ToString": %Strom.FlowTest.MyFlow.ToString{
                 state: nil
               },
               "Elixir.Strom.FlowTest.Pipeline": :ok,
               "Elixir.Strom.FlowTest.MyFlow.MyModule": %Strom.FlowTest.MyFlow.MyModule{
                 state: nil
               }
             ],
             sources: [
               %Strom.Source{
                 origin: %ReadLines{path: "test/data/numbers2.txt"},
                 pid: source2_pid
               },
               %Strom.Source{
                 origin: %ReadLines{path: "test/data/numbers1.txt"},
                 pid: source1_pid
               }
             ],
             sinks: [
               %Strom.Sink{
                 origin: %WriteLines{path: "test/data/even.txt"},
                 pid: sink_even_pid
               },
               %Strom.Sink{
                 origin: %WriteLines{path: "test/data/odd.txt"},
                 pid: sink_odd_pid
               }
             ],
             mixers: [
               %Strom.Mixer{pid: mixer_pid}
             ],
             splitters: [
               %Strom.Splitter{pid: splitter_pid}
             ]
           } = flow

    [flow_pid, source1_pid, source2_pid, sink_even_pid, sink_odd_pid, mixer_pid, splitter_pid]
    |> Enum.each(fn pid -> assert Process.alive?(pid) end)

    assert is_function(stream1)

    Strom.Flow.stop(MyFlow)

    [flow_pid, source1_pid, source2_pid, sink_even_pid, sink_odd_pid, mixer_pid, splitter_pid]
    |> Enum.each(fn pid -> refute Process.alive?(pid) end)
  end
end
