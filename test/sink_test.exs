defmodule Strom.SinkTest do
  use ExUnit.Case, async: true
  doctest Strom.Sink

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.Sink
  alias Strom.Sink.WriteLines

  def source do
    :my_stream
    |> Source.new(ReadLines.new("test/data/orders.csv"))
    |> Source.start()
  end

  setup do
    sink =
      :my_stream
      |> Sink.new(WriteLines.new("test/data/output.csv"), true)
      |> Sink.start()

    %{sink: sink}
  end

  test "start and stop", %{sink: sink} do
    assert Process.alive?(sink.pid)
    assert sink.origin.path == "test/data/output.csv"
    Sink.stop(sink)
    refute Process.alive?(sink.pid)
  end

  test "stream lines", %{sink: sink} do
    assert %{} =
             %{}
             |> Source.call(source())
             |> Sink.call(sink)

    Sink.stop(sink)
    assert File.read!("test/data/orders.csv") <> "\n" == File.read!("test/data/output.csv")
  end

  describe "custom sink" do
    alias Strom.{Transformer, Composite}

    defmodule SlowSink do
      @behaviour Strom.Sink

      defstruct continued: false

      @impl true
      def start(%__MODULE__{}), do: %__MODULE__{}

      @impl true
      def stop(%__MODULE__{}), do: %__MODULE__{}

      @impl true
      def call(%__MODULE__{continued: continued}, _data) do
        unless continued do
          receive do
            :continue ->
              :ok
          end
        end

        %__MODULE__{continued: true}
      end
    end

    def build_composite do
      plus_one = Transformer.new([:num1, :num2], &(&1 + 1), nil, buffer: 10)
      sink = Sink.new(:num1, %SlowSink{})

      [plus_one, sink]
      |> Composite.new()
      |> Composite.start()
    end

    test "slow sink" do
      composite = build_composite()
      flow = %{num1: 1..100, num2: 200..300}
      %{num2: num2} = Composite.call(flow, composite)
      Enum.to_list(num2)

      Process.sleep(10)
      transformer = Enum.find(composite.components, &is_struct(&1, Transformer))
      assert length(:sys.get_state(transformer.pid).data[:num1]) >= 10
      assert :sys.get_state(transformer.pid).data[:num2] == []

      sink = Enum.find(composite.components, &is_struct(&1, Sink))
      sink_state = :sys.get_state(sink.pid)
      send(sink_state.task.pid, :continue)

      Process.sleep(10)
      assert :sys.get_state(transformer.pid).data[:num1] == []
      Composite.stop(composite)
    end
  end
end
