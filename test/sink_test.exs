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
    alias Strom.{Mixer, Transformer, Composite}

    defmodule SlowSink do
      @behaviour Strom.Sink

      defstruct []

      @impl true
      def start(%__MODULE__{}), do: %__MODULE__{}

      @impl true
      def stop(%__MODULE__{}), do: %__MODULE__{}

      @impl true
      def call(%__MODULE__{}, _data) do
        Process.sleep(10_000)
        %__MODULE__{}
      end
    end

    def build_composite do
      mixer = Mixer.new([:num1, :num2], :numbers)
      plus_one = Transformer.new(:numbers, &(&1 + 1))
      sink = Sink.new(:numbers, %SlowSink{})

      [mixer, plus_one, sink]
      |> Composite.new()
      |> Composite.start()
    end

    test "slow sink" do
      composite = build_composite()
      flow = %{num1: 1..10_000, num2: 20_000..30_000}
      Composite.call(flow, composite)

      Process.sleep(100)

      mixer = Enum.find(composite.components, &is_struct(&1, Mixer))
      assert length(:sys.get_state(mixer.pid).data[:numbers]) == 1001
      transformer = Enum.find(composite.components, &is_struct(&1, Transformer))
      assert length(:sys.get_state(transformer.pid).data[:numbers]) == 1000
    end
  end
end
