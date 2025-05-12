defmodule Strom.SourceTest do
  use ExUnit.Case, async: true
  doctest Strom.Source

  alias Strom.Source
  alias Strom.Source.ReadLines

  setup do
    source =
      :my_stream
      |> Source.new(ReadLines.new("test/data/orders.csv"), buffer: 10)
      |> Source.start()

    %{source: source}
  end

  test "source init", %{source: source} do
    assert Process.alive?(source.pid)
    assert source.origin.path == "test/data/orders.csv"
    Source.stop(source)
    refute Process.alive?(source.pid)
    assert source.opts == [buffer: 10]
  end

  test "stream lines", %{source: source} do
    %{my_stream: stream} = Source.call(%{}, source)
    lines = Enum.to_list(stream)
    assert Enum.join(lines, "\n") == File.read!("test/data/orders.csv")
  end

  test "two sources reading the same file", %{source: source} do
    another_source =
      :another_stream
      |> Source.new(ReadLines.new("test/data/orders.csv"))
      |> Source.start()

    %{my_stream: stream, another_stream: another_stream} =
      %{}
      |> Source.call(source)
      |> Source.call(another_source)

    list = Enum.to_list(stream)
    another_list = Enum.to_list(another_stream)

    assert Enum.join(list, "\n") == File.read!("test/data/orders.csv")
    assert Enum.join(another_list, "\n") == File.read!("test/data/orders.csv")
  end

  test "two sources stream into one stream, stream are concatenated" do
    source =
      :my_stream
      |> Source.new([4, 5, 6])
      |> Source.start()

    %{my_stream: stream} = Source.call(%{my_stream: [1, 2, 3]}, source)

    numbers = Enum.to_list(stream)
    assert Enum.sort(numbers) == [1, 2, 3, 4, 5, 6]
  end

  def build_tick_stream() do
    Stream.resource(
      fn -> 0 end,
      fn
        5 -> {:halt, 5}
        counter -> {[:tick], counter + 1}
      end,
      fn counter -> counter end
    )
  end

  test "stream in the source" do
    stream = build_tick_stream()

    source =
      :my_stream
      |> Source.new(stream)
      |> Source.start()

    %{my_stream: my_stream} = Source.call(%{}, source)

    assert Enum.to_list(my_stream) == [:tick, :tick, :tick, :tick, :tick]
  end

  describe "stop" do
    defmodule CustomSource do
      @behaviour Strom.Source

      defstruct agent: nil

      def new(), do: %__MODULE__{}

      @impl true
      def start(%__MODULE__{} = source) do
        {:ok, agent} = Agent.start_link(fn -> Enum.to_list(1..10) end)
        %{source | agent: agent}
      end

      @impl true
      def call(%__MODULE__{agent: agent} = source) do
        Agent.get_and_update(agent, fn
          [] -> {nil, []}
          [datum | data] -> {datum, data}
        end)
        |> case do
          nil -> {:halt, source}
          number -> {[number], source}
        end
      end

      @impl true
      def stop(%__MODULE__{agent: agent} = source) do
        Agent.stop(agent)
        source
      end
    end

    test "it calls stop on the source before exit" do
      %Source{origin: %{agent: agent}} =
        source =
        :my_stream
        |> Source.new(CustomSource.new())
        |> Source.start()

      %{my_stream: my_stream} = Source.call(%{}, source)
      assert Enum.to_list(my_stream) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      Source.stop(source)
      assert wait_for_dying(agent)
    end
  end

  defp wait_for_dying(pid) do
    if Process.alive?(pid) do
      wait_for_dying(pid)
    else
      true
    end
  end
end
