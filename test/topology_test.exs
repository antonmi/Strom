defmodule Strom.TopologyTest do
  use ExUnit.Case
  alias Strom.{Mixer, Renamer, Sink, Source, Splitter, Topology, Transformer}
  alias Strom.Sink.Null

  defmodule MyTopology do
    use Strom.DSL
    alias Strom.Sink.Null

    def components do
      odd_even = %{
        odd: &(rem(&1, 2) == 1),
        even: &(rem(&1, 2) == 0)
      }

      [
        source(:s1, [1, 2, 3]),
        source(:s2, [4, 5, 6]),
        mix([:s1, :s2], :s),
        transform(:s, &(&1 + 1)),
        split(:s, odd_even),
        sink(:odd, %Null{})
      ]
    end
  end

  defmodule AnotherTopology do
    use Strom.DSL

    def components do
      [
        split(:numbers, %{more: &(&1 >= 10), less: &(&1 < 10)}),
        sink(:less, %Null{})
      ]
    end
  end

  def check_alive(topology) do
    [source1, source2, mixer, transformer, splitter, sink1] = topology.components
    assert Process.alive?(source1.source.pid)
    assert Process.alive?(source2.source.pid)
    assert Process.alive?(mixer.mixer.pid)
    assert Process.alive?(transformer.transformer.pid)
    assert Process.alive?(splitter.splitter.pid)
    assert Process.alive?(sink1.sink.pid)
  end

  def check_dead(topology) do
    [source1, source2, mixer, transformer, splitter, sink1] = topology.components
    refute Process.alive?(source1.source.pid)
    refute Process.alive?(source2.source.pid)
    refute Process.alive?(mixer.mixer.pid)
    refute Process.alive?(transformer.transformer.pid)
    refute Process.alive?(splitter.splitter.pid)
    refute Process.alive?(sink1.sink.pid)
  end

  describe "using components directly" do
    test "start and stop" do
      odd_even = %{
        odd: &(rem(&1, 2) == 1),
        even: &(rem(&1, 2) == 0)
      }

      components = [
        Source.new(:s1, [1, 2, 3]),
        Source.new(:s2, [4, 5, 6]),
        Mixer.new([:s1, :s2], :s),
        Transformer.new(:s, &(&1 + 1)),
        Splitter.new(:s, odd_even),
        Sink.new(:odd, %Null{})
      ]

      topology = Topology.start(components)
      assert Process.alive?(topology.pid)
      check_alive(topology)

      Topology.stop(topology)
      refute Process.alive?(topology.pid)
      check_dead(topology)
    end
  end

  describe "using dsl" do
    test "start and stop" do
      topology = Topology.start(MyTopology.components())
      assert Process.alive?(topology.pid)
      check_alive(topology)

      Topology.stop(topology)
      refute Process.alive?(topology.pid)
      check_dead(topology)
    end

    test "call" do
      topology = Topology.start(MyTopology.components())
      flow = Topology.call(%{}, topology)
      assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 6]
      Topology.stop(topology)
    end

    test "compose" do
      topology = Topology.start(MyTopology.components())
      another_topology = Topology.start(AnotherTopology.components())
      transformer = Transformer.start()

      flow =
        %{}
        |> Topology.call(topology)
        |> Transformer.call(transformer, :even, &(&1 * 3))
        |> Renamer.call(%{even: :numbers})
        |> Topology.call(another_topology)

      assert Enum.sort(Enum.to_list(flow[:more])) == [12, 18]
      Topology.stop(topology)
      Topology.stop(another_topology)
      Transformer.stop(transformer)
    end
  end

  describe "reuse topologies" do
    defmodule Topology1 do
      use Strom.DSL

      def comps do
        [
          transform(:numbers, &(&1 + 1))
        ]
      end
    end

    defmodule Topology2 do
      use Strom.DSL

      def comps do
        [
          transform(:numbers, &(&1 * 2))
        ]
      end
    end

    test "compose" do
      top11 = Topology.start(Topology1.comps())
      top21 = Topology.start(Topology2.comps())
      top12 = Topology.start(Topology1.comps())
      top22 = Topology.start(Topology2.comps())

      flow =
        %{numbers: [1, 2, 3]}
        |> Topology.call(top11)
        |> Topology.call(top21)
        |> Topology.call(top12)
        |> Topology.call(top22)

      assert Enum.sort(Enum.to_list(flow[:numbers])) == [10, 14, 18]

      Topology.stop(top11)
      Topology.stop(top21)
      Topology.stop(top12)
      Topology.stop(top22)
    end
  end
end
