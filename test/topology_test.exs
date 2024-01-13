defmodule Strom.TopologyTest do
  use ExUnit.Case
  alias Strom.{Mixer, Sink, Source, Splitter, Transformer}
  alias Strom.Sink.Null

  defmodule MyTopology do
    use Strom.Topology

    def topology(_opts) do
      [
        Source.new(:s1, [1, 2, 3]),
        Source.new(:s2, [4, 5, 6]),
        Mixer.new([:s1, :s2], :s),
        Transformer.new(:s, &(&1 + 1)),
        Splitter.new(:s, %{odd: &(rem(&1, 2) == 1), even: &(rem(&1, 2) == 0)}),
        Sink.new(:odd, %Null{})
      ]
    end
  end

  describe "start, stop, call" do
    def check_alive(topology) do
      [source1, source2, mixer, transformer, splitter, sink1] = topology.components
      assert Process.alive?(source1.pid)
      assert Process.alive?(source2.pid)
      assert Process.alive?(mixer.pid)
      assert Process.alive?(transformer.pid)
      assert Process.alive?(splitter.pid)
      assert Process.alive?(sink1.pid)
    end

    def check_dead(topology) do
      [source1, source2, mixer, transformer, splitter, sink1] = topology.components
      refute Process.alive?(source1.pid)
      refute Process.alive?(source2.pid)
      refute Process.alive?(mixer.pid)
      refute Process.alive?(transformer.pid)
      refute Process.alive?(splitter.pid)
      refute Process.alive?(sink1.pid)
    end

    test "start and stop" do
      topology = MyTopology.start()
      assert Process.alive?(topology.pid)
      check_alive(topology)

      :ok = MyTopology.stop()
      refute Process.alive?(topology.pid)
      check_dead(topology)
    end

    test "call" do
      MyTopology.start()
      %{even: even} = MyTopology.call(%{})
      assert Enum.sort(Enum.to_list(even)) == [2, 4, 6]
      MyTopology.stop()
    end
  end

  describe "crashes" do
    setup do
      topology = MyTopology.start()
      on_exit(fn -> MyTopology.stop() end)
      %{topology: topology}
    end

    test "kill component" do
      [source1 | _] = MyTopology.components()
      %{even: even} = MyTopology.call(%{})
      assert Enum.sort(Enum.to_list(even)) == [2, 4, 6]

      Process.exit(source1.pid, :kill)
      Process.sleep(1)
      [source1 | _] = MyTopology.components()
      topology = MyTopology.info()
      assert Process.alive?(topology.pid)
      assert Process.alive?(source1.pid)

      %{even: even} = MyTopology.call(%{})
      assert Enum.sort(Enum.to_list(even)) == [2, 4, 6]
    end
  end
end
