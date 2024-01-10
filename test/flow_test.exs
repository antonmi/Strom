defmodule Strom.FlowTest do
  use ExUnit.Case

  defmodule MyFlow do
    use Strom.DSL
    alias Strom.Sink.IOPuts

    def topology(_) do
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
        sink(:odd, %IOPuts{}, true),
        sink(:even, %IOPuts{}, true)
      ]
    end
  end

  def check_sup_pid(flow) do
    [source1, source2, mixer, transformer, splitter, sink1, sink2] = flow.topology
    assert source1.source.sup_pid == flow.sup_pid
    assert source2.source.sup_pid == flow.sup_pid
    assert mixer.mixer.sup_pid == flow.sup_pid
    assert transformer.transformer.sup_pid == flow.sup_pid
    assert splitter.splitter.sup_pid == flow.sup_pid
    assert sink1.sink.sup_pid == flow.sup_pid
    assert sink2.sink.sup_pid == flow.sup_pid
  end

  def check_flow_pid(flow) do
    [source1, source2, mixer, transformer, splitter, sink1, sink2] = flow.topology
    assert source1.source.flow_pid == flow.pid
    assert source2.source.flow_pid == flow.pid
    assert mixer.mixer.flow_pid == flow.pid
    assert transformer.transformer.flow_pid == flow.pid
    assert splitter.splitter.flow_pid == flow.pid
    assert sink1.sink.flow_pid == flow.pid
    assert sink2.sink.flow_pid == flow.pid
  end

  def check_alive(flow) do
    [source1, source2, mixer, transformer, splitter, sink1, sink2] = flow.topology
    refute Process.alive?(source1.source.pid)
    refute Process.alive?(source2.source.pid)
    refute Process.alive?(mixer.mixer.pid)
    refute Process.alive?(transformer.transformer.pid)
    refute Process.alive?(splitter.splitter.pid)
    refute Process.alive?(sink1.sink.pid)
    refute Process.alive?(sink2.sink.pid)
  end

  test "test all" do
    flow = MyFlow.start()
    assert is_pid(flow.pid)
    assert is_pid(flow.sup_pid)
    check_sup_pid(flow)
    check_flow_pid(flow)
    MyFlow.call(%{})

    MyFlow.stop()
    check_alive(flow)
  end
end
