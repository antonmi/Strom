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
      |> Sink.new(WriteLines.new("test/data/output.csv"), sync: true)
      |> Sink.start()

    %{sink: sink}
  end

  test "start and stop", %{sink: sink} do
    assert Process.alive?(sink.pid)
    assert sink.origin.path == "test/data/output.csv"
    Sink.stop(sink)
    refute Process.alive?(sink.pid)
  end

  test "write stream", %{sink: sink} do
    assert %{} = Sink.call(%{my_stream: ["a", "b", "c", "d"]}, sink)
    assert File.read!("test/data/output.csv") == "a\nb\nc\nd\n"
  end

  test "stream lines", %{sink: sink} do
    assert %{} =
             %{}
             |> Source.call(source())
             |> Sink.call(sink)

    Sink.stop(sink)
    assert File.read!("test/data/orders.csv") <> "\n" == File.read!("test/data/output.csv")
  end
end
