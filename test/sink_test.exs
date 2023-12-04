defmodule Strom.SinkTest do
  use ExUnit.Case, async: true

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.Sink
  alias Strom.Sink.WriteLines

  def source do
    path = "test/data/orders.csv"
    Source.start(%ReadLines{path: path})
  end

  setup do
    sink = Sink.start(%WriteLines{path: "test/data/output.csv"})
    %{sink: sink}
  end

  test "sink init args", %{sink: sink} do
    assert Process.alive?(sink.pid)
    assert sink.origin.path == "test/data/output.csv"
  end

  test "stream lines", %{sink: sink} do
    assert %{another_stream: another_stream} =
             %{}
             |> Source.stream(source(), :my_stream)
             |> Source.stream(source(), :another_stream)
             |> Sink.stream(sink, :my_stream)

    Process.sleep(10)
    lines = Enum.to_list(another_stream)

    assert Enum.join(lines, "\n") <> "\n" == File.read!("test/data/output.csv")
  end

  test "with sync lines", %{sink: sink} do
    assert %{another_stream: another_stream} =
             %{}
             |> Source.stream(source(), :my_stream)
             |> Source.stream(source(), :another_stream)
             |> Sink.stream(sink, :my_stream, true)

    content = File.read!("test/data/output.csv")
    lines = Enum.to_list(another_stream)

    assert Enum.join(lines, "\n") <> "\n" == File.read!("test/data/output.csv")
  end

  test "stop", %{sink: sink} do
    assert Sink.stop(sink) == :ok
    refute Process.alive?(sink.pid)
  end
end
