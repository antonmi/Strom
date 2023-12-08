defmodule Strom.SinkTest do
  use ExUnit.Case, async: false

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
             |> Source.call(source(), :my_stream)
             |> Source.call(source(), :another_stream)
             |> Sink.call(sink, :my_stream)

    Process.sleep(100)
    lines = Enum.to_list(another_stream)

    assert Enum.join(lines, "\n") <> "\n" == File.read!("test/data/output.csv")
  end

  test "stream lines to one_sink", %{sink: sink} do
    assert %{} =
             %{}
             |> Source.call(source(), :my_stream)
             |> Source.call(source(), :another_stream)
             |> Sink.call(sink, [:my_stream, :another_stream], true)

    original_size = String.length(File.read!("test/data/orders.csv"))
    output_size = String.length(File.read!("test/data/output.csv"))
    assert 2 * (original_size + 1) == output_size
  end

  test "with sync lines", %{sink: sink} do
    assert %{another_stream: another_stream} =
             %{}
             |> Source.call(source(), :my_stream)
             |> Source.call(source(), :another_stream)
             |> Sink.call(sink, :my_stream, true)

    lines = Enum.to_list(another_stream)

    assert Enum.join(lines, "\n") <> "\n" == File.read!("test/data/output.csv")
  end

  test "stop", %{sink: sink} do
    assert Sink.stop(sink) == :ok
    refute Process.alive?(sink.pid)
  end
end
