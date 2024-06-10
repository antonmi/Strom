defmodule Strom.ExperimentsTest do
  use ExUnit.Case, async: true

  #  alias Strom.{Composite, Transformer}

  #  @tag timeout: :infinity
  #  test "event speed" do
  #    :observer.start()
  #    print_time =
  #      Transformer.new(:stream, fn event ->
  #        IO.inspect(:erlang.system_time(:millisecond))
  #        event
  #      end)
  #
  #    transformer = Transformer.new(:stream, &(&1 + 1))
  #    transformers = List.duplicate(transformer, 200_000)
  #
  #    composite =
  #      [print_time, transformers, print_time]
  #      |> Composite.new()
  #      |> Composite.start()
  #
  #    start = :erlang.system_time(:millisecond)
  #
  #    %{stream: [1]}
  #    |> Composite.call(composite)
  #    |> Map.get(:stream)
  #    |> Enum.to_list()
  #    |> IO.inspect
  #
  #    IO.inspect(:erlang.system_time(:millisecond) - start, label: :total)
  #  end
  #
  #  test "tasks memory consumption" do
  #    :observer.start()
  #    Enum.map(1..1_000_000, fn _i ->
  #      Task.async(fn -> Process.sleep(50000) end)
  #    end)
  #    |> Enum.map(&Task.await(&1, :infinity))
  #  end
end
