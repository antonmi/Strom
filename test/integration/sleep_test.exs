defmodule Strom.Integration.SleepTest do
  use ExUnit.Case, async: false

  #  test "sleep in mixer" do
  #    flow = %{s1: Stream.cycle([1, 2, 3]), s2: Stream.cycle([10, 20, 30])}
  #
  #    sleep_fun =
  #      Strom.Function.start(
  #        &Stream.map(&1, fn el ->
  #          Process.sleep(100)
  #          el
  #        end)
  #      )
  #
  #    to_string = Strom.Function.start(&Stream.map(&1, fn el -> "#{el}" end))
  #
  #    sink = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/sleep.txt"})
  #
  #    flow
  #    |> Strom.Function.call(sleep_fun, :s1)
  #    |> Strom.Mixer.call(Strom.Mixer.start(), [:s1, :s2], :stream)
  #    |> Strom.Function.call(to_string, :stream)
  #    |> Strom.Function.call(sleep_fun, :stream)
  #    |> Strom.Sink.call(sink, :stream, true)
  #  end
end
