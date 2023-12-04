defmodule Strom.Examples.SimpleNumbersTest do
  use ExUnit.Case

  alias Strom.{Mixer, Splitter, Function}

  test "simple numbers" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    mixer = Mixer.start()
    splitter = Splitter.start()

    partitions = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    function =
      Function.start(fn stream ->
        Stream.map(stream, &(&1 + 1))
      end)

    %{odd: odd, even: even} =
      flow
      |> Mixer.stream(mixer, [:numbers1, :numbers2], :number)
      |> Function.stream(function, :number)
      |> Splitter.stream(splitter, :number, partitions)

    assert Enum.sort(Enum.to_list(odd)) == [3, 5, 7, 9, 11]
    assert Enum.sort(Enum.to_list(even)) == [2, 4, 6, 8, 10]
  end

#  test "another example" do
#    flow = %{stream: Stream.cycle([1,2,3,4])}
#    splitter = Strom.Splitter.start()
#    flow = Strom.Splitter.stream(flow, splitter, :stream, %{odd: fn el -> rem(el, 2) == 1 end, even: fn el -> rem(el, 2) == 0 end})
#    function = Strom.Function.start(fn stream ->
#      Stream.map(stream, fn el ->
#        Process.sleep(100)
#        "#{el}-foo"
#      end)
#    end)
#    flow = Strom.Function.stream(flow, function, [:odd, :even])
#    sink_odd = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/odd.txt"})
#    sink_even = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/even.txt"})
#    flow = Strom.Sink.stream(flow, sink_odd, :odd)
#    flow = Strom.Sink.stream(flow, sink_even, :even)
#  end
end
