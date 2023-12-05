defmodule Strom.LoopTest do
  use ExUnit.Case

  test "loop" do
    flow = %{stream: [1, 2, 3]}

    plus_one =
      Strom.Function.start(&Stream.map(&1, fn el -> el + 1 end))

    mixer = Strom.Mixer.start(chunk_every: 2)
    splitter = Strom.Splitter.start(chunk_every: 2)

    loop = Strom.Loop.start(timeout: 100)
    source_loop = Strom.Source.start(loop)
    sink_loop = Strom.Sink.start(loop)

    flow =
      flow
      |> Strom.Source.call(source_loop, :looped)
      |> Strom.Mixer.call(mixer, [:looped, :stream], :merged)
      |> Strom.Function.call(plus_one, :merged)
      |> Strom.Splitter.call(splitter, :merged, %{
        ok: fn el -> el >= 10 end,
        not_ok: fn el -> el < 10 end
      })
      |> Strom.Sink.call(sink_loop, :not_ok, true)

    assert Enum.to_list(flow[:ok]) == [10, 10, 10]
  end
end
