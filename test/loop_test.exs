 defmodule Strom.LoopTest do
  use ExUnit.Case, async: true

  alias Strom.{Loop, Source, Sink, Transformer, Mixer, Splitter}

  setup do
    loop_source =
      :to_loop
      |> Source.new(Loop.new(:the_loop, timeout: 50))
      |> Source.start()

    loop_sink =
      :to_loop
      |> Sink.new(Loop.new(:the_loop), sync: true)
      |> Sink.start()

    mixer =
      [:to_loop, :numbers]
      |> Mixer.new(:numbers)
      |> Mixer.start()

    transformer =
      :numbers
      |> Transformer.new(&(&1 + 1))
      |> Transformer.start()

    splitter =
      :numbers
      |> Splitter.new(%{to_loop: &(&1 < 10), numbers: &(&1 >= 10)})
      |> Splitter.start()

    %{
      loop_source: loop_source,
      loop_sink: loop_sink,
      mixer: mixer,
      transformer: transformer,
      splitter: splitter
    }
  end

  test "test", %{
    loop_source: loop_source,
    loop_sink: loop_sink,
    mixer: mixer,
    transformer: transformer,
    splitter: splitter
  } do
    flow =
      %{numbers: [1, 2, 3, 4, 5]}
      |> Source.call(loop_source)
      |> Mixer.call(mixer)
      |> Transformer.call(transformer)
      |> Splitter.call(splitter)
      |> Sink.call(loop_sink)

    assert Enum.to_list(flow[:numbers]) == [10, 10, 10, 10, 10]
  end
 end
