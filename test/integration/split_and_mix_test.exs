defmodule Strom.Integration.SplitAndMixTest do
  use ExUnit.Case

  alias Strom.{Source, Sink, Mixer, Splitter, Function}
  alias Strom.Sink.WriteLines
  alias Strom.Source.ReadLines

  setup do
    orders_source = Source.start(%ReadLines{path: "test/data/orders.csv"})
    %{orders_source: orders_source}
  end

  test "split and mix", %{orders_source: orders_source} do
    splitter = Splitter.start()
    mixer = Mixer.start()

    function =
      Function.start(fn stream ->
        Stream.map(stream, &"foo-#{&1}")
      end)

    partitions = %{
      "111" => fn el -> String.contains?(el, ",111,") end,
      "222" => fn el -> String.contains?(el, ",222,") end,
      "333" => fn el -> String.contains?(el, ",333,") end
    }

    %{modified: stream} =
      %{}
      |> Source.stream(orders_source, :orders)
      |> Splitter.stream(splitter, :orders, partitions)
      |> Function.stream(function, ["111", "222", "333"])
      |> Mixer.stream(mixer, ["111", "222", "333"], :modified)

    modified = Enum.to_list(stream)
    Enum.each(modified, fn line -> assert String.starts_with?(line, "foo-") end)
    assert length(modified) == length(String.split(File.read!("test/data/orders.csv"), "\n"))
  end

#  @tag timeout: 300_000
#  test "memory" do
#    # add :observer, :runtime_tools, :wx to extra_applications
#    :observer.start()
#
#    source = Source.start(%ReadLines{path: "test_data/input.csv"})
#
#    partitions = %{
#      "111" => fn el -> String.contains?(el, "Z,111,") end,
#      "222" => fn el -> String.contains?(el, "Z,222,") end,
#      "333" => fn el -> String.contains?(el, "Z,333,") end
#    }
#
#    splitter = Splitter.start()
#    mixer = Mixer.start()
#
#    function =
#      Function.start(fn stream ->
#        Stream.map(stream, &"foo-#{&1}")
#      end)
#
#    sink1 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/output_mixed.csv"})
#    sink2 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/output333.csv"})
#
#    %{}
#    |> Source.stream(source, :input)
#    |> Splitter.stream(splitter, :input, partitions)
#    |> Function.stream(function, ["111", "222"])
#    |> Mixer.stream(mixer, ["111", "222"], :mixed)
#    |> Sink.stream(sink1, :mixed)
#    |> Sink.stream(sink2, "333", true)
#  end
end
