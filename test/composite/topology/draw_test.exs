defmodule Strom.Composite.Topology.DrawTest do
  use ExUnit.Case, async: false
  alias Strom.{Composite, Mixer, MixerTree, Transformer, Sink, Source, Splitter}
  alias Strom.Composite.Topology

  test "draw example 1" do
    source = Source.new(:stream1, [], label: "Source of stream1")
    transformer1 = Transformer.new(:stream1, & &1, nil, label: "Transformer 1")
    splitter = Splitter.new(:stream1, [:stream4, :stream5], label: "Splitter")
    transformer5 = Transformer.new(:stream5, & &1, nil, label: "Transformer 5")

    transformer2 = Transformer.new(:stream2, & &1)
    mixer = Mixer.new([:stream1, :stream2, :stream3], :stream)
    transformer3 = Transformer.new(:stream4, & &1)
    transformer4 = Transformer.new(:stream, & &1)
    sink1 = Sink.new(:stream, %{__struct__: Sink})
    sink2 = Sink.new(:stream4, %{__struct__: Sink})

    composite =
      [
        source,
        transformer1,
        splitter,
        transformer5,
        transformer2,
        mixer,
        transformer3,
        transformer4,
        sink1,
        sink2
      ]
      |> Composite.new()

    Topology.draw(composite)
  end

  test "draw mixer tree" do
    mixer_tree = MixerTree.new([:s1, :s2, :s3, :s4, :s5, :s6, :s7], :stream, parts: 2)
    transformer = Transformer.new(:stream, & &1)

    composite =
      [mixer_tree, transformer]
      |> Composite.new()

    Topology.draw(composite)
  end

  test "draw example 2" do
    mixer1 = Mixer.new([:s1, :s2], :stream, chunk: 1)
    mixer2 = Mixer.new([:s3, :s4], :stream, chunk: 1)
    transformer = Transformer.new(:stream, & &1)

    composite =
      [mixer1, mixer2, transformer]
      |> Composite.new()

    Topology.draw(composite)
  end
end
