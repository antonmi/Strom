defmodule Strom.CompositeTest do
  use ExUnit.Case
  alias Strom.{Composite, Mixer, Renamer, Sink, Source, Splitter, Transformer}
  alias Strom.Sink.Null

  defmodule MyComposite do
    import Strom.DSL
    alias Strom.Sink.Null

    def components do
      odd_even = %{
        odd: &(rem(&1, 2) == 1),
        even: &(rem(&1, 2) == 0)
      }

      [
        source(:s1, [1, 2, 3]),
        source(:s2, [4, 5, 6]),
        mix([:s1, :s2], :s),
        transform(:s, &(&1 + 1)),
        split(:s, odd_even),
        sink(:odd, %Null{})
      ]
    end
  end

  defmodule AnotherComposite do
    import Strom.DSL

    def components do
      [
        split(:numbers, %{more: &(&1 >= 10), less: &(&1 < 10)}),
        sink(:less, %Null{})
      ]
    end
  end

  def check_alive(composite) do
    [source1, source2, mixer, transformer, splitter, sink1] = composite.components
    assert Process.alive?(source1.pid)
    assert Process.alive?(source2.pid)
    assert Process.alive?(mixer.pid)
    assert Process.alive?(transformer.pid)
    assert Process.alive?(splitter.pid)
    assert Process.alive?(sink1.pid)
  end

  def check_dead(composite) do
    [source1, source2, mixer, transformer, splitter, sink1] = composite.components
    refute Process.alive?(source1.pid)
    refute Process.alive?(source2.pid)
    refute Process.alive?(mixer.pid)
    refute Process.alive?(transformer.pid)
    refute Process.alive?(splitter.pid)
    refute Process.alive?(sink1.pid)
  end

  describe "using components directly" do
    test "start and stop" do
      odd_even = %{
        odd: &(rem(&1, 2) == 1),
        even: &(rem(&1, 2) == 0)
      }

      components = [
        Source.new(:s1, [1, 2, 3]),
        Source.new(:s2, [4, 5, 6]),
        Mixer.new([:s1, :s2], :s),
        Transformer.new(:s, &(&1 + 1)),
        Splitter.new(:s, odd_even),
        Sink.new(:odd, %Null{})
      ]

      composite = Composite.start(components)
      assert Process.alive?(composite.pid)
      check_alive(composite)

      Composite.stop(composite)
      refute Process.alive?(composite.pid)
      check_dead(composite)
    end
  end

  describe "using dsl" do
    test "start and stop" do
      composite = Composite.start(MyComposite.components())
      assert Process.alive?(composite.pid)
      check_alive(composite)

      Composite.stop(composite)
      refute Process.alive?(composite.pid)
      check_dead(composite)
    end

    test "call" do
      composite = Composite.start(MyComposite.components())
      flow = Composite.call(%{}, composite)
      assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 6]
      Composite.stop(composite)
    end

    test "compose" do
      composite = Composite.start(MyComposite.components())
      another_composite = Composite.start(AnotherComposite.components())

      transformer =
        :even
        |> Transformer.new(&(&1 * 3))
        |> Transformer.start()

      renamer =
        %{even: :numbers}
        |> Renamer.new()
        |> Renamer.start()

      flow =
        %{}
        |> Composite.call(composite)
        |> Transformer.call(transformer)
        |> Renamer.call(renamer)
        |> Composite.call(another_composite)

      assert Enum.sort(Enum.to_list(flow[:more])) == [12, 18]
      Composite.stop(composite)
      Composite.stop(another_composite)
      Transformer.stop(transformer)
    end
  end

  describe "reuse topologies" do
    defmodule Composite1 do
      import Strom.DSL

      def comps do
        [
          transform(:numbers, &(&1 + 1))
        ]
      end
    end

    defmodule Composite2 do
      import Strom.DSL

      def comps do
        [
          transform(:numbers, &(&1 * 2))
        ]
      end
    end

    test "compose" do
      comp11 = Composite.start(Composite1.comps())
      comp21 = Composite.start(Composite2.comps())
      comp12 = Composite.start(Composite1.comps())
      comp22 = Composite.start(Composite2.comps())

      flow =
        %{numbers: [1, 2, 3]}
        |> Composite.call(comp11)
        |> Composite.call(comp21)
        |> Composite.call(comp12)
        |> Composite.call(comp22)

      assert Enum.sort(Enum.to_list(flow[:numbers])) == [10, 14, 18]

      Composite.stop(comp11)
      Composite.stop(comp21)
      Composite.stop(comp12)
      Composite.stop(comp22)
    end
  end
end
