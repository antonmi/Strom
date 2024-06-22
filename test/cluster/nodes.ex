defmodule Strom.Cluster.Nodes do
  alias Strom.{Composite, Transformer, Source, Sink, Sink.Heap}

  alias Strom.{Socket, Plug}

  def composite1 do
    source = Source.new(:numbers, [1, 2, 3, 4])
    plus_one = Transformer.new(:numbers, &(&1 + 1))

    Composite.new([source, plus_one])
  end

  def composite2 do
    mult_two = Transformer.new(:numbers, &(&1 * 2))
    sink = Sink.new(:numbers, %Heap{}, true)

    Composite.new([mult_two, sink])
  end

  def node1 do
    composite = Composite.start(composite1())

    socket = Socket.new(:numbers) |> Socket.start()

    %{}
    |> Composite.call(composite)
    |> Socket.call(socket)
  end

  def node2 do
    composite = Composite.start(composite2())

    plug = Plug.new(:numbers) |> Plug.start()

    %{}
    |> Plug.call(plug)
    |> Composite.call(composite)

    sink = Enum.find(composite.components, &is_struct(&1, Sink))
    Heap.data(sink.origin)
  end
end
