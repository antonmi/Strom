defmodule Strom.Cluster.Nodes do
  alias Strom.{Composite, Transformer, Source, Sink, Sink.Heap}

  alias Strom.{Socket, Plug}

  def composite1(numbers, sleep \\ 1) do
    source = Source.new(:numbers, numbers)

    plus_one =
      Transformer.new(:numbers, fn n ->
        Process.sleep(sleep)
        n + 1
      end)

    Composite.new([source, plus_one])
  end

  def composite2(sleep \\ 1) do
    minus_one =
      Transformer.new(:numbers, fn n ->
        Process.sleep(sleep)
        n - 1
      end)

    sink = Sink.new(:numbers, %Heap{}, true)

    Composite.new([minus_one, sink])
  end

  def node1(numbers) do
    composite = Composite.start(composite1(numbers))

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
