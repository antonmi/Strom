defmodule Strom.SocketPlugTest do
  use ExUnit.Case, async: false

  alias Strom.{Transformer, Source, Sink, Sink.Heap}
  alias Strom.{Socket, Plug}
  alias Strom.Cluster.Nodes

  describe "socket and plug" do
    setup do
      %{
        source: Source.new(:numbers, [1, 2, 3, 4]) |> Source.start(),
        plus_one: Transformer.new(:numbers, &(&1 + 1)) |> Transformer.start(),
        socket: Socket.new(:numbers) |> Socket.start(),
        plug: Plug.new(:numbers) |> Plug.start(),
        mult_two: Transformer.new(:numbers, &(&1 * 2)) |> Transformer.start(),
        sink: Sink.new(:numbers, %Heap{}, true) |> Sink.start()
      }
    end

    test "call the whole pipeline", %{
      source: source,
      plus_one: plus_one,
      socket: socket,
      plug: plug,
      mult_two: mult_two,
      sink: sink
    } do
      %{}
      |> Source.call(source)
      |> Transformer.call(plus_one)
      |> Socket.call(socket)
      |> Plug.call(plug)
      |> Transformer.call(mult_two)
      |> Sink.call(sink)

      assert Heap.data(sink.origin) == [4, 6, 8, 10]
    end



    test "name clashes in socket" do
      # TODO
    end

    test "name clashes in plug" do
      # TODO
    end
  end

  describe "with cluster" do
    setup do
      :global.unregister_name({:strom, :numbers, :plug})
      :global.unregister_name({:strom, :numbers, :socket})

      [node1, node2] =
        LocalCluster.start_nodes("test", 2,
          applications: [:strom],
          files: ["test/cluster/nodes.ex"]
        )

      on_exit(fn ->
        :ok = LocalCluster.stop_nodes([node1, node2])
      end)

      %{node1: node1, node2: node2}
    end

    test "call both nodes", %{node1: node1, node2: node2} do
      numbers = [1, 2, 3, 4]
      assert :rpc.call(node1, Nodes, :node1, [numbers]) == %{}
      assert :rpc.call(node2, Nodes, :node2, []) == numbers
    end
  end
end
