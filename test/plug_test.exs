defmodule Strom.PlugTest do
  use ExUnit.Case, async: false
  alias Strom.{Transformer, Source, Sink, Sink.Heap}

  alias Strom.{Plug, Socket}

  @moduletag timeout: 5000

  test "call" do
    source = Source.new(:numbers, [1, 2, 3, 4]) |> Source.start()
    plus_one = Transformer.new(:numbers, &(&1 + 1)) |> Transformer.start()
    plug = Plug.new(:numbers) |> Plug.start()

    socket = Socket.new(:numbers) |> Socket.start()
    mult_two = Transformer.new(:numbers, &(&1 * 2)) |> Transformer.start()
    sink = Sink.new(:numbers, %Heap{}, true) |> Sink.start()

    %{}
    |> Source.call(source)
    |> Transformer.call(plus_one)
    |> Plug.call(plug)
    |> Socket.call(socket)
    |> Transformer.call(mult_two)
    |> Sink.call(sink)

    assert Heap.data(sink.origin) == [4, 6, 8, 10]
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

    test "with cluster", %{node1: node1, node2: node2} do
      assert :rpc.call(node1, TestModule, :node1, []) == %{}
      assert :rpc.call(node2, TestModule, :node2, []) == [4, 6, 8, 10]
    end
  end
end
