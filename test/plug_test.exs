defmodule Strom.PlugTest do
  use ExUnit.Case, async: false

  alias Strom.{Socket, Plug}
  alias Strom.Cluster.SocketPlugNode

  setup do
    plug = Plug.start(Plug.new(:numbers))
    on_exit(fn -> Plug.stop(plug) end)
    %{plug: plug}
  end

  test "call/2", %{plug: plug} do
    %{other: [:a, :b, :c], numbers: stream} = Plug.call(%{other: [:a, :b, :c]}, plug)

    task =
      Task.async(fn ->
        assert Enum.to_list(stream) == [1, 2, 3]
      end)

    Process.sleep(10)
    socket = Socket.start(Socket.new(:numbers))
    Socket.call(%{numbers: [1, 2, 3]}, socket)

    Task.await(task)
  end

  test "name clashes, just overrides for now", %{plug: plug} do
    assert :global.whereis_name({:strom, :numbers, :plug}) == plug.pid
    plug2 = Plug.start(Plug.new(:numbers))
    assert :global.whereis_name({:strom, :numbers, :plug}) == plug2.pid
    [node] =
      LocalCluster.start_nodes("test", 1,
        applications: [:strom],
        files: ["test/cluster/socket_plug_node.ex"]
      )
    plug3 = :rpc.call(node, SocketPlugNode, :start_plug, [:numbers])
    assert :global.whereis_name({:strom, :numbers, :plug}) == plug3.pid
  end
end
