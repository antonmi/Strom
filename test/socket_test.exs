defmodule Strom.SocketTest do
  use ExUnit.Case, async: false

  alias Strom.{Socket, Plug}
  alias Strom.Cluster.SocketPlugNode

  setup do
    socket = Socket.start(Socket.new(:numbers))
    on_exit(fn -> Socket.stop(socket) end)
    %{socket: socket}
  end

  test "call/2", %{socket: socket} do
    flow =
      %{numbers: [1, 2, 3], other: [:a, :b, :c]}
      |> Socket.call(socket)

    assert flow == %{other: [:a, :b, :c]}
    Process.sleep(10)

    plug = Plug.start(Plug.new(:numbers))
    %{numbers: stream} = Plug.call(flow, plug)
    assert Enum.to_list(stream) == [1, 2, 3]
  end

  test "name clashes, just overrides for now", %{socket: socket} do
    assert :global.whereis_name({:strom, :numbers, :socket}) == socket.pid
    socket2 = Socket.start(Socket.new(:numbers))
    assert :global.whereis_name({:strom, :numbers, :socket}) == socket2.pid
    [node] =
      LocalCluster.start_nodes("test", 1,
        applications: [:strom],
        files: ["test/cluster/socket_plug_node.ex"]
      )
    socket3 = :rpc.call(node, SocketPlugNode, :start_socket, [:numbers])
    assert :global.whereis_name({:strom, :numbers, :socket}) == socket3.pid
  end
end
