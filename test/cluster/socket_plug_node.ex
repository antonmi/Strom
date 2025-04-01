defmodule Strom.Cluster.SocketPlugNode do
  alias Strom.{Socket, Plug}

  def start_socket(name) do
    Socket.start(Socket.new(name))
  end

  def start_plug(name) do
    Plug.start(Plug.new(name))
  end
end
