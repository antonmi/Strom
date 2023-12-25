defmodule Strom.Cons do
  use GenServer

  @buffer 2

  defstruct pid: nil,
            distributor_pid: nil,
            running: false,
            client: nil,
            name: nil,
            fun: nil,
            data: []

  def start({name, fun}, distributor_pid, opts \\ []) when is_list(opts) do
    state = %__MODULE__{distributor_pid: distributor_pid, name: name, fun: fun, running: true}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def init(%__MODULE__{} = cons) do
    {:ok, %{cons | pid: self()}}
  end

  def call(cons) do
    Stream.resource(
      fn ->
        GenServer.call(cons.pid, :register_client)
      end,
      fn cons ->
        case GenServer.call(cons.pid, :get_data) do
          {:ok, data} ->
            if length(data) == 0 do
              receive do
                :continue ->
                  flush()
              end
            end
            {data, cons}

          {:error, :done} ->
            {:halt, cons}
        end
      end,
      fn cons -> cons end
    )
  end

  def stop(cons) do
    GenServer.call(cons.pid, :stop)
  end

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  def handle_call(:get_data, _from, cons) do
    if length(cons.data) == 0 and !cons.running do
      {:reply, {:error, :done}, cons}
    else
      data = cons.data
      cons = %{cons | data: []}
      GenServer.cast(cons.distributor_pid, :continue)
      {:reply, {:ok, data}, cons}
    end
  end

  def handle_call(:register_client, {pid, ref}, cons) do
    cons = %{cons | client: pid}

    {:reply, cons, cons}
  end

  def handle_call({:put_data, new_data}, _from, cons) do
    {new_data, _} = Enum.split_with(new_data, cons.fun)
    cons = %{cons | data: cons.data ++ new_data}
    if cons.client do
      send(cons.client, :continue)
    end
    {:reply, cons, cons}
  end

  def handle_cast(:continue, cons) do
    if cons.client do
      send(cons.client, :continue)
    end
    {:noreply, cons}
  end

  def handle_call(:stop, _from, cons) do
    cons = %{cons | running: false}

    {:reply, cons, cons}
  end

  def handle_call(:__state__, _from, cons), do: {:reply, cons, cons}
end
