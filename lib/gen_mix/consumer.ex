defmodule Strom.GenMix.Consumer do
  use GenServer

  defstruct pid: nil,
            mix_pid: nil,
            running: false,
            client: nil,
            name: nil,
            fun: nil,
            data: []

  def start({name, fun}, mix_pid, opts \\ []) when is_list(opts) do
    state = %__MODULE__{mix_pid: mix_pid, name: name, fun: fun, running: true}

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
      GenServer.cast(cons.mix_pid, {:consumer_got_data, {cons.name, cons.fun}})
      {:reply, {:ok, data}, cons}
    end
  end

  def handle_call(:register_client, {pid, ref}, cons) do
    cons = %{cons | client: pid}

    {:reply, cons, cons}
  end

  def handle_cast({:put_data, new_data}, cons) do
    {new_data, _} = Enum.split_with(new_data, cons.fun)
    cons = %{cons | data: cons.data ++ new_data}

    {:noreply, cons}
  end

  def handle_cast(:continue, cons) do
    if cons.client do
      send(cons.client, :continue)
    end

    {:noreply, cons}
  end

  def handle_cast(:stop, cons) do
    cons = %{cons | running: false}

    {:noreply, cons}
  end

  def handle_call(:__state__, _from, cons), do: {:reply, cons, cons}
end
