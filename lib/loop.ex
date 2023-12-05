defmodule Strom.Loop do
  use GenServer

  @default_timeout 5_000
  defstruct data: [], pid: nil, infinite: false, last_data_at: nil, timeout: @default_timeout

  def start, do: start([])

  def start(%__MODULE__{} = loop), do: loop

  def start(opts) do
    loop = %__MODULE__{
      timeout: Keyword.get(opts, :timeout, @default_timeout)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, loop)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = loop), do: {:ok, %{loop | pid: self()}}

  def call(%__MODULE__{} = loop), do: GenServer.call(loop.pid, :get_data)

  def call(%__MODULE__{} = loop, data), do: GenServer.call(loop.pid, {:put_data, data})

  def stop(%__MODULE__{} = loop), do: GenServer.call(loop.pid, :stop)

  def infinite?(%__MODULE__{infinite: infinite}), do: infinite

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:get_data, _from, %__MODULE__{data: data} = loop) do
    last_data_at = if is_nil(loop.last_data_at), do: time_now(), else: loop.last_data_at
    loop = %{loop | data: [], last_data_at: last_data_at}

    case data do
      [] ->
        if time_now() - last_data_at > loop.timeout do
          {:reply, {:error, {:halt, loop}}, loop}
        else
          {:reply, {:ok, {[], loop}}, loop}
        end

      data ->
        {:reply, {:ok, {data, loop}}, loop}
    end
  end

  def handle_call({:put_data, data}, _from, %__MODULE__{} = loop) do
    loop = %{loop | data: loop.data ++ [data], last_data_at: time_now()}
    {:reply, {:ok, {[], loop}}, loop}
  end

  def handle_call(:stop, _from, %__MODULE__{} = loop) do
    {:stop, :normal, :ok, loop}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  defp time_now do
    "Etc/UTC"
    |> DateTime.now!()
    |> DateTime.to_unix(:millisecond)
  end
end
