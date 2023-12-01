defmodule Strom.Sink do
  @callback start(map) :: map
  @callback call(map, term) :: {:ok, {term, map}} | {:error, {term, map}}
  @callback stop(map) :: map

  use GenServer

  defstruct [:origin, :pid]

  def start(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    state = %__MODULE__{origin: origin}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = state), do: {:ok, %{state | pid: self()}}

  def call(%__MODULE__{pid: pid}, data), do: GenServer.call(pid, {:call, data})

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def stream(stream, %{__struct__: sink_module} = sink) do
    stream
    |> Stream.transform(
      fn -> sink end,
      fn el, sink ->
        apply(sink_module, :call, [sink, el])
        {[el], sink}
      end,
      fn sink -> sink end
    )
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call({:call, data}, _from, %__MODULE__{origin: origin} = state) do
    {events, state} =
      case apply(origin.__struct__, :call, [origin, data]) do
        {:ok, {events, origin}} ->
          state = %{state | origin: origin}
          {events, state}

        {:error, {:halt, origin}} ->
          {:halt, %{state | origin: origin}}
      end

    {:reply, {events, state}, state}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin} = state) do
    origin = apply(origin.__struct__, :stop, [origin])
    state = %{state | origin: origin}
    {:stop, :normal, :ok, state}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}
end
