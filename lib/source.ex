defmodule Strom.Source do
  @callback start(map) :: map
  @callback call(map) :: {:ok, {term, map}} | {:error, {:halt, map}}
  @callback stop(map) :: map
  @callback infinite?(map) :: true | false

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

  def call(%__MODULE__{pid: pid}), do: GenServer.call(pid, :call)

  def infinite?(%__MODULE__{pid: pid}), do: GenServer.call(pid, :infinite)

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def stream(%{__struct__: source_module} = source) do
    Stream.resource(
      fn -> source end,
      fn source -> apply(source_module, :call, [source]) end,
      fn source -> source end
    )
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:call, _from, %__MODULE__{origin: origin} = state) do
    {events, state} =
      case apply(origin.__struct__, :call, [origin]) do
        {:ok, {events, origin}} ->
          state = %{state | origin: origin}
          {events, state}

        {:error, {:halt, origin}} ->
          state = %{state | origin: origin}

          case apply(origin.__struct__, :infinite?, [origin]) do
            true -> {[], state}
            false -> {:halt, state}
          end
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
