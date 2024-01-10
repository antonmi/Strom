defmodule Strom.Source do
  @callback start(map) :: map
  @callback call(map) :: {:ok, {[term], map}} | {:error, {:halt, map}}
  @callback stop(map) :: map
  @callback infinite?(map) :: true | false

  use GenServer

  defstruct [:origin, :pid, :flow_pid, :sup_pid]

  def start(%__MODULE__{origin: list} = source) when is_list(list) do
    start(%{source | origin: %Strom.Source.Events{events: list}})
  end

  def start(%__MODULE__{origin: origin} = source) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    source = %{source | origin: origin}
    {:ok, pid} = DynamicSupervisor.start_child(source.sup_pid, {__MODULE__, source})
    __state__(pid)
  end

  def start(list) when is_list(list) do
    start(%Strom.Source.Events{events: list})
  end

  def start(origin) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    state = %__MODULE__{origin: origin}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state)
  end

  @impl true
  def init(%__MODULE__{} = state), do: {:ok, %{state | pid: self()}}

  def call(%__MODULE__{pid: pid}), do: GenServer.call(pid, :call, :infinity)

  def infinite?(%__MODULE__{pid: pid}), do: GenServer.call(pid, :infinite)

  def stop(%__MODULE__{origin: origin, pid: pid, sup_pid: sup_pid}) do
    apply(origin.__struct__, :stop, [origin])

    if sup_pid do
      :ok
    else
      GenServer.call(pid, :stop)
    end
  end

  def call(flow, %__MODULE__{} = source, names) when is_map(flow) and is_list(names) do
    sub_flow =
      Enum.reduce(names, %{}, fn name, acc ->
        stream =
          Stream.resource(
            fn -> source end,
            fn source -> call(source) end,
            fn source -> source end
          )

        prev_stream = Map.get(flow, name, [])
        Map.put(acc, name, Stream.concat(prev_stream, stream))
      end)

    Map.merge(flow, sub_flow)
  end

  def call(flow, source, name) when is_map(flow) do
    call(flow, source, [name])
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
