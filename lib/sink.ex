defmodule Strom.Sink do
  @callback start(map) :: map
  @callback call(map, term) :: {:ok, {term, map}} | {:error, {term, map}}
  @callback stop(map) :: map

  use GenServer

  defstruct [:origin, :pid]

  def start(origin) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    state = %__MODULE__{origin: origin}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = state), do: {:ok, %{state | pid: self()}}

  def call(%__MODULE__{pid: pid}, data), do: GenServer.call(pid, {:call, data})

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def call(flow, sink, names, sync \\ false)

  def call(flow, %__MODULE__{} = sink, names, sync)
      when is_map(flow) and is_list(names) and is_boolean(sync) do
    sub_flow =
      Enum.reduce(names, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    Enum.reduce(sub_flow, flow, fn {name, stream}, acc ->
      stream =
        Stream.transform(stream, sink, fn el, sink ->
          call(sink, el)
          {[], sink}
        end)

      if sync do
        Stream.run(stream)
      else
        Task.async(fn -> Stream.run(stream) end)
      end

      Map.delete(acc, name)
    end)
  end

  def call(flow, sink, name, sync) when is_map(flow) and is_boolean(sync) do
    call(flow, sink, [name], sync)
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call({:call, data}, _from, %__MODULE__{origin: origin} = state) do
    {[], state} =
      case apply(origin.__struct__, :call, [origin, data]) do
        {:ok, {[], origin}} ->
          {[], %{state | origin: origin}}

        {:error, {:halt, origin}} ->
          {:halt, %{state | origin: origin}}
      end

    {:reply, {[], state}, state}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin} = state) do
    origin = apply(origin.__struct__, :stop, [origin])
    state = %{state | origin: origin}
    {:stop, :normal, :ok, state}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}
end
