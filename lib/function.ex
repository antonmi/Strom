defmodule Strom.Function do
  use GenServer

  defstruct function: nil, pid: nil

  def start(function) do
    state = %__MODULE__{function: function}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = state), do: {:ok, %{state | pid: self()}}

  def call(flow, %__MODULE__{function: function, pid: pid}, names)
      when is_map(flow) and is_function(function) and is_list(names) do
    streams =
      Enum.reduce(names, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    sub_flow = GenServer.call(pid, {:call, streams}, :infinity)

    Map.merge(flow, sub_flow)
  end

  def call(flow, function, name) when is_map(flow), do: call(flow, function, [name])

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call({:call, streams}, _from, state) do
    sub_flow =
      Enum.reduce(streams, %{}, fn {name, stream}, acc ->
        stream = Stream.map(stream, &state.function.(&1))
        Map.put(acc, name, stream)
      end)

    {:reply, sub_flow, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}
end
