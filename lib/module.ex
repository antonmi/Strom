defmodule Strom.Module do
  # TODO define behaviour
  use GenServer

  defstruct module: nil, pid: nil, opts: [], state: nil

  def start(module, opts \\ []) when is_atom(module) do
    state = apply(module, :start, [opts])
    state = %__MODULE__{module: module, opts: opts, state: state}
    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = state), do: {:ok, %{state | pid: self()}}

  def call(flow, %__MODULE__{pid: pid} = state, names)
      when is_map(flow) and is_list(names) do
    streams =
      Enum.reduce(names, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    sub_flow =
      Enum.reduce(streams, %{}, fn {name, stream}, acc ->
        stream =
          if is_pipeline_module?(state.module) do
            apply(state.module, :stream, [stream])
          else
            Stream.transform(stream, state.state, fn event, acc ->
              GenServer.call(pid, {:call, event, acc}, :infinity)
            end)
          end

        Map.put(acc, name, stream)
      end)

    Map.merge(flow, sub_flow)
  end

  def call(flow, state, name) when is_map(flow), do: call(flow, state, [name])

  def stop(%__MODULE__{module: module, state: state, opts: opts, pid: pid}) do
    if is_pipeline_module?(module) do
      apply(module, :stop, [])
    else
      apply(module, :stop, [state, opts])
    end

    GenServer.call(pid, :stop)
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call({:call, event, acc}, _from, state) do
    {events, acc} = apply(state.module, :call, [event, acc, state.opts])

    {:reply, {events, acc}, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  defp is_pipeline_module?(module) when is_atom(module) do
    is_list(module.alf_components())
  rescue
    _error -> false
  end
end
