defmodule Strom.Flow do
  defstruct pid: nil,
            name: nil,
            streams: [],
            modules: [],
            sources: [],
            sinks: [],
            mixers: [],
            splitters: [],
            flows: []

  use GenServer

  @type t :: %__MODULE__{}

  # TODO Supervisor
  def start(flow_module, _opts \\ []) when is_atom(flow_module) do
    state = %__MODULE__{name: flow_module}

    {:ok, pid} = GenServer.start_link(__MODULE__, state, name: flow_module)

    Strom.Builder.build(flow_module.topology(), pid)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = state) do
    {:ok, %{state | pid: self()}}
  end

  def add_stream(pid, stream), do: GenServer.call(pid, {:add_stream, stream})

  def add_component(pid, component),
    do: GenServer.call(pid, {:add_component, component})

  def run(flow_module), do: GenServer.call(flow_module, :run, :infinity)

  def stop(flow_module) when is_atom(flow_module), do: GenServer.call(flow_module, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:add_stream, stream}, _from, %__MODULE__{streams: streams} = state) do
    state = %{state | streams: [stream | streams]}
    {:reply, :ok, state}
  end

  def handle_call({:add_component, component}, _from, %__MODULE__{} = state) do
    state =
      case component do
        %Strom.Mixer{} ->
          %{state | mixers: [component | state.mixers]}

        %Strom.Splitter{} ->
          %{state | splitters: [component | state.splitters]}

        %Strom.Source{} ->
          %{state | sources: [component | state.sources]}

        %Strom.Sink{} ->
          %{state | sinks: [component | state.sinks]}

        %Strom.DSL.Module{} ->
          %{state | modules: [component | state.modules]}

        %Strom.Flow{} ->
          %{state | flows: [component | state.flows]}
      end

    {:reply, :ok, state}
  end

  def handle_call(:run, _from, %__MODULE__{streams: streams} = state) do
    streams
    |> Enum.map(fn stream ->
      Task.async(fn ->
        Stream.run(stream)
      end)
    end)
    |> Enum.map(&Task.await(&1, :infinity))

    {:reply, :ok, state}
  end

  def handle_call(:stop, _from, %__MODULE__{} = state) do
    Enum.each(state.sources, & &1.__struct__.stop(&1))
    Enum.each(state.sinks, & &1.__struct__.stop(&1))
    Enum.each(state.mixers, & &1.__struct__.stop(&1))
    Enum.each(state.splitters, & &1.__struct__.stop(&1))
    Enum.each(state.flows, & &1.__struct__.stop(&1))

    Enum.each(state.modules, fn %{module: module, state: state} ->
      if Strom.DSL.Module.is_pipeline_module?(module) do
        apply(module, :stop, [])
      else
        apply(module, :stop, [state])
      end
    end)

    {:stop, :normal, :ok, state}
  end
end
