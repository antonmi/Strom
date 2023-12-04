defmodule Strom.Flow do
  defstruct pid: nil,
            name: nil,
            streams: [],
            modules: [],
            sources: [],
            sinks: [],
            mixers: [],
            splitters: [],
            flows: [],
            module: nil,
            topology: []

  use GenServer
  alias Strom.DSL

  @type t :: %__MODULE__{}

  # TODO Supervisor
  def start(flow_module, _opts \\ []) when is_atom(flow_module) do
    state = %__MODULE__{name: flow_module, module: flow_module}

    {:ok, pid} = GenServer.start_link(__MODULE__, state, name: flow_module)

    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{module: module} = state) do
    topology = Strom.Builder.build(module.flow_topology())
    {:ok, %{state | pid: self(), topology: topology}}
  end

  def topology(flow_module), do: GenServer.call(flow_module, :topology)

  def run(flow_module, flow), do: GenServer.call(flow_module, {:run, flow}, :infinity)

  def stop(flow_module) when is_atom(flow_module), do: GenServer.call(flow_module, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:topology, _from, state), do: {:reply, state.topology, state}

  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:run, init_flow}, _from, %__MODULE__{streams: streams} = state) do
    flow =
      state.topology
      |> Enum.reduce(init_flow, fn component, flow ->
        case component do
          %DSL.Source{source: source, name: name} ->
            Strom.Source.stream(flow, source, name)

          %DSL.Sink{sink: sink, name: name, sync: sync} ->
            Strom.Sink.stream(flow, sink, name, sync)

          %DSL.Mixer{mixer: mixer, inputs: inputs, output: output} ->
            Strom.Mixer.stream(flow, mixer, inputs, output)

          %DSL.Splitter{splitter: splitter, input: input, partitions: partitions} ->
            Strom.Splitter.stream(flow, splitter, input, partitions)

          %DSL.Function{function: function, inputs: inputs} ->
            Strom.Function.stream(flow, function, inputs)

          %DSL.Module{module: module, inputs: inputs, state: state} = mod ->
            Strom.Module.stream(flow, module, inputs)
        end
      end)

    {:reply, flow, state}
  end

  def handle_call(:stop, _from, %__MODULE__{} = state) do
    state.topology
    |> Enum.each(fn component ->
      case component do
        %DSL.Source{source: source} ->
          Strom.Source.stop(source)

        %DSL.Sink{sink: sink} ->
          Strom.Sink.stop(sink)

        %DSL.Mixer{mixer: mixer} ->
          Strom.Mixer.stop(mixer)

        %DSL.Splitter{splitter: splitter} ->
          Strom.Splitter.stop(splitter)

        %DSL.Function{function: function, inputs: inputs} ->
          Strom.Function.stop(function)

        %DSL.Module{module: module, inputs: inputs, state: state} = mod ->
          Strom.Module.stop(module)
      end
    end)

    {:stop, :normal, :ok, state}
  end

  def handle_info({_task_ref, :ok}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end
end
