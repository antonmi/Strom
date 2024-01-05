defmodule Strom.Flow do
  defstruct pid: nil,
            name: nil,
            module: nil,
            opts: [],
            topology: []

  use GenServer
  alias Strom.DSL

  @type t :: %__MODULE__{}

  # TODO Supervisor
  def start(flow_module, opts \\ []) when is_atom(flow_module) do
    state = %__MODULE__{name: flow_module, module: flow_module, opts: opts}

    {:ok, pid} = GenServer.start_link(__MODULE__, state, name: flow_module)

    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{module: module} = state) do
    topology =
      state.opts
      |> module.topology()
      |> List.flatten()
      |> build()

    {:ok, %{state | pid: self(), topology: topology}}
  end

  defp build(components) do
    components
    |> Enum.map(fn component ->
      case component do
        %DSL.Source{origin: origin} = source ->
          %{source | source: Strom.Source.start(origin)}

        %DSL.Sink{origin: origin} = sink ->
          %{sink | sink: Strom.Sink.start(origin)}

        %DSL.Mix{opts: opts} = mix ->
          %{mix | mixer: Strom.Mixer.start(opts)}

        %DSL.Splitter{opts: opts} = splitter ->
          %{splitter | splitter: Strom.Splitter.start(opts)}

        %DSL.Transform{opts: nil} = fun ->
          %{fun | call: Strom.Transformer.start()}

        %DSL.Transform{opts: opts} = fun when is_list(opts) ->
          %{fun | call: Strom.Transformer.start(opts)}

        %DSL.Rename{names: names} = ren ->
          rename = Strom.Renamer.start(names)
          %{ren | rename: rename}
      end
    end)
  end

  def info(flow_module), do: GenServer.call(flow_module, :info)

  def call(flow_module, flow), do: GenServer.call(flow_module, {:call, flow}, :infinity)

  def stop(flow_module) when is_atom(flow_module), do: GenServer.call(flow_module, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:info, _from, state), do: {:reply, state.topology, state}

  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:call, init_flow}, _from, %__MODULE__{} = state) do
    flow =
      state.topology
      |> Enum.reduce(init_flow, fn component, flow ->
        case component do
          %DSL.Source{source: source, names: names} ->
            Strom.Source.call(flow, source, names)

          %DSL.Sink{sink: sink, names: names, sync: sync} ->
            Strom.Sink.call(flow, sink, names, sync)

          %DSL.Mix{mixer: mixer, inputs: inputs, output: output} ->
            Strom.Mixer.call(flow, mixer, inputs, output)

          %DSL.Splitter{splitter: splitter, input: input, partitions: partitions} ->
            Strom.Splitter.call(flow, splitter, input, partitions)

          %DSL.Transform{call: call, function: function, acc: acc, inputs: inputs} ->
            if is_function(function, 1) do
              Strom.Transformer.call(flow, call, inputs, function)
            else
              Strom.Transformer.call(flow, call, inputs, {function, acc})
            end

          %DSL.Rename{rename: rename, names: names} ->
            Strom.Renamer.call(flow, rename, names)
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

        %DSL.Mix{mixer: mixer} ->
          Strom.Mixer.stop(mixer)

        %DSL.Splitter{splitter: splitter} ->
          Strom.Splitter.stop(splitter)

        %DSL.Transform{call: call} ->
          Strom.Transformer.stop(call)
      end
    end)

    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info(:continue, flow) do
    {:noreply, flow}
  end

  def handle_info({_task_ref, :ok}, flow) do
    # do nothing for now
    {:noreply, flow}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, flow) do
    # do nothing for now
    {:noreply, flow}
  end
end
