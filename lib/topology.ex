defmodule Strom.Topology do
  defstruct pid: nil,
            components: []

  use GenServer
  alias Strom.DSL

  @type t :: %__MODULE__{}

  def start(components) when is_list(components) do
    state = %__MODULE__{components: components}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{components: components} = topology) do
    components =
      components
      |> List.flatten()
      |> build()

    {:ok, %{topology | pid: self(), components: components}}
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

        %DSL.Split{opts: opts} = split ->
          %{split | splitter: Strom.Splitter.start(opts)}

        %DSL.Transform{opts: opts} = transform when is_list(opts) ->
          %{transform | transformer: Strom.Transformer.start(opts)}

        %DSL.Rename{} = ren ->
          %{ren | rename: Strom.Renamer.start()}
      end
    end)
  end

  def call(flow, %__MODULE__{pid: pid}), do: GenServer.call(pid, {:call, flow}, :infinity)

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:call, init_flow}, _from, %__MODULE__{} = topology) do
    flow =
      Enum.reduce(topology.components, init_flow, fn component, flow ->
        case component do
          %DSL.Source{source: source, names: names} ->
            Strom.Source.call(flow, source, names)

          %DSL.Sink{sink: sink, names: names, sync: sync} ->
            Strom.Sink.call(flow, sink, names, sync)

          %DSL.Mix{mixer: mixer, inputs: inputs, output: output} ->
            Strom.Mixer.call(flow, mixer, inputs, output)

          %DSL.Split{splitter: splitter, input: input, partitions: partitions} ->
            Strom.Splitter.call(flow, splitter, input, partitions)

          %DSL.Transform{transformer: transformer, function: function, acc: acc, inputs: inputs} ->
            if is_function(function, 1) do
              Strom.Transformer.call(flow, transformer, inputs, function)
            else
              Strom.Transformer.call(flow, transformer, inputs, {function, acc})
            end

          %DSL.Rename{names: names} ->
            Strom.Renamer.call(flow, names)
        end
      end)

    {:reply, flow, topology}
  end

  def handle_call(:stop, _from, %__MODULE__{components: components} = topology) do
    Enum.each(components, fn component ->
      case component do
        %DSL.Source{source: source} ->
          Strom.Source.stop(source)

        %DSL.Sink{sink: sink} ->
          Strom.Sink.stop(sink)

        %DSL.Mix{mixer: mixer} ->
          Strom.Mixer.stop(mixer)

        %DSL.Split{splitter: splitter} ->
          Strom.Splitter.stop(splitter)

        %DSL.Transform{transformer: transformer} ->
          Strom.Transformer.stop(transformer)

        %DSL.Rename{rename: rename} ->
          Strom.Renamer.stop(rename)
      end
    end)

    {:stop, :normal, :ok, topology}
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
