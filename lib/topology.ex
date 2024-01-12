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
        %Strom.Source{origin: origin, names: names} ->
          %{Strom.Source.start(origin) | names: names}

        %Strom.Sink{origin: origin, names: names} ->
          %{Strom.Sink.start(origin) | names: names}

        %Strom.Mixer{opts: opts} = mixer ->
          %{mixer | gen_mix: Strom.Mixer.start(opts)}

        %Strom.Splitter{opts: opts} = split ->
          %{split | gen_mix: Strom.Splitter.start(opts)}

        %Strom.Transformer{opts: opts, function: function, acc: acc, inputs: inputs}
        when is_list(opts) ->
          %{Strom.Transformer.start(opts) | function: function, acc: acc, inputs: inputs}

        %Strom.Renamer{names: names} ->
          Strom.Renamer.start(names)
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
          %Strom.Source{names: names} = source ->
            Strom.Source.call(flow, source, names)

          %Strom.Sink{names: names, sync: sync} = sink ->
            Strom.Sink.call(flow, sink, names, sync)

          %Strom.Mixer{inputs: inputs, output: output} = mixer ->
            Strom.Mixer.call(flow, mixer, inputs, output)

          %Strom.Splitter{input: input, partitions: partitions} = splitter ->
            Strom.Splitter.call(flow, splitter, input, partitions)

          %Strom.Transformer{function: function, acc: acc, inputs: inputs} = transformer ->
            if is_function(function, 1) do
              Strom.Transformer.call(flow, transformer, inputs, function)
            else
              Strom.Transformer.call(flow, transformer, inputs, {function, acc})
            end

          %Strom.Renamer{} = renamer ->
            Strom.Renamer.call(flow, renamer)
        end
      end)

    {:reply, flow, topology}
  end

  def handle_call(:stop, _from, %__MODULE__{components: components} = topology) do
    Enum.each(components, fn component ->
      case component do
        %Strom.Source{} = source ->
          Strom.Source.stop(source)

        %Strom.Sink{} = sink ->
          Strom.Sink.stop(sink)

        %Strom.Mixer{} = mixer ->
          Strom.Mixer.stop(mixer)

        %Strom.Splitter{} = splitter ->
          Strom.Splitter.stop(splitter)

        %Strom.Transformer{} = transformer ->
          Strom.Transformer.stop(transformer)

        %Strom.Renamer{} = renamer ->
          Strom.Renamer.stop(renamer)
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
