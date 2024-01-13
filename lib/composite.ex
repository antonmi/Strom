defmodule Strom.Composite do
  defstruct pid: nil,
            components: []

  use GenServer

  @type t :: %__MODULE__{}

  def start(components) when is_list(components) do
    state = %__MODULE__{components: components}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{components: components} = composite) do
    components =
      components
      |> List.flatten()
      |> build()

    {:ok, %{composite | pid: self(), components: components}}
  end

  defp build(components) do
    components
    |> Enum.map(fn component ->
      case component do
        %Strom.Source{} = source ->
          Strom.Source.start(source)

        %Strom.Sink{} = sink ->
          Strom.Sink.start(sink)

        %Strom.Mixer{opts: opts} = mixer ->
          Strom.Mixer.start(mixer, opts)

        %Strom.Splitter{opts: opts} = splitter ->
          Strom.Splitter.start(splitter, opts)

        %Strom.Transformer{opts: opts} = transformer when is_list(opts) ->
          Strom.Transformer.start(transformer, opts)

        %Strom.Renamer{} = renamer ->
          Strom.Renamer.start(renamer)
      end
    end)
  end

  def call(flow, %__MODULE__{pid: pid}), do: GenServer.call(pid, {:call, flow}, :infinity)

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:__state__, _from, composite), do: {:reply, composite, composite}

  def handle_call({:call, init_flow}, _from, %__MODULE__{} = composite) do
    flow =
      Enum.reduce(composite.components, init_flow, fn component, flow ->
        case component do
          %Strom.Source{} = source ->
            Strom.Source.call(flow, source)

          %Strom.Sink{} = sink ->
            Strom.Sink.call(flow, sink)

          %Strom.Mixer{} = mixer ->
            Strom.Mixer.call(flow, mixer)

          %Strom.Splitter{} = splitter ->
            Strom.Splitter.call(flow, splitter)

          %Strom.Transformer{} = transformer ->
            Strom.Transformer.call(flow, transformer)

          %Strom.Renamer{} = renamer ->
            Strom.Renamer.call(flow, renamer)
        end
      end)

    {:reply, flow, composite}
  end

  def handle_call(:stop, _from, %__MODULE__{components: components} = composite) do
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

    {:stop, :normal, :ok, composite}
  end

  @impl true
  def handle_info(:continue, composite) do
    {:noreply, composite}
  end

  def handle_info({_task_ref, :ok}, composite) do
    # do nothing for now
    {:noreply, composite}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, composite) do
    # do nothing for now
    {:noreply, composite}
  end
end
