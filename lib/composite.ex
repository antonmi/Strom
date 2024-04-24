defmodule Strom.Composite do
  @moduledoc """
  Runs a set of components and is a component itself, meaning that a composite has the same interface - it accepts flow as input and returns a modified flow.

      ## Example
      iex> alias Strom.{Composite, Transformer, Splitter, Source, Sink}
      iex> transformer = Transformer.new(:s, &(&1 + 1))
      iex> splitter = Splitter.new(:s, %{odd: &(rem(&1, 2) == 1), even: &(rem(&1, 2) == 0)})
      iex> composite = [transformer, splitter] |> Composite.new() |> Composite.start()
      iex> source = :s |> Source.new([1, 2, 3]) |> Source.start()
      iex> %{odd: odd, even: even} = %{} |> Source.call(source) |> Composite.call(composite)
      iex> {Enum.to_list(odd), Enum.to_list(even)}
      {[3], [2, 4]}

      ## Composites can be created from other composites
      iex> alias Strom.{Composite, Transformer, Splitter, Source, Sink}
      iex> transformer = Transformer.new(:s, &(&1 + 1))
      iex> splitter = Splitter.new(:s, %{odd: &(rem(&1, 2) == 1), even: &(rem(&1, 2) == 0)})
      iex> c1 = Composite.new([transformer])
      iex> c2 = Composite.new([splitter])
      iex> source = Source.new(:s, [1, 2, 3])
      iex> composite = [source, c1, c2] |> Composite.new() |> Composite.start()
      iex> %{odd: odd, even: even} = %{} |> Composite.call(composite)
      iex> {Enum.to_list(odd), Enum.to_list(even)}
      {[3], [2, 4]}
  """

  defstruct pid: nil,
            components: []

  use GenServer

  @type t :: %__MODULE__{}

  @spec new([struct()]) :: __MODULE__.t()
  def new(components) when is_list(components) do
    components =
      Enum.reduce(components, [], fn component, acc ->
        case component do
          %__MODULE__{components: components} ->
            acc ++ components

          component ->
            acc ++ [component]
        end
      end)

    %__MODULE__{components: components}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{} = composite) do
    {:ok, pid} = GenServer.start_link(__MODULE__, composite)
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

  def build(components) do
    components
    |> Enum.map(fn component ->
      case component do
        %Strom.Source{} = source ->
          Strom.Source.start(source)

        %Strom.Sink{} = sink ->
          Strom.Sink.start(sink)

        %Strom.Mixer{} = mixer ->
          Strom.Mixer.start(mixer)

        %Strom.Splitter{} = splitter ->
          Strom.Splitter.start(splitter)

        %Strom.Transformer{} = transformer ->
          Strom.Transformer.start(transformer)

        %Strom.Renamer{} = renamer ->
          Strom.Renamer.start(renamer)
      end
    end)
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{pid: pid}), do: GenServer.call(pid, {:call, flow}, :infinity)

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:__state__, _from, composite), do: {:reply, composite, composite}

  def handle_call({:call, init_flow}, _from, %__MODULE__{} = composite) do
    flow = reduce_flow(composite.components, init_flow)

    {:reply, flow, composite}
  end

  def handle_call(:stop, _from, %__MODULE__{components: components} = composite) do
    stop_components(components)

    {:stop, :normal, :ok, composite}
  end

  def reduce_flow(components, init_flow) do
    Enum.reduce(components, init_flow, fn component, flow ->
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
  end

  def stop_components(components) do
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
  end

  @impl true
  def handle_info({_task_ref, :ok}, composite) do
    # do nothing for now
    {:noreply, composite}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, composite) do
    # do nothing for now
    {:noreply, composite}
  end
end
