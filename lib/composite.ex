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
            name: nil,
            components: []

  use GenServer

  @type t :: %__MODULE__{}

  @spec new([struct()]) :: __MODULE__.t()
  def new(components, name \\ nil) when is_list(components) do
    components =
      components
      |> List.flatten()
      |> Enum.reduce([], fn component, acc ->
        case component do
          %__MODULE__{components: components} ->
            acc ++ components

          component ->
            acc ++ [component]
        end
      end)

    name = if name, do: name, else: generate_name(components)

    %__MODULE__{name: name, components: components}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{} = composite) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        Strom.DynamicSupervisor,
        %{id: __MODULE__, start: {__MODULE__, :start_link, [composite]}, restart: :permanent}
      )

    Process.link(pid)
    :sys.get_state(pid)
  end

  def start_link(%__MODULE__{name: name} = composite) do
    GenServer.start_link(__MODULE__, composite, name: {:global, name})
  end

  @impl true
  def init(%__MODULE__{components: components} = composite) do
    {:ok, %{composite | pid: self(), components: build(components)}}
  end

  def build(components) do
    Enum.map(components, fn %{__struct__: module} = component ->
      component
      |> module.start()
      |> tap(&monitor_component/1)
    end)
  end

  @spec call(Strom.flow(), __MODULE__.t() | atom()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name}),
    do: GenServer.call({:global, name}, {:call, flow}, :infinity)

  def call(flow, name) when is_atom(name),
    do: GenServer.call({:global, name}, {:call, flow}, :infinity)

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{name: name, components: components}) do
    pid = :global.whereis_name(name)
    Process.unlink(pid)
    stop_components(components)
    DynamicSupervisor.terminate_child(Strom.DynamicSupervisor, pid)
  end

  @impl true
  def handle_call({:call, init_flow}, _from, %__MODULE__{} = composite) do
    flow = reduce_flow(composite.components, init_flow)
    collect_garbage(composite)
    {:reply, flow, composite}
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, _pid, _reason},
        %__MODULE__{components: components} = composite
      ) do
    stop_components(components)
    {:noreply, %{composite | components: build(components)}}
  end

  def reduce_flow(components, init_flow) do
    Enum.reduce(components, init_flow, fn %{__struct__: module} = component, flow ->
      flow
      |> module.call(component)
      |> tap(fn _flow -> collect_garbage(component) end)
    end)
  end

  def stop_components(components) do
    Enum.each(components, fn %{__struct__: module} = component ->
      module.stop(component)
    end)
  end

  defp generate_name(components) do
    components
    |> Enum.map(fn %{__struct__: struct} -> to_string(struct) end)
    |> Enum.map(&String.at(&1, 13))
    |> Enum.join("")
    |> String.slice(0..15)
    |> then(&(&1 <> "_" <> timestamp_postfix()))
    |> String.to_atom()
  end

  defp monitor_component(%Strom.Renamer{}), do: :nothing

  defp monitor_component(component) do
    Process.monitor(component.pid)
  end

  defp collect_garbage(%Strom.Renamer{}), do: :nothing

  defp collect_garbage(component) do
    spawn(fn ->
      :erlang.garbage_collect(component.pid)
    end)
  end

  defp timestamp_postfix do
    :erlang.system_time()
    |> rem(round(1.0e9))
    |> to_string()
  end
end
