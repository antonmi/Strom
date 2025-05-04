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
  alias Strom.Renamer

  @type t :: %__MODULE__{}

  @spec new([struct()]) :: __MODULE__.t()
  def new(components, name \\ nil) when is_list(components) do
    components =
      components
      |> List.flatten()
      |> Enum.flat_map(fn
        %__MODULE__{components: components} -> components
        component -> [component]
      end)

    name = if name, do: name, else: generate_name(components)

    %__MODULE__{name: name, components: components}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{} = composite) do
    registry_name = :"Registry_#{composite.name}"

    {:ok, _registry_pid} =
      DynamicSupervisor.start_child(
        Strom.DynamicSupervisor,
        %{
          id: registry_name,
          start: {Registry, :start_link, [[keys: :unique, name: registry_name]]},
          restart: :temporary
        }
      )

    {:ok, pid} =
      DynamicSupervisor.start_child(
        Strom.DynamicSupervisor,
        %{id: __MODULE__, start: {__MODULE__, :start_link, [composite]}, restart: :temporary}
      )

    Process.link(pid)
    %{composite | pid: pid}
  end

  def start_link(%__MODULE__{name: name} = composite) do
    GenServer.start_link(__MODULE__, composite, name: name)
  end

  @impl true
  def init(%__MODULE__{name: name} = composite) do
    {:ok, %{composite | pid: self(), components: start_components(composite.components, name)}}
  end

  def components(%__MODULE__{name: name}) do
    GenServer.call(name, :components)
  end

  @spec call(Strom.flow(), __MODULE__.t() | atom()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name}),
    do: GenServer.call(name, {:call, flow}, :infinity)

  def call(flow, name) when is_atom(name),
    do: GenServer.call(name, {:call, flow}, :infinity)

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{name: name}) do
    pid = Process.whereis(name)
    Process.unlink(pid)
    GenServer.call(name, :stop_components)
    DynamicSupervisor.terminate_child(Strom.DynamicSupervisor, pid)
  end

  def start_components(components, name) do
    components
    |> Enum.reduce([], fn
      %{__struct__: Renamer} = component, acc ->
        [Renamer.start(component) | acc]

      %{__struct__: module} = component, acc ->
        component = %{component | composite: {name, make_ref()}}
        component = module.start(component)
        Process.monitor(component.pid)
        [component | acc]
    end)
    |> Enum.reverse()
  end

  def delete(composite, index) do
    GenServer.call(composite.name, {:delete, index})
  end

  def insert(composite, index, new_components) do
    GenServer.call(composite.name, {:insert, index, new_components})
  end

  @impl true
  def handle_call({:call, init_flow}, _from, %__MODULE__{} = composite) do
    flow = reduce_flow(composite.components, init_flow)
    {:reply, flow, composite}
  end

  def handle_call(:components, _from, %__MODULE__{components: components} = composite) do
    {:reply, components, composite}
  end

  def handle_call(:stop_components, _from, %__MODULE__{components: components} = composite) do
    stop_components(components)
    {:reply, :ok, composite}
  end

  def handle_call(
        {:delete, index},
        _from,
        %__MODULE__{components: components} = composite
      ) do
    component = Enum.at(components, index)
    input_streams = Strom.GenMix.state(component.pid).input_streams
    :ok = component.__struct__.stop(component)

    next_component = Enum.at(components, index + 1)
    GenServer.call(next_component.pid, {:reregister, component.composite, input_streams})

    {:reply, composite, %{composite | components: List.delete_at(components, index)}}
  end

  def handle_call(
        {:insert, index, new_components},
        _from,
        %__MODULE__{components: components, name: name} = composite
      )
      when is_list(new_components) do
    component_after = Enum.at(components, index)
    gm_after = Strom.GenMix.state(component_after.pid)

    new_components = start_components(new_components, name)
    flow = reduce_flow(new_components, gm_after.input_streams)

    GenServer.call(component_after.pid, {:reregister, gm_after.composite, flow})

    components =
      components
      |> List.insert_at(index, new_components)
      |> List.flatten()

    {:reply, composite, %{composite | components: components}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, :normal}, composite) do
    # component stopped normally
    {:noreply, composite}
  end

  def handle_info(
        {:DOWN, _ref, :process, pid, _not_normal},
        %__MODULE__{components: components} = composite
      ) do
    component = Enum.find(components, fn %{pid: ^pid} -> true end)
    {:stop, {:component_crashed, component}, composite}
  end

  defp reduce_flow(components, init_flow) do
    Enum.reduce(components, init_flow, fn %{__struct__: module} = component, flow ->
      module.call(flow, component)
    end)
  end

  defp stop_components(components) do
    Enum.each(components, fn %{__struct__: module} = component ->
      module.stop(component)
    end)
  end

  defp generate_name(components) do
    components
    |> Enum.map(fn %{__struct__: struct} -> to_string(struct) end)
    |> Enum.map_join("", &String.at(&1, 13))
    |> String.slice(0..15)
    |> then(&(&1 <> "_" <> timestamp_postfix()))
    |> String.to_atom()
  end

  defp timestamp_postfix do
    :erlang.system_time()
    |> rem(round(1.0e9))
    |> to_string()
  end
end
