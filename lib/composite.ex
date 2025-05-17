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

  alias Strom.Composite.Manipulations
  alias Strom.Composite.StartStop

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

    name = if name, do: name, else: StartStop.generate_name(components)

    %__MODULE__{name: name, components: components}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{} = composite) do
    %{composite | pid: StartStop.start(composite)}
  end

  def supervisor_name(name), do: :"Supervisor_#{name}"

  def component_supervisor_name(name), do: :"ComponentSupervisor_#{name}"

  def task_supervisor_name(name), do: :"TaskSupervisor_#{name}"

  def start_link(%__MODULE__{name: name} = composite) do
    GenServer.start_link(__MODULE__, composite, name: name)
  end

  @impl true
  def init(%__MODULE__{} = composite) do
    {:ok, %{composite | pid: self()}, {:continue, :start_components}}
  end

  @impl true
  def handle_continue(
        :start_components,
        %__MODULE__{name: name, components: components} = composite
      ) do
    {:noreply, %{composite | components: StartStop.start_components(components, name)}}
  end

  def components(%__MODULE__{name: name}) do
    GenServer.call(name, :components)
  end

  @spec call(Strom.flow(), __MODULE__.t() | atom()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name}),
    do: GenServer.call(name, {:call, flow}, :infinity)

  def call(flow, name) when is_atom(name),
    do: GenServer.call(name, {:call, flow}, :infinity)

  def call_flow(components, init_flow) do
    Enum.reduce(components, init_flow, fn %{__struct__: module} = component, flow ->
      module.call(flow, component)
    end)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = composite), do: StartStop.stop(composite)

  @spec delete(__MODULE__.t(), {integer(), integer()}) :: __MODULE__.t() | {:error, atom()}
  def delete(composite, {index_from, index_to}) do
    GenServer.call(composite.name, {:delete, index_from, index_to})
  end

  @spec delete(__MODULE__.t(), integer()) :: __MODULE__.t() | {:error, atom()}
  def delete(composite, index) do
    delete(composite, {index, index})
  end

  @spec insert(__MODULE__.t(), integer(), Strom.component()) ::
          {__MODULE__.t(), Strom.flow()} | {:error, atom()}
  def insert(composite, index, new_component) when is_struct(new_component) do
    insert(composite, index, [new_component])
  end

  @spec insert(__MODULE__.t(), integer(), list(Strom.component())) ::
          {__MODULE__.t(), Strom.flow()} | {:error, atom()}
  def insert(composite, index, new_components) when is_list(new_components) do
    GenServer.call(composite.name, {:insert, index, new_components})
  end

  @spec replace(__MODULE__.t(), integer(), Strom.component()) ::
          {__MODULE__.t(), Strom.flow()} | {:error, atom()}
  def replace(composite, index, new_component)
      when is_integer(index) and is_struct(new_component) do
    replace(composite, {index, index}, [new_component])
  end

  @spec replace(__MODULE__.t(), {integer(), integer()}, Strom.component()) ::
          {__MODULE__.t(), Strom.flow()} | {:error, atom()}
  def replace(composite, {index_from, index_to}, new_component) when is_struct(new_component) do
    replace(composite, {index_from, index_to}, [new_component])
  end

  @spec replace(__MODULE__.t(), {integer(), integer()}, list(Strom.component())) ::
          {__MODULE__.t(), Strom.flow()} | {:error, atom()}
  def replace(composite, {index_from, index_to}, new_components) when is_list(new_components) do
    GenServer.call(composite.name, {:replace, {index_from, index_to}, new_components})
  end

  @spec replace(__MODULE__.t(), integer(), list(Strom.component())) ::
          {__MODULE__.t(), Strom.flow()} | {:error, atom()}
  def replace(composite, index, new_components)
      when is_integer(index) and is_list(new_components) do
    replace(composite, {index, index}, new_components)
  end

  @impl true
  def handle_call({:call, init_flow}, _from, %__MODULE__{} = composite) do
    flow = call_flow(composite.components, init_flow)
    {:reply, flow, composite}
  end

  def handle_call(:components, _from, %__MODULE__{components: components} = composite) do
    {:reply, components, composite}
  end

  def handle_call(:stop_components, _from, %__MODULE__{components: components} = composite) do
    stop_components(components)
    {:reply, :ok, composite}
  end

  def handle_call(:stop, _from, %__MODULE__{} = composite) do
    {:stop, :normal, :ok, composite}
  end

  def handle_call(
        {:replace, {index_from, index_to}, new_components},
        _from,
        %__MODULE__{components: components, name: name} = composite
      )
      when is_list(new_components) do
    case Manipulations.replace(components, index_from, index_to, new_components, name) do
      {components, _deleted_components, subflow} ->
        composite = %{composite | components: components}
        {:reply, {composite, subflow}, composite}

      {:error, message} ->
        {:reply, {:error, message}, composite}
    end
  end

  def handle_call(
        {:delete, index_from, index_to},
        _from,
        %__MODULE__{components: components, name: name} = composite
      ) do
    case Manipulations.replace(components, index_from, index_to, [], name) do
      {components, _deleted_components, subflow} ->
        composite = %{composite | components: components}
        {:reply, {composite, subflow}, composite}

      {:error, message} ->
        {:reply, {:error, message}, composite}
    end
  end

  def handle_call(
        {:insert, index, new_components},
        _from,
        %__MODULE__{components: components, name: name} = composite
      )
      when is_list(new_components) do
    case Manipulations.insert(components, index, new_components, name) do
      {components, [], subflow} ->
        composite = %{composite | components: components}
        {:reply, {composite, subflow}, composite}

      {:error, message} ->
        {:reply, {:error, message}, composite}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, :normal}, composite) do
    # component stopped normally
    Process.demonitor(ref, [:flush])
    {:noreply, composite}
  end

  def handle_info(
        {:DOWN, _ref, :process, pid, _not_normal},
        %__MODULE__{components: components} = composite
      ) do
    component = Enum.find(components, fn %{pid: ^pid} -> true end)
    Enum.each(components, & &1.__struct__.stop(&1))
    {:stop, {:component_crashed, component}, composite}
  end

  defp stop_components(components) do
    Enum.each(components, fn %{__struct__: module} = component ->
      module.stop(component)
    end)
  end
end
