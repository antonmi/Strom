defmodule Strom.Topology do
  defstruct pid: nil,
            module: nil,
            opts: [],
            components: []

  use GenServer

  alias Strom.Composite

  def start(topology_module, opts \\ []) when is_atom(topology_module) do
    strom_sup_pid = Process.whereis(Strom.DynamicSupervisor)

    pid =
      case DynamicSupervisor.start_child(
             strom_sup_pid,
             %{
               id: __MODULE__,
               start:
                 {__MODULE__, :start_link, [%__MODULE__{module: topology_module, opts: opts}]},
               restart: :transient
             }
           ) do
        {:ok, pid} ->
          pid

        {:error, {:already_started, pid}} ->
          pid
      end

    __state__(pid)
  end

  def start_link(%__MODULE__{} = topology) do
    GenServer.start_link(__MODULE__, topology, name: topology.module)
  end

  @impl true
  def init(%__MODULE__{module: module} = topology) do
    components =
      topology.opts
      |> module.topology()
      |> List.flatten()
      |> Composite.build()

    Enum.each(components, &Process.link(&1.pid))

    {:ok, %{topology | pid: self(), components: components}}
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  def info(module), do: GenServer.call(module, :__state__)

  def components(module), do: GenServer.call(module, :components)

  def call(flow, module), do: GenServer.call(module, {:call, flow}, :infinity)

  def stop(module) when is_atom(module), do: GenServer.call(module, :stop)

  @impl true
  def handle_call(:__state__, _from, topology), do: {:reply, topology, topology}

  def handle_call(:components, _from, topology), do: {:reply, topology.components, topology}

  def handle_call({:call, init_flow}, _from, %__MODULE__{} = topology) do
    flow = Composite.reduce_flow(topology.components, init_flow)

    {:reply, flow, topology}
  end

  def handle_call(:stop, _from, %__MODULE__{} = topology) do
    Composite.stop_components(topology.components)

    {:stop, :normal, :ok, topology}
  end

  @impl true
  def handle_info(:continue, topology) do
    {:noreply, topology}
  end

  def handle_info({_task_ref, :ok}, topology) do
    # do nothing for now
    {:noreply, topology}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, topology) do
    # do nothing for now
    {:noreply, topology}
  end

  defmacro __using__(_opts) do
    quote do
      import Strom.DSL

      def start(opts \\ []) do
        Strom.Topology.start(__MODULE__, opts)
      end

      def call(flow) when is_map(flow) do
        Strom.Topology.call(flow, __MODULE__)
      end

      def stop do
        Strom.Topology.stop(__MODULE__)
      end

      def info do
        Strom.Topology.info(__MODULE__)
      end

      def components do
        Strom.Topology.components(__MODULE__)
      end
    end
  end
end
