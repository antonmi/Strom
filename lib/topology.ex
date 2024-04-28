defmodule Strom.Topology do
  @moduledoc """
  Runs set of components. Restarts all of them in case of crash.
  Provides `start`, `stop`, `call` function on the module level.
  Starts topology as a process with the topology module name.
  So only one topology with the given name can exist.

      ## Example:
      iex>  defmodule OddEvenTopology do
      ...>    use Strom.Topology
      ...>    alias Strom.Sink.Null
      ...>
      ...>    def topology(_opts) do
      ...>      [
      ...>        Source.new(:s1, [1, 2, 3]),
      ...>        Source.new(:s2, [4, 5, 6]),
      ...>        Mixer.new([:s1, :s2], :s),
      ...>        Transformer.new(:s, &(&1 + 1)),
      ...>        Splitter.new(:s, %{odd: &(rem(&1, 2) == 1), even: &(rem(&1, 2) == 0)}),
      ...>        Sink.new(:odd, Null.new(), true)
      ...>      ]
      ...>    end
      ...>  end
      iex> OddEvenTopology.start()
      iex> %{even: even} = OddEvenTopology.call(%{})
      iex> Enum.sort(Enum.to_list(even))
      [2, 4, 6]
  """

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

  defmacro __using__(_opts) do
    quote do
      import Strom.DSL
      alias Strom.{Composite, Mixer, Sink, Source, Splitter, Transformer, Topology}

      def start(opts \\ []), do: Topology.start(__MODULE__, opts)

      def call(flow) when is_map(flow), do: Topology.call(flow, __MODULE__)

      def stop, do: Topology.stop(__MODULE__)

      def info, do: Topology.info(__MODULE__)

      def components, do: Topology.components(__MODULE__)
    end
  end
end
