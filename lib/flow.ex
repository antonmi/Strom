# defmodule Strom.Flow do
#  defstruct pid: nil,
#            name: nil,
#            module: nil,
#            sup_pid: nil,
#            opts: [],
#            topology: []
#
#  use GenServer
#  alias Strom.DSL
#  alias Strom.FlowSupervisor
#
#  @type t :: %__MODULE__{}
#
#  def start(flow_module, opts \\ []) when is_atom(flow_module) do
#    strom_sup_pid = Process.whereis(Strom.DynamicSupervisor)
#
#    pid =
#      case DynamicSupervisor.start_child(
#             strom_sup_pid,
#             %{
#               id: __MODULE__,
#               start:
#                 {__MODULE__, :start_link,
#                  [%__MODULE__{name: flow_module, module: flow_module, opts: opts}]},
#               restart: :transient
#             }
#           ) do
#        {:ok, pid} ->
#          pid
#
#        {:error, {:already_started, pid}} ->
#          pid
#      end
#
#    __state__(pid)
#  end
#
#  def start_link(%__MODULE__{} = flow) do
#    GenServer.start_link(__MODULE__, flow, name: flow.name)
#  end
#
#  @impl true
#  def init(%__MODULE__{module: module} = flow) do
#    sup_pid = start_flow_supervisor(flow.name)
#
#    topology =
#      flow.opts
#      |> module.topology()
#      |> List.flatten()
#      |> build(self(), sup_pid)
#
#    {:ok, %{flow | pid: self(), sup_pid: sup_pid, topology: topology}}
#  end
#
#  defp start_flow_supervisor(name) do
#    sup_pid =
#      case FlowSupervisor.start_link(%{name: :"#{name}_Supervisor"}) do
#        {:ok, pid} -> pid
#        {:error, {:already_started, pid}} -> pid
#      end
#
#    Process.unlink(sup_pid)
#    Process.monitor(sup_pid)
#    sup_pid
#  end
#
#  defp build(components, flow_pid, sup_pid) do
#    components
#    |> Enum.map(fn component ->
#      case component do
#        %DSL.Source{origin: origin} = source ->
#          src = %Strom.Source{origin: origin, flow_pid: flow_pid, sup_pid: sup_pid}
#          %{source | source: Strom.Source.start(src)}
#
#        %DSL.Sink{origin: origin} = sink ->
#          snk = %Strom.Sink{origin: origin, flow_pid: flow_pid, sup_pid: sup_pid}
#          %{sink | sink: Strom.Sink.start(snk)}
#
#        %DSL.Mix{opts: opts} = mix ->
#          mixer = %Strom.Mixer{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid}
#          %{mix | mixer: Strom.Mixer.start(mixer)}
#
#        %DSL.Split{opts: opts} = split ->
#          splitter = %Strom.Splitter{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid}
#          %{split | splitter: Strom.Splitter.start(splitter)}
#
#        %DSL.Transform{opts: opts} = transform when is_list(opts) ->
#          transformer = %Strom.Transformer{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid}
#          %{transform | transformer: Strom.Transformer.start(transformer)}
#
#        %DSL.Rename{} = ren ->
#          %{ren | rename: Strom.Renamer.start()}
#      end
#    end)
#  end
#
#  def info(flow_module), do: GenServer.call(flow_module, :info)
#
#  def call(flow_module, flow), do: GenServer.call(flow_module, {:call, flow}, :infinity)
#
#  def stop(flow_module) when is_atom(flow_module), do: GenServer.call(flow_module, :stop)
#
#  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)
#
#  @impl true
#  def handle_call(:info, _from, state), do: {:reply, state.topology, state}
#
#  def handle_call(:__state__, _from, state), do: {:reply, state, state}
#
#  def handle_call({:call, init_flow}, _from, %__MODULE__{} = state) do
#    flow =
#      state.topology
#      |> Enum.reduce(init_flow, fn component, flow ->
#        case component do
#          %DSL.Source{source: source, names: names} ->
#            Strom.Source.call(flow, source, names)
#
#          %DSL.Sink{sink: sink, names: names, sync: sync} ->
#            Strom.Sink.call(flow, sink, names, sync)
#
#          %DSL.Mix{mixer: mixer, inputs: inputs, output: output} ->
#            Strom.Mixer.call(flow, mixer, inputs, output)
#
#          %DSL.Split{splitter: splitter, input: input, partitions: partitions} ->
#            Strom.Splitter.call(flow, splitter, input, partitions)
#
#          %DSL.Transform{transformer: transformer, function: function, acc: acc, inputs: inputs} ->
#            if is_function(function, 1) do
#              Strom.Transformer.call(flow, transformer, inputs, function)
#            else
#              Strom.Transformer.call(flow, transformer, inputs, {function, acc})
#            end
#
#          %DSL.Rename{names: names} ->
#            Strom.Renamer.call(flow, names)
#        end
#      end)
#
#    {:reply, flow, state}
#  end
#
#  def handle_call(:stop, _from, %__MODULE__{} = flow) do
#    flow.topology
#    |> Enum.each(fn component ->
#      case component do
#        %DSL.Source{source: source} ->
#          Strom.Source.stop(source)
#
#        %DSL.Sink{sink: sink} ->
#          Strom.Sink.stop(sink)
#
#        %DSL.Mix{mixer: mixer} ->
#          Strom.Mixer.stop(mixer)
#
#        %DSL.Split{splitter: splitter} ->
#          Strom.Splitter.stop(splitter)
#
#        %DSL.Transform{transformer: transformer} ->
#          Strom.Transformer.stop(transformer)
#
#        %DSL.Rename{rename: rename} ->
#          Strom.Renamer.stop(rename)
#      end
#    end)
#
#    Supervisor.stop(flow.sup_pid)
#    {:stop, :normal, :ok, flow}
#  end
#
#  @impl true
#  def handle_info(:continue, flow) do
#    {:noreply, flow}
#  end
#
#  def handle_info({_task_ref, :ok}, flow) do
#    # do nothing for now
#    {:noreply, flow}
#  end
#
#  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, flow) do
#    # do nothing for now
#    {:noreply, flow}
#  end
# end
