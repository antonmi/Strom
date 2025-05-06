defmodule Strom.GenMix do
  @moduledoc """
  Generic functionality used by other components.
  """

  use GenServer

  @chunk 1
  @buffer 1000

  defstruct pid: nil,
            composite: nil,
            process_chunk: nil,
            inputs: [],
            outputs: %{},
            accs: %{},
            opts: [],
            chunk: @chunk,
            buffer: @buffer,
            no_wait: false,
            input_streams: %{},
            tasks: %{},
            tasks_started: false,
            tasks_run: false,
            asks: %{},
            data: %{},
            data_size: 0,
            waiting_tasks: %{}

  alias Strom.Composite
  alias Strom.GenMix.Streams
  alias Strom.GenMix.Tasks

  def start(%__MODULE__{process_chunk: process_chunk, opts: opts, composite: composite} = gm)
      when is_list(opts) do
    gm = %{
      gm
      | process_chunk: if(process_chunk, do: process_chunk, else: &process_chunk/4),
        chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer),
        no_wait: Keyword.get(opts, :no_wait, false)
    }

    partitions = PartitionSupervisor.partitions(Strom.ComponentSupervisor)
    partition_key = Enum.random(1..partitions)

    child_name =
      case composite do
        nil ->
          {:via, PartitionSupervisor, {Strom.ComponentSupervisor, partition_key}}

        {name, _} ->
          Composite.component_supervisor_name(name)
      end

    {:ok, pid} =
      DynamicSupervisor.start_child(
        child_name,
        %{id: __MODULE__, start: {__MODULE__, :start_link, [gm]}, restart: :temporary}
      )

    %{gm | pid: pid}
  end

  def start_link(%__MODULE__{} = gm) do
    case name_or_pid(gm) do
      nil ->
        GenServer.start_link(__MODULE__, gm)

      registry_name ->
        GenServer.start_link(__MODULE__, gm, name: registry_name)
    end
  end

  def name_or_pid(%{pid: pid, composite: nil}), do: pid

  def name_or_pid(%{composite: composite}) do
    {composite_name, ref} = composite
    registry_name = Composite.registry_name(composite_name)
    {:via, Registry, {registry_name, ref}}
  end

  @impl true
  def init(%__MODULE__{} = gm) do
    {:ok, %{gm | pid: self()}}
  end

  @spec call(map(), map()) :: map() | no_return()
  def call(flow, gm), do: Streams.call(flow, gm)

  def state(pid), do: GenServer.call(pid, :state)

  @spec stop(any()) :: any()
  def stop(gm) do
    GenServer.call(gm.pid, :stop)
  end

  def process_chunk(_input_stream_name, chunk, outputs, nil) do
    outputs
    |> Enum.reduce({%{}, false, nil}, fn {output_name, output_stream_fun}, {acc, any?, nil} ->
      {data, _} = Enum.split_with(chunk, output_stream_fun)
      {Map.put(acc, output_name, data), any? || Enum.any?(data), nil}
    end)
  end

  @impl true
  def handle_call(
        {:start_tasks, input_streams},
        _from,
        %__MODULE__{tasks_started: false} = gm
      )
      when is_map(input_streams) do
    tasks = Tasks.start_tasks(input_streams, gm)

    {:reply, {:ok, name_or_pid(gm)},
     %{gm | tasks_started: true, tasks: tasks, input_streams: input_streams}}
  end

  def handle_call(
        {:start_tasks, input_streams},
        _from,
        %__MODULE__{tasks_started: true} = gm
      )
      when is_map(input_streams) do
    {:reply, {:ok, name_or_pid(gm)}, gm}
  end

  def handle_call(:stop, _from, %__MODULE__{tasks: tasks, composite: nil} = gm) do
    Tasks.send_to_tasks(tasks, :halt_task)
    {:stop, :normal, :ok, gm}
  end

  def handle_call(:stop, _from, %__MODULE__{tasks: tasks, composite: {name, ref}} = gm) do
    Tasks.send_to_tasks(tasks, :halt_task)
    Registry.unregister(Composite.registry_name(name), ref)

    {:stop, :normal, :ok, gm}
  end

  def handle_call(:state, _from, %__MODULE__{} = gm) do
    {:reply, gm, gm}
  end

  def handle_call(
        {:restart, {name, new_ref}, new_input_streams},
        _from,
        %__MODULE__{composite: {name, ref}} = gm
      ) do
    gm =
      if new_ref != ref do
        registry_name = Composite.registry_name(name)
        {:ok, _pid} = Registry.register(registry_name, new_ref, nil)
        %{gm | composite: {name, new_ref}}
      else
        gm
      end

    Tasks.send_to_tasks(gm.tasks, :halt_task)

    tasks =
      new_input_streams
      |> Tasks.start_tasks(gm)
      |> Tasks.run_tasks(gm.accs)

    Streams.send_to_clients(gm.asks, {:continue_ask, name_or_pid(gm)})

    {:reply, gm,
     %{
       gm
       | composite: {name, new_ref},
         input_streams: new_input_streams,
         tasks: tasks
     }}
  end

  @impl true
  def handle_cast(:run_tasks, %__MODULE__{tasks_started: true, tasks_run: false} = gm) do
    Tasks.run_tasks(gm.tasks, gm.accs)

    {:noreply, %{gm | tasks_run: true}}
  end

  def handle_cast(:run_tasks, %__MODULE__{tasks_run: true} = gm) do
    {:noreply, gm}
  end

  def handle_cast(
        {:new_data, {task_pid, input_name}, {new_data, new_acc}},
        %__MODULE__{} = gm
      ) do
    {all_data, remaining_asks, total_count} = Streams.process_new_data(new_data, gm.data, gm.asks)

    waiting_tasks =
      Streams.continue_or_wait(
        {task_pid, input_name},
        {gm.tasks, gm.waiting_tasks},
        {total_count, gm.buffer}
      )

    {:noreply,
     %{
       gm
       | data: all_data,
         accs: Map.put(gm.accs, input_name, new_acc),
         data_size: total_count,
         asks: remaining_asks,
         waiting_tasks: waiting_tasks
     }}
  end

  def handle_cast({:ask, output_name, client_pid}, %__MODULE__{} = gm) do
    {:noreply, Streams.handle_ask(output_name, client_pid, gm)}
  end

  @impl true
  def handle_info({ref, pid}, gm) do
    # Task is done when started with Task.Supervisor.async_nolink
    Tasks.handle_normal_task_stop(ref, pid, gm)
  end

  def handle_info({:DOWN, ref, :process, pid, :normal}, gm) do
    # Task is done when started in local supervisor
    Tasks.handle_normal_task_stop(ref, pid, gm)
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, gm) do
    # The task failed
    {task_pid, name} = Tasks.handle_task_error(gm, pid)

    tasks =
      gm.tasks
      |> Map.delete(pid)
      |> Map.put(task_pid, name)

    {:noreply, %{gm | tasks: tasks}}
  end
end
