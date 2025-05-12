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
            clients: %{},
            tasks_started: false,
            tasks_run: false,
            stopping: false,
            before_stop: nil,
            data: %{},
            data_size: 0,
            waiting_tasks: %{},
            waiting_clients: %{}

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

        name ->
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
    GenServer.start_link(__MODULE__, gm)
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
  def handle_call({:start_tasks, input_streams}, _from, %__MODULE__{} = gm)
      when is_map(input_streams) do
    new_tasks = Tasks.start_tasks(input_streams, gm)
    tasks = Map.merge(gm.tasks, new_tasks)

    {:reply, {:ok, gm.pid, new_tasks},
     %{gm | tasks_started: true, tasks_run: false, tasks: tasks, input_streams: input_streams}}
  end

  def handle_call(
        {:run_tasks, output_name},
        {client_pid, _ref},
        %__MODULE__{tasks_started: true, tasks_run: false} = gm
      ) do
    Tasks.run_tasks(gm.tasks, gm.accs)
    clients = Map.put(gm.clients, client_pid, output_name)
    {:reply, :ok, %{gm | tasks_run: true, clients: clients}}
  end

  def handle_call(
        {:run_tasks, output_name},
        {client_pid, _ref},
        %__MODULE__{tasks_started: true, tasks_run: true} = gm
      ) do
    clients = Map.put(gm.clients, client_pid, output_name)
    {:reply, :ok, %{gm | clients: clients}}
  end

  def handle_call(:stop, _from, %__MODULE__{} = gm) do
    gm = %{gm | stopping: true}

    if ready_to_stop?(gm) do
      if gm.before_stop, do: gm.before_stop.()

      {:stop, :normal, :ok, gm}
    else
      # send_halt_to_tasks(gm.tasks)
      {:reply, :ok, gm}
    end
  end

  def handle_call(:state, _from, %__MODULE__{} = gm) do
    {:reply, gm, gm}
  end

  def handle_call(
        {:transfer_tasks, new_tasks},
        _from,
        %__MODULE__{} = gm
      ) do
    Enum.each(gm.tasks, fn {task_pid, stream_name} ->
      {pid_for_stream, _} = Enum.find(new_tasks, fn {_, name} -> name == stream_name end)
      send(task_pid, {:task, :run_new_tasks_and_halt, {pid_for_stream, gm.accs[stream_name]}})
    end)

    {:reply, gm, gm}
  end

  @impl true

  def handle_cast({:gen_mix, :stopping}, gm) do
    {:noreply, %{gm | stopping: true}}
  end

  def handle_cast(
        {:put_data, {input_name, task_pid}, {new_data, new_acc}},
        %__MODULE__{} = gm
      ) do
    {all_data, waiting_clients, total_count} =
      Streams.new_data(new_data, gm.outputs, gm.data, gm.waiting_clients)

    waiting_tasks =
      if total_count < gm.buffer do
        send(task_pid, {:task, input_name, :continue})
        gm.waiting_tasks
      else
        Map.put(gm.waiting_tasks, task_pid, input_name)
      end

    gm = %{
      gm
      | data: all_data,
        accs: Map.put(gm.accs, input_name, new_acc),
        data_size: total_count,
        waiting_clients: waiting_clients,
        waiting_tasks: waiting_tasks
    }

    after_action(gm)
  end

  def handle_cast({:get_data, {output_name, client_pid}}, %__MODULE__{} = gm) do
    output_data = Map.get(gm.data, output_name, [])
    initital_data_size = gm.data_size

    gm =
      case {output_data, map_size(gm.tasks)} do
        # no tasks left, send done
        {[], 0} ->
          send(client_pid, {:client, output_name, :done})
          %{gm | data: Map.put(gm.data, output_name, [])}

        # no data, but there are running tasks, wait.
        {[], _} ->
          %{gm | waiting_clients: Map.put(gm.waiting_clients, client_pid, output_name)}

        # there is data, send it to the client
        {output_data, _} ->
          send(client_pid, {:client, output_name, {:data, output_data}})

          %{
            gm
            | data: Map.put(gm.data, output_name, []),
              data_size: gm.data_size - length(output_data)
          }
      end

    any_data_taken = gm.data_size < initital_data_size

    waiting_tasks =
      if any_data_taken and gm.data_size < gm.buffer do
        send_continue_to_tasks(gm.waiting_tasks)

        %{}
      else
        gm.waiting_tasks
      end

    gm = %{gm | waiting_tasks: waiting_tasks}
    after_action(gm)
  end

  defp after_action(gm) do
    cond do
      ready_to_stop?(gm) ->
        {data, data_size} =
          send_data_to_clients(gm.data, gm.data_size, gm.clients)

        send_done_to_clients(gm.clients)

        if gm.before_stop, do: gm.before_stop.()

        {:stop, :normal,
         %{
           gm
           | data: data,
             data_size: data_size,
             tasks_started: false,
             tasks_run: false,
             waiting_clients: %{},
             clients: %{}
         }}

      ready_to_finish?(gm) ->
        {data, data_size} =
          send_data_to_clients(gm.data, gm.data_size, gm.waiting_clients)

        send_done_to_clients(gm.waiting_clients)

        {:noreply,
         %{gm | data: data, data_size: data_size, tasks_run: false, waiting_clients: %{}}}

      true ->
        {:noreply, gm}
    end
  end

  defp send_continue_to_tasks(tasks) do
    Enum.each(tasks, fn {task_pid, input_name} ->
      send(task_pid, {:task, input_name, :continue})
    end)
  end

  # defp send_halt_to_tasks(tasks) do
  # Enum.each(tasks, fn {task_pid, _} ->
  # send(task_pid, {:task, :halt})
  # end)
  # end

  defp send_done_to_clients(clients) do
    Enum.each(clients, fn {client_pid, output_name} ->
      send(client_pid, {:client, output_name, :done})
    end)
  end

  defp send_data_to_clients(data, data_size, clients) do
    Enum.reduce(clients, {data, data_size}, fn {client_pid, output_name}, {data, count} ->
      case Map.get(data, output_name, []) do
        [] ->
          {data, count}

        data_for_client ->
          send(client_pid, {:client, output_name, {:data, data_for_client}})
          {Map.put(data, output_name, []), count - length(data_for_client)}
      end
    end)
  end

  def ready_to_finish?(gm) do
    map_size(gm.tasks) == 0 and map_size(gm.waiting_tasks) == 0
  end

  defp ready_to_stop?(gm) do
    ready_to_finish?(gm) and gm.stopping
  end

  @impl true
  def handle_info({ref, pid}, gm) do
    # Task finished when started with TaskSupervisor
    Process.demonitor(ref, [:flush])
    task_finished(pid, gm)
  end

  def handle_info({:DOWN, ref, :process, pid, :normal}, gm) do
    # Task finished when started in Composite
    Process.demonitor(ref, [:flush])
    task_finished(pid, gm)
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

  defp task_finished(pid, %__MODULE__{no_wait: false} = gm) do
    gm = %{
      gm
      | tasks: Map.delete(gm.tasks, pid),
        waiting_tasks: Map.delete(gm.waiting_tasks, pid)
    }

    after_action(gm)
  end

  defp task_finished(_pid, %__MODULE__{no_wait: true} = gm) do
    send_data_to_clients(gm.data, gm.data_size, gm.clients)
    send_done_to_clients(gm.clients)

    {:noreply,
     %{
       gm
       | tasks_run: false,
         tasks: %{}
     }}
  end
end
