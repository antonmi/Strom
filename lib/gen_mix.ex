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

  def start(%__MODULE__{process_chunk: process_chunk, opts: opts} = gm) when is_list(opts) do
    gm = %{
      gm
      | process_chunk: if(process_chunk, do: process_chunk, else: &process_chunk/4),
        chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer),
        no_wait: Keyword.get(opts, :no_wait, false)
    }

    partitions = PartitionSupervisor.partitions(Strom.ComponentSupervisor)
    partition_key = Enum.random(1..partitions)

    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, partition_key}},
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

  defp name_or_pid(%{pid: pid, composite: nil}), do: pid

  defp name_or_pid(%{composite: composite}) do
    {composite_name, ref} = composite
    registry_name = String.to_existing_atom("Registry_#{composite_name}")
    {:via, Registry, {registry_name, ref}}
  end

  @impl true
  def init(%__MODULE__{} = gm) do
    {:ok, %{gm | pid: self()}}
  end

  @spec call(map(), map()) :: map() | no_return()
  def call(flow, gm) do
    input_streams =
      Enum.reduce(gm.inputs, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    gm_identifier =
      case GenServer.call(gm.pid, {:start_tasks, input_streams}) do
        {:ok, gm_pid} ->
          gm_pid

        {:error, :already_called} ->
          raise "Compoment has been already called"
      end

    sub_flow = build_sub_flow(gm.outputs, gm_identifier)

    flow
    |> Map.drop(gm.inputs)
    |> Map.merge(sub_flow)
  end

  def state(pid), do: GenServer.call(pid, :state)

  defp build_sub_flow(outputs, gm_identifier) do
    Enum.reduce(outputs, %{}, fn {output_name, _fun}, flow ->
      stream =
        Stream.resource(
          fn ->
            GenServer.cast(gm_identifier, :run_tasks)
            gm_identifier
          end,
          fn gm_identifier ->
            ask_and_wait(gm_identifier, output_name)
          end,
          fn gm_identifier -> gm_identifier end
        )

      Map.put(flow, output_name, stream)
    end)
  end

  defp ask_and_wait(gm_identifier, output_name) do
    GenServer.cast(gm_identifier, {:ask, output_name, self()})

    receive do
      {^output_name, :done} ->
        {:halt, gm_identifier}

      {^output_name, events} ->
        {events, gm_identifier}

      {:continue_ask, gm_identifier} ->
        ask_and_wait(gm_identifier, output_name)
    end
  end

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

  defp run_stream_in_task({name, stream}, {gm_identifier, outputs, chunk}, process_chunk) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        acc =
          receive do
            {:run_task, acc} ->
              acc
          end

        stream
        |> Stream.chunk_every(chunk)
        |> Stream.transform(
          fn -> acc end,
          fn chunk, acc ->
            case process_chunk.(name, chunk, outputs, acc) do
              {new_data, true, new_acc} ->
                GenServer.cast(gm_identifier, {:new_data, name, {new_data, new_acc}})

                receive do
                  :continue_task ->
                    {[], new_acc}

                  :halt_task ->
                    {:halt, new_acc}
                end

              {_new_data, false, new_acc} ->
                {[], new_acc}
            end
          end,
          fn acc -> acc end
        )
        |> Stream.run()

        self()
      end
    )
  end

  @impl true
  def handle_call(
        {:start_tasks, input_streams},
        _from,
        %__MODULE__{tasks_started: false} = gm
      )
      when is_map(input_streams) do
    tasks = do_start_tasks(input_streams, gm)

    {:reply, {:ok, name_or_pid(gm)},
     %{gm | tasks_started: true, tasks: tasks, input_streams: input_streams}}
  end

  def handle_call(
        {:start_tasks, input_streams},
        _from,
        %__MODULE__{tasks_started: true} = gm
      )
      when is_map(input_streams) do
    {:reply, {:error, :already_called}, gm}
  end

  def handle_call(:stop, _from, %__MODULE__{tasks: tasks, composite: nil} = gm) do
    send_to_tasks(tasks, :halt_task)
    {:stop, :normal, :ok, gm}
  end

  def handle_call(:stop, _from, %__MODULE__{tasks: tasks, composite: {name, ref}} = gm) do
    send_to_tasks(tasks, :halt_task)
    Registry.unregister(String.to_existing_atom("Registry_#{name}"), ref)

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
        registry_name = String.to_existing_atom("Registry_#{name}")
        {:ok, _pid} = Registry.register(registry_name, new_ref, nil)
        %{gm | composite: {name, new_ref}}
      else
        gm
      end

    send_to_tasks(gm.tasks, :halt_task)

    tasks =
      new_input_streams
      |> do_start_tasks(gm)
      |> do_run_tasks(gm.accs)

    send_to_clients(gm.asks, {:continue_ask, name_or_pid(gm)})

    {:reply, gm,
     %{
       gm
       | composite: {name, new_ref},
         input_streams: new_input_streams,
         tasks: tasks
     }}
  end

  def send_to_tasks(tasks, message) do
    Enum.each(tasks, &send(elem(&1, 1), message))
  end

  defp send_to_clients(asks, message) do
    Enum.each(asks, &send(elem(&1, 1), message))
  end

  defp do_start_tasks(input_streams, gm) do
    Enum.reduce(input_streams, %{}, fn {name, stream}, acc ->
      task =
        run_stream_in_task(
          {name, stream},
          {name_or_pid(gm), gm.outputs, gm.chunk},
          gm.process_chunk
        )

      Map.put(acc, name, task.pid)
    end)
  end

  defp do_run_tasks(tasks, accs) do
    Enum.each(tasks, fn {name, task_pid} ->
      send(task_pid, {:run_task, accs[name]})
    end)

    tasks
  end

  defp terminate_tasks(tasks) do
    Enum.each(
      Map.values(tasks),
      &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1)
    )
  end

  defp process_new_data(new_data, gm_data, asks) do
    Enum.reduce(new_data, {gm_data, asks, 0}, fn {output_name, data}, {all_data, asks, count} ->
      data_for_output = Map.get(all_data, output_name, []) ++ data

      {data_for_output, asks} =
        case {data_for_output, asks[output_name]} do
          {[], _} ->
            {data_for_output, asks}

          {data_for_output, nil} ->
            {data_for_output, asks}

          {data_for_output, client_pid} ->
            send(client_pid, {output_name, data_for_output})
            {[], Map.delete(asks, output_name)}
        end

      {Map.put(all_data, output_name, data_for_output), asks, count + length(data_for_output)}
    end)
  end

  defp continue_or_wait(name, {tasks, waiting_tasks}, {total_count, buffer}) do
    case {tasks[name], total_count < buffer} do
      {nil, _} ->
        waiting_tasks

      {task_pid, true} ->
        send(task_pid, :continue_task)
        waiting_tasks

      {task_pid, false} ->
        Map.put(waiting_tasks, name, task_pid)
    end
  end

  @impl true
  def handle_cast(:run_tasks, %__MODULE__{tasks_started: true, tasks_run: false} = gm) do
    do_run_tasks(gm.tasks, gm.accs)

    {:noreply, %{gm | tasks_run: true}}
  end

  def handle_cast(:run_tasks, %__MODULE__{tasks_run: true} = gm) do
    {:noreply, gm}
  end

  def handle_cast(
        {:new_data, input_name, {new_data, new_acc}},
        %__MODULE__{} = gm
      ) do
    {all_data, remaining_asks, total_count} = process_new_data(new_data, gm.data, gm.asks)

    waiting_tasks =
      continue_or_wait(input_name, {gm.tasks, gm.waiting_tasks}, {total_count, gm.buffer})

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

  def handle_cast({:ask, output_name, client_pid}, %__MODULE__{asks: asks} = gm) do
    {asks, new_data, data_size_for_output} =
      case Map.get(gm.data, output_name, []) do
        [] ->
          if map_size(gm.tasks) == 0 do
            send(client_pid, {output_name, :done})
            {Map.delete(asks, output_name), gm.data, 0}
          else
            {Map.put(asks, output_name, client_pid), gm.data, 0}
          end

        events ->
          send(client_pid, {output_name, events})

          {Map.delete(asks, output_name), Map.put(gm.data, output_name, []), length(events)}
      end

    new_data_size = gm.data_size - data_size_for_output

    waiting_tasks =
      if new_data_size < gm.buffer do
        send_to_tasks(gm.waiting_tasks, :continue_task)
        %{}
      else
        gm.waiting_tasks
      end

    {:noreply,
     %{
       gm
       | asks: asks,
         data: new_data,
         data_size: new_data_size,
         waiting_tasks: waiting_tasks
     }}
  end

  @impl true
  def handle_info({ref, pid}, gm) do
    # Task is done
    Process.demonitor(ref, [:flush])

    case Enum.find(gm.tasks, fn {_name, task_pid} -> task_pid == pid end) do
      nil ->
        # from another component
        {:noreply, gm}

      {input_name, _} ->
        {tasks, waiting_tasks} =
          if gm.no_wait do
            terminate_tasks(gm.tasks)
            {%{}, %{}}
          else
            {Map.delete(gm.tasks, input_name), Map.delete(gm.waiting_tasks, input_name)}
          end

        asks =
          case map_size(tasks) do
            0 ->
              Enum.each(gm.asks, fn {output_name, client_pid} ->
                send(client_pid, {output_name, :done})
              end)

              %{}

            _more ->
              gm.asks
          end

        {:noreply, %{gm | tasks: tasks, waiting_tasks: waiting_tasks, asks: asks}}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, gm) do
    # The task failed
    {name, _} = Enum.find(gm.tasks, fn {_name, task_pid} -> task_pid == pid end)
    stream = Map.get(gm.input_streams, name)

    task =
      run_stream_in_task(
        {name, stream},
        {name_or_pid(gm), gm.outputs, gm.chunk},
        gm.process_chunk
      )

    send(task.pid, {:run_task, gm.accs[name]})
    {:noreply, %{gm | tasks: Map.put(gm.tasks, name, task.pid)}}
  end
end
