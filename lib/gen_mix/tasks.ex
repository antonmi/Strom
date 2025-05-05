defmodule Strom.GenMix.Tasks do
  @moduledoc "Utility module. There are fucntions for manipulating tasks in gen_mix"

  alias Strom.GenMix
  alias Strom.Composite

  def start_tasks(input_streams, %GenMix{} = gm) do
    Enum.reduce(input_streams, %{}, fn {name, stream}, acc ->
      task_pid =
        run_stream_in_task(
          {name, stream},
          {GenMix.name_or_pid(gm), gm.outputs, gm.chunk},
          gm.process_chunk,
          gm.composite
        )

      Map.put(acc, name, task_pid)
    end)
  end

  def run_tasks(tasks, accs) do
    Enum.each(tasks, fn {name, task_pid} ->
      send(task_pid, {:run_task, accs[name]})
    end)

    tasks
  end

  def send_to_tasks(tasks, message) do
    Enum.each(tasks, &send(elem(&1, 1), message))
  end

  def handle_normal_task_stop(ref, pid, gm) do
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

  def handle_task_error(%GenMix{} = gm, pid) do
    {name, _} = Enum.find(gm.tasks, fn {_name, task_pid} -> task_pid == pid end)
    stream = Map.get(gm.input_streams, name)

    task_pid =
      run_stream_in_task(
        {name, stream},
        {GenMix.name_or_pid(gm), gm.outputs, gm.chunk},
        gm.process_chunk,
        gm.composite
      )

    send(task_pid, {:run_task, gm.accs[name]})
    {name, task_pid}
  end

  def terminate_tasks(tasks) do
    Enum.each(
      Map.values(tasks),
      &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1)
    )
  end

  defp run_stream_in_task({name, stream}, {gm_identifier, outputs, chunk}, process_chunk, nil) do
    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
        task_function({name, stream}, {gm_identifier, outputs, chunk}, process_chunk)
      )

    task.pid
  end

  defp run_stream_in_task(
         {name, stream},
         {gm_identifier, outputs, chunk},
         process_chunk,
         {composite_name, _gm_ref}
       ) do
    supervisor_name = Composite.task_supervisor_name(composite_name)

    {:ok, task_pid} =
      DynamicSupervisor.start_child(
        supervisor_name,
        %{
          id: Task,
          start:
            {Task, :start_link,
             [task_function({name, stream}, {gm_identifier, outputs, chunk}, process_chunk)]},
          restart: :temporary
        }
      )

    Process.unlink(task_pid)
    Process.monitor(task_pid)

    task_pid
  end

  defp task_function({name, stream}, {gm_identifier, outputs, chunk}, process_chunk) do
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
  end
end
