defmodule Strom.GenMix.Tasks do
  @moduledoc "Utility module. There are fucntions for manipulating tasks in gen_mix"

  alias Strom.GenMix
  alias Strom.Composite

  def start_tasks(input_streams, %GenMix{} = gm) do
    Enum.reduce(input_streams, %{}, fn {name, stream}, acc ->
      task_pid =
        run_stream_in_task(
          {name, stream},
          {gm.pid, gm.outputs, gm.chunk},
          gm.process_chunk,
          gm.composite
        )

      Map.put(acc, task_pid, name)
    end)
  end

  def run_tasks(tasks, accs) do
    Enum.each(tasks, fn {task_pid, name} ->
      send(task_pid, {:run_task, accs[name]})
    end)

    tasks
  end

  def send_to_tasks(tasks, message) do
    Enum.each(tasks, &send(elem(&1, 0), message))
  end

  def handle_normal_task_stop(ref, task_pid, gm) do
    Process.demonitor(ref, [:flush])

    {tasks, waiting_tasks} =
      if gm.no_wait do
        terminate_tasks(gm.tasks)
        {%{}, %{}}
      else
        {Map.delete(gm.tasks, task_pid), Map.delete(gm.waiting_tasks, task_pid)}
      end

    asks =
      case map_size(tasks) do
        0 ->
          # no tasks, sending the :done message to all asks
          Enum.each(gm.asks, fn {client_pid, output_name} ->
            send(client_pid, {output_name, :done})
          end)

          %{}

        _more ->
          gm.asks
      end

    # handling the "stopping" case
    if gm.stopping and map_size(tasks) == 0 and gm.data_size == 0 do
      # sending the halt_client message to all the seen clients
      # tasks are already stopped a this moment
      Enum.each(gm.clients, &send(&1, :halt_client))

      {:stop, :normal, %{gm | tasks: %{}}}
    else
      {:noreply, %{gm | tasks: tasks, waiting_tasks: waiting_tasks, asks: asks}}
    end
  end

  def handle_task_error(%GenMix{} = gm, pid) do
    {_, name} = Enum.find(gm.tasks, fn {task_pid, _name} -> task_pid == pid end)
    stream = Map.get(gm.input_streams, name)

    task_pid =
      run_stream_in_task(
        {name, stream},
        {gm.pid, gm.outputs, gm.chunk},
        gm.process_chunk,
        gm.composite
      )

    send(task_pid, {:run_task, gm.accs[name]})
    {task_pid, name}
  end

  def terminate_tasks(tasks) do
    Enum.each(
      Map.keys(tasks),
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
         composite_name
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

  defp task_function({name, stream}, {gm_pid, outputs, chunk}, process_chunk) do
    fn ->
      acc =
        receive do
          {:run_task, acc} ->
            acc
        end

      stream
      |> Stream.chunk_every(chunk)
      |> Stream.transform(
        fn -> {gm_pid, acc} end,
        fn chunk, {gm_pid, acc} ->
          case process_chunk.(name, chunk, outputs, acc) do
            {new_data, true, new_acc} ->
              GenServer.cast(gm_pid, {:new_data, {self(), name}, {new_data, new_acc}})

              receive do
                :continue_task ->
                  {[], {gm_pid, new_acc}}

                :halt_task ->
                  {:halt, {gm_pid, new_acc}}

                {_stream_name, :done} ->
                  # addressed to client, halt task
                  # it happens when component are deleted
                  {:halt, {gm_pid, new_acc}}

                :halt_client ->
                  # addressed to client, halt task
                  # it happens when component are deleted
                  {:halt, {gm_pid, new_acc}}

                message ->
                  # raising for now, there are probably some corner cases
                  raise "Unexpected message #{inspect(message)} in #{inspect(self())}"
              end

            {_new_data, false, new_acc} ->
              {[], {gm_pid, new_acc}}
          end
        end,
        fn {gm_pid, acc} -> {gm_pid, acc} end
      )
      |> Stream.run()

      self()
    end
  end
end
