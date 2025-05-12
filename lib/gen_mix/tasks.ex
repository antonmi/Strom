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
      send(task_pid, {:task, :run, accs[name]})
    end)

    tasks
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

    send(task_pid, {:task, :run, gm.accs[name]})
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
          {:task, :run, acc} ->
            acc
        end

      stream
      |> Stream.chunk_every(chunk)
      |> Stream.transform(
        fn -> {gm_pid, acc} end,
        fn chunk, {gm_pid, acc} ->
          case process_chunk.(name, chunk, outputs, acc) do
            {new_data, true, new_acc} ->
              GenServer.cast(gm_pid, {:put_data, {name, self()}, {new_data, new_acc}})

              receive do
                {:task, ^name, :continue} ->
                  {[], {gm_pid, new_acc}}

                {:task, :halt} ->
                  {:halt, {gm_pid, new_acc}}

                {:task, :run, _acc} = message ->
                  # each client tries to run tasks, so we need to flush the message
                  flush(message)
                  {[], {gm_pid, new_acc}}

                {:task, :run_new_tasks_and_halt, {task_pid, acc}} ->
                  send(task_pid, {:task, :run, acc})
                  {:halt, {gm_pid, new_acc}}
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

  defp flush(message) do
    receive do
      ^message ->
        flush(message)
    after
      0 ->
        :ok
    end
  end
end
