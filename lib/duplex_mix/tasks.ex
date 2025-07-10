defmodule Strom.DuplexMix.Tasks do
  @moduledoc "Utility module. There are fucntions for manipulating tasks in duplex_mix"
  alias Strom.DuplexMix
  alias Strom.Composite

  def start_tasks(input_streams, %DuplexMix{} = gm, side) do
    Enum.reduce(input_streams, %{}, fn {name, stream}, acc ->
      task_pid =
        run_stream_in_task(
          {name, stream},
          {gm.pid, gm.outputs, gm.chunk},
          gm.process_chunk,
          gm.composite,
          side
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

  def handle_task_error(%DuplexMix{} = gm, pid) do
    {left_tasks, right_tasks} = gm.tasks

    {name, stream, side} =
      case Enum.find(left_tasks, fn {task_pid, _name} -> task_pid == pid end) do
        {_, name} ->
          {name, Map.get(gm.input_streams, name), :left}

        nil ->
          {_, name} = Enum.find(right_tasks, fn {task_pid, _name} -> task_pid == pid end)
          {name, Map.get(gm.input_streams, name), :right}
      end

    task_pid =
      run_stream_in_task(
        {name, stream},
        {gm.pid, gm.outputs, gm.chunk},
        gm.process_chunk,
        gm.composite,
        side
      )

    send(task_pid, {:task, :run, gm.accs[name]})
    {side, task_pid, name}
  end

  def terminate_tasks(tasks) do
    Enum.each(
      Map.keys(tasks),
      &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1)
    )
  end

  defp run_stream_in_task(
         {name, stream},
         {gm_identifier, outputs, chunk},
         process_chunk,
         nil,
         side
       ) do
    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
        task_function({name, stream}, {gm_identifier, outputs, chunk}, process_chunk, side)
      )

    task.pid
  end

  defp run_stream_in_task(
         {name, stream},
         {gm_identifier, outputs, chunk},
         process_chunk,
         composite_name,
         side
       ) do
    supervisor_name = Composite.task_supervisor_name(composite_name)

    {:ok, task_pid} =
      DynamicSupervisor.start_child(
        supervisor_name,
        %{
          id: Task,
          start:
            {Task, :start_link,
             [task_function({name, stream}, {gm_identifier, outputs, chunk}, process_chunk, side)]},
          restart: :temporary
        }
      )

    Process.unlink(task_pid)
    Process.monitor(task_pid)

    task_pid
  end

  defp task_function({name, stream}, {gm_pid, outputs, chunk}, process_chunk, side) do
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
          outputs =
            case side do
              :left -> elem(outputs, 0)
              :right -> elem(outputs, 1)
            end

          case process_chunk.(name, chunk, outputs, acc) do
            {{new_data_forward, any_data_forward?, new_acc},
             {new_data_backward, any_data_backward?, new_acc}}
            when any_data_forward? or any_data_backward? ->
              case side do
                :left ->
                  # TODO do it in one call
                  if any_data_forward? do
                    GenServer.cast(
                      gm_pid,
                      {:put_data_from_left, {name, self()}, {new_data_forward, new_acc}}
                    )
                  end

                  if any_data_backward? do
                    GenServer.cast(
                      gm_pid,
                      {:put_data_from_right, {name, self()}, {new_data_backward, new_acc}}
                    )
                  end

                :right ->
                  # TODO do it in one call
                  if any_data_forward? do
                    GenServer.cast(
                      gm_pid,
                      {:put_data_from_right, {name, self()}, {new_data_forward, new_acc}}
                    )
                  end

                  if any_data_backward? do
                    GenServer.cast(
                      gm_pid,
                      {:put_data_from_left, {name, self()}, {new_data_backward, new_acc}}
                    )
                  end
              end

              receive do
                {:task, :halt} ->
                  {:halt, {gm_pid, new_acc}}

                {:task, :run_new_tasks_and_halt, {task_pid, acc}} ->
                  send(task_pid, {:task, :run, acc})
                  {:halt, {gm_pid, new_acc}}

                {:task, :run, _acc} ->
                  # ignore the message
                  {[], {gm_pid, new_acc}}

                {:task, ^name, :continue} ->
                  {[], {gm_pid, new_acc}}
              end

            {{_, false, new_acc}, {_, false, new_acc}} ->
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
