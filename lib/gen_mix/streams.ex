defmodule Strom.GenMix.Streams do
  @moduledoc "Utility module. There are functions for manipulating data in gen_mix"

  alias Strom.GenMix
  alias Strom.GenMix.Tasks

  def call(flow, gm) do
    input_streams =
      Enum.reduce(gm.inputs, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    {:ok, gm_identifier} = GenServer.call(gm.pid, {:start_tasks, input_streams})

    sub_flow = build_sub_flow(gm.outputs, gm_identifier)

    flow
    |> Map.drop(gm.inputs)
    |> Map.merge(sub_flow)
  end

  def process_new_data(new_data, gm_data, asks) do
    Enum.reduce(new_data, {gm_data, asks, 0}, fn {output_name, data}, {all_data, asks, count} ->
      data_for_output = Map.get(all_data, output_name, []) ++ data

      asks_for_output = Enum.filter(asks, fn {_, output} -> output == output_name end)

      {data_for_output, asks} =
        case {data_for_output, asks_for_output} do
          {[], _} ->
            {data_for_output, asks}

          {data_for_output, []} ->
            {data_for_output, asks}

          {data_for_output, asks_for_output} ->
            {client_pid, output_name} = Enum.random(asks_for_output)
            send(client_pid, {output_name, data_for_output})
            {[], Map.delete(asks, client_pid)}
        end

      {Map.put(all_data, output_name, data_for_output), asks, count + length(data_for_output)}
    end)
  end

  def continue_or_wait({task_pid, name}, {_tasks, waiting_tasks}, {total_count, buffer}) do
    if total_count < buffer do
      send(task_pid, :continue_task)
      waiting_tasks
    else
      Map.put(waiting_tasks, task_pid, name)
    end
  end

  def handle_ask(output_name, client_pid, %GenMix{} = gm) do
    {asks, new_data, data_size_for_output} =
      case Map.get(gm.data, output_name, []) do
        [] ->
          if map_size(gm.tasks) == 0 do
            send(client_pid, {output_name, :done})
            {Map.delete(gm.asks, client_pid), gm.data, 0}
          else
            {Map.put(gm.asks, client_pid, output_name), gm.data, 0}
          end

        events ->
          send(client_pid, {output_name, events})

          {Map.delete(gm.asks, client_pid), Map.put(gm.data, output_name, []), length(events)}
      end

    new_data_size = gm.data_size - data_size_for_output

    waiting_tasks =
      if new_data_size < gm.buffer do
        Tasks.send_to_tasks(gm.waiting_tasks, :continue_task)
        %{}
      else
        gm.waiting_tasks
      end

    %{
      gm
      | asks: asks,
        data: new_data,
        data_size: new_data_size,
        waiting_tasks: waiting_tasks
    }
  end

  def send_to_clients(asks, message) do
    Enum.each(asks, &send(elem(&1, 0), message))
  end

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

      :continue_ask ->
        ask_and_wait(gm_identifier, output_name)

      :halt_task ->
        # message for the task
        IO.inspect("message for task :halt_task in #{inspect(self())}")
        {:halt, gm_identifier}

      message ->
        raise "Unexpected message #{inspect(message)} in #{inspect(self())}"
    end
  end
end
