defmodule Strom.GenMix.Streams do
  @moduledoc "Utility module. There are functions for manipulating data in gen_mix"

  alias Strom.GenMix

  def call(flow, gm) do
    input_streams =
      Enum.reduce(gm.inputs, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    {gm_pid, _new_tasks} = GenMix.start_tasks(gm.pid, input_streams)

    sub_flow = build_sub_flow(gm.outputs, gm_pid)

    flow = Map.drop(flow, gm.inputs)

    Enum.reduce(sub_flow, flow, fn {name, stream}, flow ->
      case Map.get(flow, name) do
        nil -> Map.put(flow, name, stream)
        existing_stream -> Map.put(flow, name, Stream.concat(existing_stream, stream))
      end
    end)
  end

  defp build_sub_flow(outputs, gm_pid) do
    Enum.reduce(outputs, %{}, fn {output_name, _fun}, flow ->
      stream =
        Stream.resource(
          fn ->
            :ok = GenMix.run_tasks(gm_pid, output_name)
            gm_pid
          end,
          fn gm_pid ->
            ask_and_wait(gm_pid, output_name)
          end,
          fn gm_pid ->
            gm_pid
          end
        )

      Map.put(flow, output_name, stream)
    end)
  end

  defp ask_and_wait(gm_pid, output_name) do
    GenServer.cast(gm_pid, {:get_data, {output_name, self()}})

    receive do
      {:client, ^output_name, {:data, output_data}} ->
        {output_data, gm_pid}

      {:client, ^output_name, :done} ->
        {:halt, gm_pid}
    end
  end

  def new_data(new_data, outputs, gm_data, waiting_clients) do
    Enum.reduce(
      outputs,
      {gm_data, waiting_clients, 0},
      fn {output_name, _fun}, {all_data, waiting, count} ->
        data_for_output = Map.get(all_data, output_name, []) ++ Map.get(new_data, output_name, [])

        if length(data_for_output) > 0 do
          case Enum.filter(waiting, fn {_, name} -> name == output_name end) do
            [] ->
              {Map.put(all_data, output_name, data_for_output), waiting,
               count + length(data_for_output)}

            clients ->
              {client_pid, _} = Enum.random(clients)
              send(client_pid, {:client, output_name, {:data, data_for_output}})
              {Map.put(all_data, output_name, []), Map.delete(waiting, client_pid), count}
          end
        else
          {all_data, waiting, count}
        end
      end
    )
  end
end
