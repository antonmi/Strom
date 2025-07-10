defmodule Strom.DuplexMix.Streams do
  @moduledoc "Utility module. There are functions for manipulating data in duplex_mix"

  alias Strom.DuplexMix

  def call({left_flow, right_flow}, gm) do
    left_input_streams =
      Enum.reduce(elem(gm.inputs, 0), %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(left_flow, name))
      end)

    right_input_streams =
      Enum.reduce(elem(gm.inputs, 1), %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(right_flow, name))
      end)

    {gm_pid, _new_tasks} =
      DuplexMix.start_tasks(gm.pid, {left_input_streams, right_input_streams})

    left_output_names = Enum.uniq(Map.keys(elem(gm.outputs, 0)) ++ elem(gm.inputs, 1))
    right_output_names = Enum.uniq(Map.keys(elem(gm.outputs, 1)) ++ elem(gm.inputs, 0))

    left_sub_flow = build_sub_flow(left_output_names, gm_pid, :left)
    right_sub_flow = build_sub_flow(right_output_names, gm_pid, :right)

    left_flow = Map.drop(left_flow, elem(gm.inputs, 0))
    right_flow = Map.drop(right_flow, elem(gm.inputs, 1))

    left_flow =
      Enum.reduce(left_sub_flow, left_flow, fn {name, stream}, flow ->
        case Map.get(flow, name) do
          nil -> Map.put(flow, name, stream)
          existing_stream -> Map.put(flow, name, Stream.concat(existing_stream, stream))
        end
      end)

    right_flow =
      Enum.reduce(right_sub_flow, right_flow, fn {name, stream}, flow ->
        case Map.get(flow, name) do
          nil -> Map.put(flow, name, stream)
          existing_stream -> Map.put(flow, name, Stream.concat(existing_stream, stream))
        end
      end)

    {left_flow, right_flow}
  end

  defp build_sub_flow(output_names, gm_pid, side) do
    Enum.reduce(output_names, %{}, fn output_name, flow ->
      stream =
        Stream.resource(
          fn ->
            :ok = DuplexMix.run_tasks(gm_pid, output_name, side)
            gm_pid
          end,
          fn gm_pid ->
            ask_and_wait(gm_pid, output_name, side)
          end,
          fn gm_pid ->
            gm_pid
          end
        )

      Map.put(flow, output_name, stream)
    end)
  end

  defp ask_and_wait(gm_pid, output_name, side) do
    case side do
      :left -> GenServer.cast(gm_pid, {:get_left_data, {output_name, self()}})
      :right -> GenServer.cast(gm_pid, {:get_right_data, {output_name, self()}})
    end

    receive do
      {:client, ^output_name, {:data, output_data}} ->
        {output_data, gm_pid}

      {:client, ^output_name, :done} ->
        {:halt, gm_pid}
    end
  end

  def new_data(new_data, output_names, gm_data, waiting_clients) do
    Enum.reduce(
      output_names,
      {gm_data, waiting_clients, 0},
      fn output_name, {all_data, waiting, count} ->
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
