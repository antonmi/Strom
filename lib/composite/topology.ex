defmodule Strom.Composite.Topology do
  @moduledoc """
  Draws a topology of a composite
  """
  alias Strom.{Composite, Mixer, Splitter, Source, Transformer, Sink}

  @info_width 70

  @spec draw(Composite.t()) :: :ok
  def draw(%Composite{} = composite) do
    IO.puts("")
    components = refresh_components(composite)

    {streams, _} =
      Enum.reduce(components, {[], 0}, fn %{inputs: inputs, outputs: outputs} = component,
                                          {streams, index} ->
        {inputs, outputs} =
          case component do
            %Source{} -> {[], Map.keys(outputs)}
            %Sink{} -> {inputs, []}
            _ -> {inputs, Map.keys(outputs)}
          end

        started = inputs -- streams
        streams = draw_line(index, component, streams ++ started, inputs, outputs)

        {streams, index + 1}
      end)

    draw_stream_names(streams)
    draw_streams(streams, [])
    :ok
  end

  defp refresh_components(composite) do
    if is_pid(composite.pid) do
      Composite.components(composite)
    else
      composite.components
    end
  end

  defp draw_line(index, component, streams_after_inputs, inputs, outputs) do
    draw_stream_names(streams_after_inputs)
    input_positions = draw_streams(streams_after_inputs, inputs)
    average_position = average_position(input_positions)

    streams_after_outputs =
      find_place_for_outputs(streams_after_inputs, inputs, outputs, average_position)

    draw_component_description(index, component)
    draw_component(component, streams_after_outputs, outputs, average_position)

    streams_after_outputs
  end

  defp average_position(input_positions) do
    case input_positions do
      [] ->
        0

      positions when is_list(positions) ->
        round(Enum.sum(positions) / length(positions))
    end
  end

  defp find_place_for_outputs(streams, inputs, outputs, average_position) do
    ended = inputs -- outputs

    streams =
      Enum.reduce(streams, [], fn name, acc ->
        case Enum.member?(ended, name) do
          true ->
            [nil | acc]

          false ->
            [name | acc]
        end
      end)
      |> Enum.reverse()

    Enum.reduce(outputs, streams, fn output, acc ->
      case Enum.member?(streams, output) do
        true ->
          acc

        false ->
          nils =
            Enum.zip(acc, 0..length(acc))
            |> Enum.filter(fn {name, _} -> is_nil(name) end)

          case nils do
            [] ->
              [output | acc]

            nils when is_list(nils) ->
              {nil, closest_to_average} =
                Enum.min_by(nils, fn {_, index} -> abs(index - average_position) end)

              List.replace_at(acc, closest_to_average, output)
          end
      end
    end)
  end

  defp draw_stream_names(streams) do
    string =
      streams
      |> Enum.filter(& &1)
      |> Enum.map(&to_string/1)
      |> Enum.join(" ")

    IO.write(format_to_width("\e[3m#{string}\e[0m", @info_width))
  end

  defp draw_streams(streams, inputs) do
    {_, input_positions} =
      Enum.reduce(streams, {0, []}, fn name, {counter, acc} ->
        cond do
          Enum.member?(inputs, name) ->
            IO.write("\u275A ")
            {counter + 1, [counter | acc]}

          is_nil(name) ->
            IO.write("  ")
            {counter + 1, acc}

          true ->
            IO.write("| ")
            {counter + 1, acc}
        end
      end)

    IO.puts("")
    input_positions
  end

  defp draw_component_description(index, component) do
    case Keyword.get(component.opts, :label, nil) do
      nil ->
        case component do
          %Mixer{} ->
            IO.write(format_to_width("\e[1mMixer (#{index})\e[0m", @info_width))

          %Splitter{} ->
            IO.write(format_to_width("\e[1mSplitter (#{index})\e[0m", @info_width))

          %Transformer{} ->
            IO.write(format_to_width("\e[1mTransformer (#{index})\e[0m", @info_width))

          %Source{} ->
            IO.write(format_to_width("\e[1mSource (#{index})\e[0m", @info_width))

          %Sink{} ->
            IO.write(format_to_width("\e[1mSink (#{index})\e[0m", @info_width))
        end

      label ->
        IO.write(format_to_width("\e[1m#{label} (#{index})\e[0m", @info_width))
    end
  end

  defp draw_component(component, streams_after_outputs, outputs, average_position) do
    Enum.with_index(streams_after_outputs, fn name, index ->
      if index == average_position do
        component_character(component)
      else
        if name do
          if Enum.member?(outputs, name) do
            IO.write(". ")
          else
            IO.write("| ")
          end
        else
          IO.write("  ")
        end
      end
    end)

    IO.puts("")
  end

  defp component_character(%Mixer{}), do: IO.write("Y ")
  defp component_character(%Splitter{}), do: IO.write("\u039B ")
  defp component_character(%Transformer{}), do: IO.write("\u23FA ")
  defp component_character(%Source{}), do: IO.write("\u25BC ")
  defp component_character(%Sink{}), do: IO.write("\u25B2 ")

  defp format_to_width(string, width) do
    string = Enum.join(List.duplicate(" ", width), "") <> string
    String.slice(string, -(width - 2), width - 2) <> "  "
  end
end
