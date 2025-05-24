defmodule Strom.SplitterTree do
  @moduledoc "Composite of mixers, use it when you need mixing a lot of streams"
  alias Strom.Composite
  alias Strom.Splitter

  @type event() :: any()
  @parts 2
  @modes [:copy, :hash]
  @default_mode :copy

  @spec new(Strom.stream_name(), [Strom.stream_name()], list()) :: Composite.t()
  def new(input, outputs, opts \\ [])
      when is_atom(input) and is_list(outputs) and is_list(opts) do
    {mode, opts} = define_mode(opts)
    {parts, opts} = Keyword.pop(opts, :parts, @parts)
    splitters = build_splitters(input, outputs, 0, parts, opts, mode)
    Composite.new(splitters)
  end

  defp build_splitters(input, outputs, level, parts, opts, mode) do
    {splitters, local_inputs, count} =
      outputs
      |> Enum.chunk_every(parts)
      |> Enum.reduce({[], [], 0}, fn stream_names, {acc, local_inputs, counter} ->
        local_input = String.to_atom("_st_#{level}#{counter}")
        local_outputs = build_local_outputs(stream_names, mode)
        splitter = Splitter.new(local_input, local_outputs, opts)
        {[splitter | acc], [local_input | local_inputs], counter + 1}
      end)

    local_inputs = Enum.reverse(local_inputs)
    splitters = Enum.reverse(splitters)

    if count > parts do
      build_splitters(input, local_inputs, level + 1, parts, opts, mode) ++ splitters
    else
      local_outputs = build_local_outputs(local_inputs, mode)
      [Splitter.new(input, local_outputs, opts) | splitters]
    end
  end

  defp define_mode(opts) do
    {mode, opts} = Keyword.pop(opts, :mode, @default_mode)

    if mode not in @modes do
      raise "Mode #{mode} must be in #{@modes}"
    end

    {mode, opts}
  end

  defp build_local_outputs(stream_names, :copy) do
    stream_names
  end

  defp build_local_outputs(stream_names, :hash) do
    {local_outputs, _} =
      Enum.reduce(stream_names, {%{}, 0}, fn output, {acc, index} ->
        acc =
          Map.put(acc, output, fn event ->
            :erlang.phash2(event, length(stream_names)) == index
          end)

        {acc, index + 1}
      end)

    local_outputs
  end
end
