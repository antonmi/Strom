defmodule Strom.MixerTree do
  @moduledoc "Composite of mixers, use it when you need mixing a lot of streams"
  alias Strom.Composite
  alias Strom.Mixer

  @type event() :: any()

  @spec new(
          [Strom.stream_name()] | %{Strom.stream_name() => (event() -> as_boolean(any))},
          Strom.stream_name(),
          list()
        ) :: Composite.t()

  @parts 2

  def new(inputs, output, opts \\ [])
      when is_list(inputs) or (is_map(inputs) and map_size(inputs) > 0 and is_list(opts)) do
    {parts, opts} = Keyword.pop(opts, :parts, @parts)
    mixers = build_mixers(inputs, 0, parts, output, opts)
    Composite.new(mixers)
  end

  defp build_mixers(inputs, level, parts, final_output, opts) do
    {mixers, outputs, count} =
      inputs
      |> Enum.chunk_every(parts)
      |> Enum.reduce({[], [], 0}, fn stream_names, {acc, outputs, counter} ->
        output = String.to_atom("_mt_#{level}#{counter}")
        mixer = Mixer.new(stream_names, output, opts)
        {[mixer | acc], [output | outputs], counter + 1}
      end)

    mixers = Enum.reverse(mixers)
    outputs = Enum.reverse(outputs)

    if count > parts do
      mixers ++ build_mixers(outputs, level + 1, parts, final_output, opts)
    else
      mixers ++ [Mixer.new(outputs, final_output)]
    end
  end
end
