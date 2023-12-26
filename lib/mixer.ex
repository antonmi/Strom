defmodule Strom.Mixer do
  alias Strom.GenMix

  def start(opts \\ []) when is_list(opts) do
    GenMix.start(opts)
  end

  def call(flow, %GenMix{} = mix, to_mix, name) when is_map(flow) and is_list(to_mix) do
    inputs =
      Enum.reduce(to_mix, %{}, fn name, acc ->
        Map.put(acc, name, fn _el -> true end)
      end)

    outputs = %{name => fn _el -> true end}

    GenMix.call(flow, mix, inputs, outputs)
  end

  def call(flow, %GenMix{} = mix, to_mix, name) when is_map(flow) and is_map(to_mix) do
    outputs = %{name => fn _el -> true end}
    GenMix.call(flow, mix, to_mix, outputs)
  end

  def stop(%GenMix{} = mix), do: GenMix.stop(mix)
end
