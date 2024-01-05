defmodule Strom.Splitter do
  alias Strom.GenMix

  def start(opts \\ []) when is_list(opts) do
    GenMix.start(opts)
  end

  def call(flow, %GenMix{} = mix, name, partitions) when is_list(partitions) do
    inputs = %{name => fn _el -> true end}

    outputs =
      Enum.reduce(partitions, %{}, fn name, acc ->
        Map.put(acc, name, fn _el -> true end)
      end)

    GenMix.call(flow, mix, inputs, outputs)
  end

  def call(flow, %GenMix{} = mix, name, partitions) when is_map(partitions) do
    inputs = %{name => fn _el -> true end}
    GenMix.call(flow, mix, inputs, partitions)
  end

  def stop(%GenMix{} = mix), do: GenMix.stop(mix)
end
