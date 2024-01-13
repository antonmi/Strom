defmodule Strom.Mixer do
  alias Strom.GenMix

  defstruct pid: nil,
            inputs: [],
            output: nil,
            opts: []

  def new(inputs, output)
      when is_list(inputs) or (is_map(inputs) and map_size(inputs) > 0) do
    %__MODULE__{inputs: inputs, output: output}
  end

  def start(%__MODULE__{inputs: inputs, output: output} = mixer, opts \\ []) do
    inputs =
      if is_list(inputs) do
        Enum.reduce(inputs, %{}, fn name, acc ->
          Map.put(acc, name, fn _el -> true end)
        end)
      else
        inputs
      end

    outputs = %{output => fn _el -> true end}

    gen_mix = %GenMix{
      inputs: inputs,
      outputs: outputs,
      opts: opts
    }

    {:ok, pid} = GenMix.start(gen_mix)
    %{mixer | pid: pid, opts: opts}
  end

  def call(flow, %__MODULE__{pid: pid}) do
    GenMix.call(flow, pid)
  end

  def stop(%__MODULE__{pid: pid}), do: GenMix.stop(pid)
end
