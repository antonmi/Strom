defmodule Strom.Splitter do
  alias Strom.GenMix

  defstruct pid: nil,
            input: nil,
            outputs: [],
            opts: []

  def new(input, outputs) when is_list(outputs) or (is_map(outputs) and map_size(outputs)) > 0 do
    %Strom.Splitter{input: input, outputs: outputs}
  end

  def start(
        %__MODULE__{input: input, outputs: outputs} =
          splitter,
        opts \\ []
      ) do
    inputs = %{input => fn _el -> true end}

    outputs =
      if is_list(outputs) do
        Enum.reduce(outputs, %{}, fn name, acc ->
          Map.put(acc, name, fn _el -> true end)
        end)
      else
        outputs
      end

    gen_mix = %GenMix{
      inputs: inputs,
      outputs: outputs,
      opts: opts
    }

    {:ok, pid} = GenMix.start(gen_mix)
    %{splitter | pid: pid, opts: opts}
  end

  def call(flow, %__MODULE__{pid: pid}) do
    GenMix.call(flow, pid)
  end

  def stop(%__MODULE__{pid: pid}), do: GenMix.stop(pid)
end
