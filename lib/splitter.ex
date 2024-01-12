defmodule Strom.Splitter do
  alias Strom.GenMix

  defstruct [:opts, :flow_pid, :sup_pid]

  def new(input, partitions, opts \\ []) do
    unless is_map(partitions) and map_size(partitions) > 0 do
      raise "Branches in splitter must be a map, given: #{inspect(partitions)}"
    end

    %Strom.DSL.Split{
      input: input,
      partitions: partitions,
      opts: opts
    }
  end

  def start(args \\ [])

  def start(%__MODULE__{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid}) do
    gen_mix = %GenMix{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid}
    GenMix.start(gen_mix)
  end

  def start(opts) when is_list(opts) do
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
