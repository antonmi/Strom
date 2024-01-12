defmodule Strom.Mixer do
  alias Strom.GenMix

  defstruct [:gen_mix, :inputs, :output, :opts, :flow_pid, :sup_pid]

  def new(inputs, output, opts \\ []) do
    unless is_list(inputs) do
      raise "Mixer sources must be a list, given: #{inspect(inputs)}"
    end

    %__MODULE__{inputs: inputs, output: output, opts: opts}
  end

  def start(args \\ [])

  def start(%__MODULE__{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid} = mixer) do
    gen_mix = %GenMix{opts: opts, flow_pid: flow_pid, sup_pid: sup_pid}
    GenMix.start(gen_mix)
  end

  def start(opts) when is_list(opts) do
    GenMix.start(opts)
  end

  def call(flow, %__MODULE__{gen_mix: mix}, to_mix, name) when is_map(flow) and is_list(to_mix) do
    inputs =
      Enum.reduce(to_mix, %{}, fn name, acc ->
        Map.put(acc, name, fn _el -> true end)
      end)

    outputs = %{name => fn _el -> true end}

    GenMix.call(flow, mix, inputs, outputs)
  end

  def call(flow, %__MODULE__{gen_mix: mix}, to_mix, name) when is_map(flow) and is_map(to_mix) do
    outputs = %{name => fn _el -> true end}
    GenMix.call(flow, mix, to_mix, outputs)
  end

  def stop(%__MODULE__{gen_mix: mix}), do: GenMix.stop(mix)
end
