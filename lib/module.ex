defmodule Strom.Module do
  use GenServer

  defstruct module: nil, opts: [], state: nil

  def start(module, opts \\ []) do
    state = apply(module, :start, [opts])
    %__MODULE__{module: module, state: state, opts: opts}
  end

  def stream(flow, %__MODULE__{module: module, state: state}, names)
      when is_map(flow) and is_list(names) do
    streams = Map.take(flow, names)

    sub_flows =
      Enum.reduce(streams, %{}, fn {name, stream}, acc ->
        stream = apply(module, :stream, [stream, state])
        Map.put(acc, name, stream)
      end)

    Map.merge(flow, sub_flows)
  end

  def stream(flow, %__MODULE__{} = state, name) do
    stream(flow, state, [name])
  end

  def stop(%__MODULE__{module: module, state: state}) do
    apply(module, :stop, [state])
  end
end
