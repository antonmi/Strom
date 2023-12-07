defmodule Strom.Module do
  defstruct module: nil, opts: [], state: nil

  def start(module, opts \\ []) do
    state = apply(module, :start, [opts])
    %__MODULE__{module: module, state: state, opts: opts}
  end

  def call(flow, %__MODULE__{module: module, state: state, opts: opts}, names)
      when is_map(flow) and is_list(names) do
    streams =
      Enum.reduce(names, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    sub_flows =
      Enum.reduce(streams, %{}, fn {name, stream}, acc ->
        stream =
          if is_pipeline_module?(module) do
            apply(module, :stream, [stream])
          else
            Stream.transform(stream, state, fn el, acc ->
              apply(module, :call, [el, acc, opts])
            end)
          end

        Map.put(acc, name, stream)
      end)

    Map.merge(flow, sub_flows)
  end

  def call(flow, %__MODULE__{} = state, name) do
    call(flow, state, [name])
  end

  defp is_pipeline_module?(module) when is_atom(module) do
    is_list(module.alf_components())
  rescue
    _error -> false
  end

  def stop(%__MODULE__{module: module, state: state, opts: opts}) do
    if is_pipeline_module?(module) do
      apply(module, :stop, [])
    else
      apply(module, :stop, [state, opts])
    end
  end
end
