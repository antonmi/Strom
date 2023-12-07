defmodule Strom.Function do
  defstruct function: nil

  def start(function) do
    %__MODULE__{function: function}
  end

  def call(flow, %__MODULE__{function: function}, names)
      when is_map(flow) and is_function(function) and is_list(names) do
    streams =
      Enum.reduce(names, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    sub_flows =
      Enum.reduce(streams, %{}, fn {name, stream}, acc ->
        stream = Stream.map(stream, &function.(&1))
        Map.put(acc, name, stream)
      end)

    Map.merge(flow, sub_flows)
  end

  def call(flow, %__MODULE__{function: function}, name) do
    call(flow, %__MODULE__{function: function}, [name])
  end

  def stop(%__MODULE__{}), do: :ok
end
