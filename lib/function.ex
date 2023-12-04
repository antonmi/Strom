defmodule Strom.Function do
  use GenServer

  defstruct function: nil

  def start(function) do
    %__MODULE__{function: function}
  end

  def stream(flow, %__MODULE__{function: function}, names)
      when is_map(flow) and is_function(function) and is_list(names) do
    streams = Map.take(flow, names)

    sub_flows =
      Enum.reduce(streams, %{}, fn {name, stream}, acc ->
        Map.put(acc, name, function.(stream))
      end)

    Map.merge(flow, sub_flows)
  end

  def stream(flow, %__MODULE__{function: function}, name) do
    stream(flow, %__MODULE__{function: function}, [name])
  end

  def stop(%__MODULE__{}), do: :ok |> IO.inspect()
end
