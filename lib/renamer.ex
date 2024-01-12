defmodule Strom.Renamer do
  defstruct []

  def start() do
    %__MODULE__{}
  end

  def call(flow, names) when is_map(names) do
    Enum.reduce(names, flow, fn {name, new_name}, acc ->
      acc
      |> Map.put(new_name, Map.fetch!(acc, name))
      |> Map.delete(name)
    end)
  end

  def stop(%__MODULE__{}), do: :ok
end
