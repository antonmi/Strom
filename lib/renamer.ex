defmodule Strom.Renamer do
  defstruct names: %{}

  def new(names) when is_map(names) and map_size(names) > 0 do
    %__MODULE__{names: names}
  end

  def start(%__MODULE__{names: names} = renamer) when is_map(names) and map_size(names) > 0 do
    renamer
  end

  def call(flow, %__MODULE__{names: names}) do
    Enum.reduce(names, flow, fn {name, new_name}, acc ->
      acc
      |> Map.put(new_name, Map.fetch!(acc, name))
      |> Map.delete(name)
    end)
  end

  def stop(%__MODULE__{}), do: :ok
end
