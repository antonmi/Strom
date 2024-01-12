defmodule Strom.Renamer do
  defstruct names: %{}

  def start(names) when is_map(names) do
    %__MODULE__{names: names}
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
