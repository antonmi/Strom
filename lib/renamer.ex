defmodule Strom.Renamer do
  @moduledoc """
    Renames streams in flow.

    ## Example
    iex> alias Strom.Renamer
    iex> flow = %{s1: [1], s2: [2]}
    iex> renamer = %{s1: :foo1, s2: :foo2} |> Renamer.new() |> Renamer.start()
    iex> Renamer.call(flow, renamer)
    %{foo1: [1], foo2: [2]}
  """
  defstruct names: %{}

  @type t() :: %__MODULE__{}

  @spec new(map()) :: __MODULE__.t()
  def new(names) when is_map(names) and map_size(names) > 0 do
    %__MODULE__{names: names}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{names: names} = renamer) when is_map(names) and map_size(names) > 0 do
    renamer
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{names: names}) do
    Enum.reduce(names, flow, fn {name, new_name}, acc ->
      acc
      |> Map.put(new_name, Map.fetch!(acc, name))
      |> Map.delete(name)
    end)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{}), do: :ok
end
