defmodule Strom.Sink.Null do
  @behaviour Strom.Sink

  defstruct []

  def new, do: %__MODULE__{}

  @impl true
  def start(%__MODULE__{} = null), do: null

  @impl true
  def call(%__MODULE__{} = null, _data), do: null

  @impl true
  def stop(%__MODULE__{} = null), do: null
end
