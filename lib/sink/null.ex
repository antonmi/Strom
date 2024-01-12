defmodule Strom.Sink.Null do
  @behaviour Strom.Sink

  defstruct []

  @impl true
  def start(%__MODULE__{} = null), do: null

  @impl true
  def call(%__MODULE__{} = null, _data), do: {:ok, {[], null}}

  @impl true
  def stop(%__MODULE__{} = null), do: null
end
