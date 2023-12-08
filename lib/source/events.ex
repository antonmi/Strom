defmodule Strom.Source.Events do
  @behaviour Strom.Source

  defstruct infinite: false, events: []

  @impl true
  def start(%__MODULE__{} = state), do: state

  @impl true
  def call(%__MODULE__{events: events} = state) do
    case events do
      [hd | tl] ->
        {:ok, {[hd], %{state | events: tl}}}

      [] ->
        {:error, {:halt, state}}
    end
  end

  @impl true
  def stop(%__MODULE__{} = stream), do: stream

  @impl true
  def infinite?(%__MODULE__{infinite: infinite}), do: infinite
end
