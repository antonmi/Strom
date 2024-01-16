defmodule Strom.Source.IOGets do
  @behaviour Strom.Source

  defstruct infinite: false

  def new, do: %__MODULE__{}

  @impl true
  def start(%__MODULE__{} = io_gets), do: io_gets

  @impl true
  def call(%__MODULE__{} = io_gets) do
    data = IO.gets("IOGets> ")
    {:ok, {[String.trim(data)], io_gets}}
  end

  @impl true
  def stop(%__MODULE__{} = io_gets), do: io_gets

  @impl true
  def infinite?(%__MODULE__{infinite: infinite}), do: infinite
end
