defmodule Strom.Source.IOGets do
  @moduledoc "Source for reading IO inputs"
  @behaviour Strom.Source

  defstruct []

  def new, do: %__MODULE__{}

  @impl true
  def start(%__MODULE__{} = io_gets), do: io_gets

  @impl true
  def call(%__MODULE__{} = io_gets) do
    data = IO.gets("IOGets> ")
    {[String.trim(data)], io_gets}
  end

  @impl true
  def stop(%__MODULE__{} = io_gets), do: io_gets
end
