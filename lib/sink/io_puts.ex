defmodule Strom.Sink.IOPuts do
  @behaviour Strom.Sink

  defstruct line_sep: "", prefix: ""

  def new(prefix \\ "", line_sep \\ ""), do: %__MODULE__{line_sep: line_sep, prefix: prefix}

  @impl true
  def start(%__MODULE__{} = io_puts), do: io_puts

  @impl true
  def call(%__MODULE__{} = io_puts, data) do
    IO.puts(io_puts.prefix <> "#{data}" <> io_puts.line_sep)

    {:ok, {[], io_puts}}
  end

  @impl true
  def stop(%__MODULE__{} = io_puts), do: io_puts
end
