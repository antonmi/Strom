defmodule Strom.Sink.WriteLines do
  @behaviour Strom.Sink

  defstruct path: nil, file: nil

  @line_sep "\n"

  @impl true
  def start(%__MODULE__{} = write_lines) do
    file = File.open!(write_lines.path, [:write])
    %{write_lines | file: file}
  end

  @impl true
  def call(%__MODULE__{} = write_lines, data) do
    :ok = IO.write(write_lines.file, data <> @line_sep)

    {:ok, {[], write_lines}}
  end

  @impl true
  def stop(%__MODULE__{} = write_lines) do
    %{write_lines | file: File.close(write_lines.file)}
  end
end
