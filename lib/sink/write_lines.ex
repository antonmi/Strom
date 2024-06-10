defmodule Strom.Sink.WriteLines do
  @behaviour Strom.Sink

  @line_sep "\n"

  defstruct path: nil, file: nil, line_sep: @line_sep

  def new(path, line_sep \\ @line_sep) when is_binary(path) and is_binary(line_sep) do
    %__MODULE__{path: path, line_sep: line_sep}
  end

  @impl true
  def start(%__MODULE__{} = write_lines) do
    file = File.open!(write_lines.path, [:write])
    %{write_lines | file: file}
  end

  @impl true
  def call(%__MODULE__{} = write_lines, data) do
    :ok = IO.write(write_lines.file, data <> write_lines.line_sep)

    write_lines
  end

  @impl true
  def stop(%__MODULE__{} = write_lines) do
    %{write_lines | file: File.close(write_lines.file)}
  end
end
