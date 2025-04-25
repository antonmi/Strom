defmodule Strom.Source.ReadLines do
  @moduledoc "Source for reading a file line by line"
  @behaviour Strom.Source

  defstruct path: nil, file: nil

  def new(path) when is_binary(path), do: %__MODULE__{path: path}

  @impl true
  def start(%__MODULE__{} = read_lines), do: %{read_lines | file: File.open!(read_lines.path)}

  @impl true
  def call(%__MODULE__{} = read_lines) do
    case read_line(read_lines.file) do
      {:ok, data} ->
        {[String.trim(data)], read_lines}

      {:error, :eof} ->
        {:halt, read_lines}
    end
  end

  @impl true
  def stop(%__MODULE__{} = read_lines), do: %{read_lines | file: File.close(read_lines.file)}

  defp read_line(file) do
    case IO.read(file, :line) do
      data when is_binary(data) ->
        {:ok, data}

      :eof ->
        {:error, :eof}

      {:error, :terminated} ->
        {:error, :eof}

      {:error, reason} ->
        raise reason
    end
  end
end
