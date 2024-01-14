defmodule Strom.Splitter do
  @moduledoc """
  Split a stream into several streams by applying given functions on events

      ## Example
      iex> alias Strom.Splitter
      iex> outputs = %{s1: &(rem(&1, 2) == 0), s2: &(rem(&1, 2) == 1)}
      iex> splitter = :stream |> Splitter.new(outputs) |> Splitter.start()
      iex> %{s1: s1, s2: s2} = Splitter.call(%{stream: [1, 2, 3]}, splitter)
      iex> {Enum.to_list(s1), Enum.to_list(s2)}
      {[2], [1, 3]}

      ## Can also just duplicate a stream
      iex> alias Strom.Splitter
      iex> splitter = :stream |> Splitter.new([:s1, :s2]) |> Splitter.start()
      iex> %{s1: s1, s2: s2} = Splitter.call(%{stream: [1, 2, 3]}, splitter)
      iex> {Enum.to_list(s1), Enum.to_list(s2)}
      {[1, 2, 3], [1, 2, 3]}
  """
  alias Strom.GenMix

  defstruct pid: nil,
            input: nil,
            outputs: [],
            opts: []

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(
          Strom.stream_name(),
          [Strom.stream_name()] | %{Strom.stream_name() => (event() -> as_boolean(any))}
        ) :: __MODULE__.t()
  def new(input, outputs) when is_list(outputs) or (is_map(outputs) and map_size(outputs)) > 0 do
    %Strom.Splitter{input: input, outputs: outputs}
  end

  @spec start(__MODULE__.t(), buffer: integer()) :: __MODULE__.t()
  def start(
        %__MODULE__{input: input, outputs: outputs} =
          splitter,
        opts \\ []
      ) do
    inputs = %{input => fn _el -> true end}

    outputs =
      if is_list(outputs) do
        Enum.reduce(outputs, %{}, fn name, acc ->
          Map.put(acc, name, fn _el -> true end)
        end)
      else
        outputs
      end

    gen_mix = %GenMix{
      inputs: inputs,
      outputs: outputs,
      opts: opts
    }

    {:ok, pid} = GenMix.start(gen_mix)
    %{splitter | pid: pid, opts: opts}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{pid: pid}) do
    GenMix.call(flow, pid)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid}), do: GenMix.stop(pid)
end
