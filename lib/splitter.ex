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
            composite: nil,
            inputs: %{},
            outputs: %{},
            opts: []

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(
          Strom.stream_name(),
          [Strom.stream_name()] | %{Strom.stream_name() => (event() -> as_boolean(any))},
          list()
        ) :: __MODULE__.t()
  def new(input, outputs, opts \\ [])

  def new(input, outputs, opts) when is_list(outputs) and is_list(opts) do
    outputs =
      Enum.reduce(outputs, %{}, fn name, acc ->
        Map.put(acc, name, fn _el -> true end)
      end)

    %__MODULE__{inputs: [input], outputs: outputs, opts: opts}
  end

  def new(input, outputs, opts)
      when is_map(outputs) and map_size(outputs) > 0 and is_list(opts) do
    %__MODULE__{inputs: [input], outputs: outputs, opts: opts}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(
        %__MODULE__{inputs: inputs, outputs: outputs, opts: opts, composite: composite} = splitter
      ) do
    gen_mix =
      GenMix.start(%GenMix{
        inputs: inputs,
        outputs: outputs,
        opts: opts,
        process_chunk: &process_chunk/4,
        composite: composite
      })

    %{splitter | pid: gen_mix.pid}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{} = splitter) do
    GenMix.call(flow, splitter)
  end

  def process_chunk(_input_stream_name, chunk, outputs, nil) do
    outputs
    |> Enum.reduce({%{}, false, nil}, fn {stream_name, fun}, {acc, any?, nil} ->
      {data, _} = Enum.split_with(chunk, fun)
      {Map.put(acc, stream_name, data), any? || Enum.any?(data), nil}
    end)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = splitter), do: GenMix.stop(splitter)
end
