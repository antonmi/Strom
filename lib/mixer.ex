defmodule Strom.Mixer do
  @moduledoc """
  Mix several streams into one. Use Strom.GenMix under the hood

      ## Example
      iex> alias Strom.Mixer
      iex> mixer = [:s1, :s2] |> Mixer.new(:stream) |> Mixer.start()
      iex> flow = %{s1: [1, 2, 3], s2: [4, 5, 6]}
      iex> %{stream: stream} = Mixer.call(flow, mixer)
      iex> stream |> Enum.to_list() |> Enum.sort()
      [1, 2, 3, 4, 5, 6]

      ## Can also accept a map with functions as values. Works like "filter".
      iex> alias Strom.Mixer
      iex> inputs = %{s1: &(rem(&1, 2) == 0), s2: &(rem(&1, 2) == 1)}
      iex> mixer = inputs |> Mixer.new(:stream) |> Mixer.start()
      iex> flow = %{s1: [1, 2, 3], s2: [4, 5, 6]}
      iex> %{stream: stream} = Mixer.call(flow, mixer)
      iex> stream |> Enum.to_list() |> Enum.sort()
      [2, 5]
  """
  alias Strom.GenMix

  defstruct pid: nil,
            inputs: %{},
            outputs: %{},
            opts: []

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(
          [Strom.stream_name()] | %{Strom.stream_name() => (event() -> as_boolean(any))},
          Strom.stream_name(),
          list()
        ) :: __MODULE__.t()
  def new(inputs, output, opts \\ [])
      when is_list(inputs) or (is_map(inputs) and map_size(inputs) > 0 and is_list(opts)) do
    inputs =
      if is_list(inputs) do
        Enum.reduce(inputs, %{}, fn name, acc ->
          Map.put(acc, name, fn _el -> true end)
        end)
      else
        inputs
      end

    outputs = %{output => fn _el -> true end}

    %__MODULE__{inputs: inputs, outputs: outputs, opts: opts}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{inputs: inputs, outputs: outputs, opts: opts} = mixer) do
    gen_mix =
      GenMix.start(%GenMix{
        inputs: inputs,
        outputs: outputs,
        opts: opts
      })

    %{mixer | pid: gen_mix.pid}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{} = mixer) do
    GenMix.call(flow, mixer)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = mixer), do: GenMix.stop(mixer)
end
