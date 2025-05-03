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
  """
  alias Strom.GenMix

  defstruct pid: nil,
            composite: nil,
            inputs: [],
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
      when is_list(inputs) and is_atom(output) and is_list(opts) do
    outputs = %{output => fn _el -> true end}

    %__MODULE__{inputs: inputs, outputs: outputs, opts: opts}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(
        %__MODULE__{inputs: inputs, outputs: outputs, opts: opts, composite: composite} = mixer
      ) do
    gen_mix =
      GenMix.start(%GenMix{
        inputs: inputs,
        outputs: outputs,
        opts: opts,
        process_chunk: &process_chunk/4,
        composite: composite
      })

    %{mixer | pid: gen_mix.pid}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{} = mixer) do
    GenMix.call(flow, mixer)
  end

  def process_chunk(_input_stream_name, chunk, outputs, nil) when map_size(outputs) == 1 do
    [stream_name] = Map.keys(outputs)
    {%{stream_name => chunk}, Enum.any?(chunk), nil}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = mixer), do: GenMix.stop(mixer)
end
