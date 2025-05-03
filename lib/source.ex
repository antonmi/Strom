defmodule Strom.Source do
  @moduledoc """
  Produces stream of events.

      ## Example with Enumerable
      iex> alias Strom.Source
      iex> source = :numbers |> Source.new([1, 2, 3]) |> Source.start()
      iex> %{numbers: stream} = Source.call(%{}, source)
      iex> Enum.to_list(stream)
      [1, 2, 3]

      ## Example with file
      iex> alias Strom.{Source, Source.ReadLines}
      iex> source = :numbers |> Source.new(ReadLines.new("test/data/numbers1.txt")) |> Source.start()
      iex> %{numbers: stream} = Source.call(%{}, source)
      iex> Enum.to_list(stream)
      ["1", "2", "3", "4", "5"]

      ## If two sources are applied to one stream, the streams will be concatenated (Stream.concat/2)
      iex> alias Strom.{Source, Source.ReadLines}
      iex> source1 = :numbers |> Source.new([1, 2, 3]) |> Source.start()
      iex> source2 = :numbers |> Source.new(ReadLines.new("test/data/numbers1.txt")) |> Source.start()
      iex> %{numbers: stream} = %{} |> Source.call(source1) |> Source.call(source2)
      iex> Enum.to_list(stream)
      [1, 2, 3, "1", "2", "3", "4", "5"]

  Source defines a `@behaviour`. One can easily implement their own sources.
  See `Strom.Source.ReadLines`, `Strom.Source.Events`, `Strom.Source.IOGets`
  """

  @callback start(map) :: map
  @callback call(map) :: {[term], map} | {:halt, map} | no_return()
  @callback stop(map) :: map

  alias Strom.GenMix

  defstruct pid: nil,
            composite: nil,
            origin: nil,
            inputs: [],
            outputs: %{},
            opts: []

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(Strom.stream_name(), struct() | [event()] | Strom.stream(), list()) :: __MODULE__.t()
  def new(name, origin, opts \\ [])

  def new(input, origin, opts) when is_atom(input) and is_list(origin) and is_list(opts) do
    %__MODULE__{
      origin: Stream.concat([], origin),
      inputs: [input],
      outputs: %{input => fn _ -> true end},
      opts: opts
    }
  end

  def new(input, origin, opts)
      when is_atom(input) and (is_struct(origin) or is_function(origin, 2)) and is_list(opts) do
    %__MODULE__{
      origin: origin,
      inputs: [input],
      outputs: %{input => fn _ -> true end},
      opts: opts
    }
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(
        %__MODULE__{
          origin: origin,
          inputs: inputs,
          outputs: outputs,
          opts: opts,
          composite: composite
        } = source
      ) do
    origin =
      if is_struct(origin) do
        apply(origin.__struct__, :start, [origin])
      else
        origin
      end

    gen_mix =
      GenMix.start(%GenMix{
        inputs: inputs,
        outputs: outputs,
        opts: opts,
        process_chunk: &process_chunk/4,
        composite: composite
      })

    %{source | pid: gen_mix.pid, origin: origin}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{origin: origin, inputs: [input]} = source) when is_map(flow) do
    stream =
      if is_struct(origin) do
        build_stream(origin)
      else
        origin
      end

    prev_stream = Map.get(flow, input, [])

    flow
    |> Map.put(input, Stream.concat(prev_stream, stream))
    |> GenMix.call(source)
  end

  def build_stream(origin) do
    Stream.resource(
      fn ->
        origin
      end,
      fn origin ->
        apply(origin.__struct__, :call, [origin])
      end,
      fn origin -> origin end
    )
  end

  def process_chunk(input_stream_name, chunk, _outputs, nil) do
    {%{input_stream_name => chunk}, Enum.any?(chunk), nil}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = source), do: GenMix.stop(source)
end
