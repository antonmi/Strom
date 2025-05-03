defmodule Strom.Sink do
  @moduledoc """
  Runs a given steam and `call` origin on each even in stream.
  By default it runs the stream asynchronously (in `Task.async`).
  One can pass `true` a the third argument to the `Sink.new/3` function to run a stream synchronously.

      ## Example
      iex> alias Strom.{Sink, Sink.WriteLines}
      iex> sink = :strings |> Sink.new(WriteLines.new("test/data/sink.txt"), sync: true) |> Sink.start()
      iex> %{} = Sink.call(%{strings: ["a", "b", "c"]}, sink)
      iex> File.read!("test/data/sink.txt")
      "a\\nb\\nc\\n"

  Sink defines a `@behaviour`. One can easily implement their own sinks.
  See `Strom.Sink.Writeline`, `Strom.Sink.IOPuts`, `Strom.Sink.Null`
  """
  @callback start(map) :: map
  @callback call(map, term) :: map | no_return()
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

  @spec new(Strom.stream_name(), struct(), list()) :: __MODULE__.t()
  def new(input, origin, opts \\ [])
      when is_atom(input) and is_struct(origin) and is_list(opts) do
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
        } = sink
      ) do
    origin = apply(origin.__struct__, :start, [origin])

    gen_mix =
      GenMix.start(%GenMix{
        inputs: inputs,
        outputs: outputs,
        opts: opts,
        process_chunk: &process_chunk/4,
        composite: composite
      })

    %{sink | pid: gen_mix.pid, origin: origin}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{origin: origin, inputs: [input], opts: opts} = sink)
      when is_map(flow) do
    stream =
      %{input => build_stream(origin, Map.get(flow, input))}
      |> GenMix.call(sink)
      |> Map.get(input)

    if Keyword.get(opts, :sync, false) do
      Stream.run(stream)
    else
      spawn(fn -> Stream.run(stream) end)
    end

    Map.delete(flow, input)
  end

  def build_stream(origin, stream) do
    Stream.transform(stream, origin, fn el, origin ->
      origin = apply(origin.__struct__, :call, [origin, el])
      {[], origin}
    end)
  end

  def process_chunk(input_stream_name, _chunk, _outputs, nil) do
    {%{input_stream_name => []}, false, nil}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = sink), do: GenMix.stop(sink)
end
