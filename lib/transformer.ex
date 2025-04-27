defmodule Strom.Transformer do
  @moduledoc """
  Transforms a stream or several streams.
  It works as Stream.map/2 or Stream.transform/3.

      ## `map` example:
      iex> alias Strom.Transformer
      iex> transformer = :numbers |> Transformer.new(&(&1*2)) |> Transformer.start()
      iex> flow = %{numbers: [1, 2, 3]}
      iex> %{numbers: stream} = Transformer.call(flow, transformer)
      iex> Enum.to_list(stream)
      [2, 4, 6]

      ## `reduce` example:
      iex> alias Strom.Transformer
      iex> fun = fn el, acc -> {[el, acc], acc + 10} end
      iex> transformer = :numbers |> Transformer.new(fun, 10) |> Transformer.start()
      iex> flow = %{numbers: [1, 2, 3]}
      iex> %{numbers: stream} = Transformer.call(flow, transformer)
      iex> Enum.to_list(stream)
      [1, 10, 2, 20, 3, 30]

      ## it can be applied to several streams:
      iex> alias Strom.Transformer
      iex> transformer = [:s1, :s2] |> Transformer.new(&(&1*2)) |> Transformer.start()
      iex> flow = %{s1: [1, 2, 3], s2: [4, 5, 6]}
      iex> %{s1: s1, s2: s2} = Transformer.call(flow, transformer)
      iex> {Enum.to_list(s1), Enum.to_list(s2)}
      {[2, 4, 6], [8, 10, 12]}
  """

  alias Strom.GenMix

  defstruct pid: nil,
            inputs: [],
            outputs: %{},
            acc: nil,
            opts: []

  @type t() :: %__MODULE__{}
  @type event() :: any()
  @type acc() :: any()
  @type func() ::
          (event() -> event())
          | (event(), acc() -> {[event()], acc()})

  @spec new(Strom.stream_name(), func(), acc(), list()) :: __MODULE__.t()
  def new(names, function, acc \\ nil, opts \\ [])
      when (is_atom(names) or is_list(names)) and is_function(function) and is_list(opts) do
    inputs = if is_list(names), do: names, else: [names]

    function =
      if is_function(function, 1) do
        fn el, nil -> {[function.(el)], nil} end
      else
        function
      end

    outputs =
      Enum.reduce(inputs, %{}, fn name, out ->
        Map.put(out, name, fn el, acc -> function.(el, acc) end)
      end)

    %__MODULE__{inputs: inputs, outputs: outputs, opts: opts, acc: acc}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{inputs: inputs, outputs: outputs, acc: acc, opts: opts} = transformer) do
    gen_mix =
      GenMix.start(%GenMix{
        inputs: inputs,
        outputs: outputs,
        accs: Enum.reduce(inputs, %{}, fn name, accs -> Map.put(accs, name, acc) end),
        opts: opts,
        process_chunk: &process_chunk/4
      })

    %{transformer | pid: gen_mix.pid}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{} = transformer) do
    GenMix.call(flow, transformer)
  end

  def process_chunk(input_stream_name, chunk, outputs, acc) do
    output_function = Map.get(outputs, input_stream_name)

    {chunk, new_acc} =
      Enum.flat_map_reduce(chunk, acc, fn el, acc ->
        output_function.(el, acc)
      end)

    {%{input_stream_name => chunk}, Enum.any?(chunk), new_acc}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = transformer), do: GenMix.stop(transformer)
end
