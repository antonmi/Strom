defmodule Strom.DuplexTransformer do
  @moduledoc false

  alias Strom.DuplexMix

  defstruct pid: nil,
            composite: nil,
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
  def new(names, {fn_left, fn_right}, acc \\ nil, opts \\ [])
      when (is_atom(names) or is_list(names)) and is_function(fn_left) and is_function(fn_right) and
             is_list(opts) do
    inputs = if is_list(names), do: names, else: [names]

    {fn_left, fn_right} = {wrap_function(fn_left), wrap_function(fn_right)}

    outputs_left =
      Enum.reduce(inputs, %{}, fn name, out ->
        Map.put(out, name, fn el, acc -> fn_left.(el, acc) end)
      end)

    outputs_right =
      Enum.reduce(inputs, %{}, fn name, out ->
        Map.put(out, name, fn el, acc -> fn_right.(el, acc) end)
      end)

    %__MODULE__{
      inputs: {inputs, inputs},
      outputs: {outputs_left, outputs_right},
      opts: opts,
      acc: acc
    }
  end

  defp wrap_function(function) do
    if is_function(function, 1) do
      fn el, nil ->
        {left, right} = function.(el)
        {{[left], [right]}, nil}
      end
    else
      function
    end
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(
        %__MODULE__{inputs: inputs, outputs: outputs, acc: acc, opts: opts, composite: composite} =
          transformer
      ) do
    accs = Enum.reduce(elem(inputs, 0), %{}, fn name, accs -> Map.put(accs, name, acc) end)

    duplex_mix =
      DuplexMix.start(%DuplexMix{
        inputs: inputs,
        outputs: outputs,
        accs: {accs, accs},
        opts: opts,
        process_chunk: &process_chunk/4,
        composite: composite
      })

    %{transformer | pid: duplex_mix.pid}
  end

  @spec call(Strom.duplex_flow(), __MODULE__.t()) :: Strom.duplex_flow()
  def call({flow_left, flow_right}, %__MODULE__{} = transformer) do
    DuplexMix.call({flow_left, flow_right}, transformer)
  end

  @spec process_chunk(atom(), list(), map(), any()) ::
          {{Strom.flow(), boolean(), any()}, {Strom.flow(), boolean(), any()}}
  def process_chunk(input_stream_name, chunk, outputs, acc) do
    output_function = Map.get(outputs, input_stream_name)

    {{outputs_forward, outputs_backward}, new_acc} =
      chunk
      |> Enum.reduce(acc, fn event, acc ->
        output_function.(event, acc)
      end)

    {{%{input_stream_name => outputs_forward}, Enum.any?(outputs_forward), new_acc},
     {%{input_stream_name => outputs_backward}, Enum.any?(outputs_backward), new_acc}}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{} = transformer), do: DuplexMix.stop(transformer)
end
