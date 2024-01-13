defmodule Strom.DSL do

  alias Strom.{Transformer, Mixer, Renamer, Sink, Source, Splitter}

  defmacro source(name, origin) do
    quote do
      Source.new(unquote(name), unquote(origin))
    end
  end

  defmacro sink(name, origin, sync \\ false) do
    quote do
      Sink.new(unquote(name), unquote(origin), unquote(sync))
    end
  end

  defmacro mix(inputs, output, opts \\ []) do
    quote do
      %Mixer{inputs: unquote(inputs), output: unquote(output), opts: unquote(opts)}
    end
  end

  defmacro split(input, outputs, opts \\ []) do
    quote do
      unless is_map(unquote(outputs)) and map_size(unquote(outputs)) > 0 do
        raise "Outputs in splitter must be a map, given: #{inspect(unquote(outputs))}"
      end

      %Splitter{
        input: unquote(input),
        outputs: unquote(outputs),
        opts: unquote(opts)
      }
    end
  end

  defmacro transform(names, function, acc \\ nil, opts \\ []) do
    quote do
      %Transformer{
        function: unquote(function),
        acc: unquote(acc),
        opts: unquote(opts),
        names: unquote(names)
      }
    end
  end

  defmacro from(module, opts \\ []) do
    quote do
      unless is_atom(unquote(module)) do
        raise "Flow must be a module, given: #{inspect(unquote(module))}"
      end

      apply(unquote(module), :topology, [unquote(opts)])
    end
  end

  defmacro rename(names) do
    quote do
      unless is_map(unquote(names)) do
        raise "Names must be a map, given: #{inspect(unquote(names))}"
      end

      %Renamer{names: unquote(names)}
    end
  end
end
