defmodule Strom.DSL do
  defmacro source(name, origin) do
    quote do
      unless is_struct(unquote(origin)) or is_list(unquote(origin)) do
        raise "Source origin must be a struct or just simple list, given: #{inspect(unquote(origin))}"
      end

      %Strom.Source{origin: unquote(origin), name: unquote(name)}
    end
  end

  defmacro sink(name, origin, sync \\ false) do
    quote do
      unless is_struct(unquote(origin)) do
        raise "Sink origin must be a struct, given: #{inspect(unquote(origin))}"
      end

      %Strom.Sink{origin: unquote(origin), name: unquote(name), sync: unquote(sync)}
    end
  end

  defmacro mix(inputs, output, opts \\ []) do
    quote do
      unless is_list(unquote(inputs)) or is_map(unquote(inputs)) do
        raise "Mixer sources must be a list or map, given: #{inspect(unquote(inputs))}"
      end

      %Strom.Mixer{inputs: unquote(inputs), output: unquote(output), opts: unquote(opts)}
    end
  end

  defmacro split(input, outputs, opts \\ []) do
    quote do
      unless is_map(unquote(outputs)) and map_size(unquote(outputs)) > 0 do
        raise "Branches in splitter must be a map, given: #{inspect(unquote(outputs))}"
      end

      %Strom.Splitter{
        input: unquote(input),
        outputs: unquote(outputs),
        opts: unquote(opts)
      }
    end
  end

  defmacro transform(names, function, acc \\ nil, opts \\ []) do
    quote do
      %Strom.Transformer{
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

      %Strom.Renamer{names: unquote(names)}
    end
  end
end
