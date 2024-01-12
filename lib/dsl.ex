defmodule Strom.DSL do
  defmacro source(names, origin) do
    quote do
      unless is_struct(unquote(origin)) or is_list(unquote(origin)) do
        raise "Source origin must be a struct or just simple list, given: #{inspect(unquote(origin))}"
      end

      %Strom.Source{origin: unquote(origin), names: unquote(names)}
    end
  end

  defmacro sink(names, origin, sync \\ false) do
    quote do
      unless is_struct(unquote(origin)) do
        raise "Sink origin must be a struct, given: #{inspect(unquote(origin))}"
      end

      %Strom.Sink{origin: unquote(origin), names: unquote(names), sync: unquote(sync)}
    end
  end

  defmacro mix(inputs, output, opts \\ []) do
    quote do
      unless is_list(unquote(inputs)) do
        raise "Mixer sources must be a list, given: #{inspect(unquote(inputs))}"
      end

      %Strom.Mixer{inputs: unquote(inputs), output: unquote(output), opts: unquote(opts)}
    end
  end

  defmacro split(input, partitions, opts \\ []) do
    quote do
      unless is_map(unquote(partitions)) and map_size(unquote(partitions)) > 0 do
        raise "Branches in splitter must be a map, given: #{inspect(unquote(partitions))}"
      end

      %Strom.Splitter{
        input: unquote(input),
        partitions: unquote(partitions),
        opts: unquote(opts)
      }
    end
  end

  defmacro transform(inputs, function, acc \\ nil, opts \\ []) do
    quote do
      %Strom.Transformer{
        function: unquote(function),
        acc: unquote(acc),
        opts: unquote(opts),
        inputs: unquote(inputs)
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

  defmacro __using__(_opts) do
    quote do
      import Strom.DSL

      @spec start(term) :: Strom.Flow.t()
      def start(opts \\ []) do
        Strom.Flow.start(__MODULE__, opts)
      end

      @spec call(map) :: map()
      def call(flow) when is_map(flow) do
        Strom.Flow.call(__MODULE__, flow)
      end

      @spec stop() :: :ok
      def stop do
        Strom.Flow.stop(__MODULE__)
      end

      @spec info() :: list()
      def info do
        Strom.Flow.info(__MODULE__)
      end
    end
  end
end
