defmodule Strom.DSL do
  defmodule Source do
    defstruct source: nil, origin: nil, names: []
  end

  defmodule Sink do
    defstruct sink: nil, origin: nil, names: [], sync: false
  end

  defmodule Mixer do
    defstruct mixer: nil, opts: [], inputs: [], output: nil
  end

  defmodule Splitter do
    defstruct splitter: nil, opts: [], input: nil, partitions: %{}
  end

  defmodule Function do
    defstruct function: nil, opts: [], inputs: []
  end

  defmodule Transform do
    defstruct function: nil, acc: nil, inputs: [], call: nil
  end

  defmodule Module do
    defstruct module: nil, opts: [], inputs: [], state: nil
  end

  defmodule Rename do
    defstruct names: nil, rename: nil
  end

  defmacro source(names, origin) do
    quote do
      unless is_struct(unquote(origin)) or is_list(unquote(origin)) do
        raise "Source origin must be a struct or just simple list, given: #{inspect(unquote(origin))}"
      end

      %Strom.DSL.Source{origin: unquote(origin), names: unquote(names)}
    end
  end

  defmacro sink(names, origin, sync \\ false) do
    quote do
      unless is_struct(unquote(origin)) do
        raise "Sink origin must be a struct, given: #{inspect(unquote(origin))}"
      end

      %Strom.DSL.Sink{origin: unquote(origin), names: unquote(names), sync: unquote(sync)}
    end
  end

  defmacro mixer(inputs, output, opts \\ []) do
    quote do
      unless is_list(unquote(inputs)) do
        raise "Mixer sources must be a list, given: #{inspect(unquote(inputs))}"
      end

      %Strom.DSL.Mixer{inputs: unquote(inputs), output: unquote(output), opts: unquote(opts)}
    end
  end

  defmacro splitter(input, partitions, opts \\ []) do
    quote do
      unless is_map(unquote(partitions)) and map_size(unquote(partitions)) > 0 do
        raise "Branches in splitter must be a map, given: #{inspect(unquote(partitions))}"
      end

      %Strom.DSL.Splitter{
        input: unquote(input),
        partitions: unquote(partitions),
        opts: unquote(opts)
      }
    end
  end

  defmacro function(inputs, function, opts \\ []) do
    quote do
      %Strom.DSL.Function{
        function: unquote(function),
        opts: unquote(opts),
        inputs: unquote(inputs)
      }
    end
  end

  defmacro transform(inputs, function, acc) do
    quote do
      %Strom.DSL.Transform{
        function: unquote(function),
        acc: unquote(acc),
        inputs: unquote(inputs)
      }
    end
  end

  defmacro transform(inputs, function) do
    quote do
      %Strom.DSL.Transform{
        function: unquote(function),
        acc: nil,
        inputs: unquote(inputs)
      }
    end
  end

  defmacro module(inputs, module, opts \\ []) do
    quote do
      %Strom.DSL.Module{
        module: unquote(module),
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

      %Strom.DSL.Rename{names: unquote(names)}
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
