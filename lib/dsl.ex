defmodule Strom.DSL do
  defmodule Module do
    defstruct module: nil, opts: [], inputs: [], state: nil
  end

  defmodule Function do
    defstruct function: nil, opts: [], inputs: []
  end

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

  defmacro source(names, origin) do
    quote do
      unless is_struct(unquote(origin)) do
        raise "Source origin must be a struct, given: #{inspect(unquote(origin))}"
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

  defmacro module(inputs, module, opts \\ []) do
    quote do
      %Strom.DSL.Module{module: unquote(module), opts: unquote(opts), inputs: unquote(inputs)}
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

  defmacro __using__(_opts) do
    quote do
      import Strom.DSL

      @before_compile Strom.DSL

      @spec start() :: Strom.Flow.t()
      def start do
        Strom.Flow.start(__MODULE__)
      end

      @spec topology() :: list(map)
      def topology() do
        Strom.Flow.topology(__MODULE__)
      end

      @spec call(map) :: map()
      def call(flow) when is_map(flow) do
        Strom.Flow.call(__MODULE__, flow)
      end

      @spec stop() :: :ok
      def stop do
        Strom.Flow.stop(__MODULE__)
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def flow_topology, do: @topology
    end
  end
end
