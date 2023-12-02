defmodule Strom.DSL do
  defmodule Module do
    defstruct module: nil, opts: []

    def is_pipeline_module?(module) when is_atom(module) do
      is_list(module.alf_components())
    rescue
      _error -> false
    end
  end

  defmodule Function do
    defstruct function: nil
  end

  defmodule Source do
    defstruct origin: nil
  end

  defmodule Sink do
    defstruct origin: nil
  end

  defmodule Mixer do
    defstruct sources: []
  end

  defmodule Splitter do
    defstruct branches: []
  end

  defmodule Run do
    defstruct run: nil
  end

  defmacro source(origin) do
    quote do
      unless is_struct(unquote(origin)) do
        raise "Source origin must be a struct, given: #{inspect(unquote(origin))}"
      end

      %Strom.DSL.Source{origin: unquote(origin)}
    end
  end

  defmacro sink(origin) do
    quote do
      unless is_struct(unquote(origin)) do
        raise "Sink origin must be a struct, given: #{inspect(unquote(origin))}"
      end

      %Strom.DSL.Sink{origin: unquote(origin)}
    end
  end

  defmacro mixer(sources) do
    quote do
      unless is_list(unquote(sources)) do
        raise "Mixer sources must be a list, given: #{inspect(unquote(sources))}"
      end

      %Strom.DSL.Mixer{sources: unquote(sources)}
    end
  end

  defmacro splitter(branches) do
    quote do
      unless is_map(unquote(branches)) do
        raise "Branches in splitter must be a map, given: #{inspect(unquote(branches))}"
      end

      %Strom.DSL.Splitter{branches: unquote(branches)}
    end
  end

  defmacro module(module, opts \\ []) do
    quote do
      %Strom.DSL.Module{module: unquote(module), opts: unquote(opts)}
    end
  end

  defmacro function(function) do
    quote do
      %Strom.DSL.Function{function: unquote(function)}
    end
  end

  defmacro run() do
    quote do
      %Strom.DSL.Run{}
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

      @spec run() :: :ok
      def run() do
        Strom.Flow.run(__MODULE__)
      end

      @spec stop() :: :ok
      def stop do
        Strom.Flow.stop(__MODULE__)
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def topology, do: @topology
    end
  end
end
