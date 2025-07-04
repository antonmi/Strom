defmodule Strom.DSL do
  @moduledoc """
  DSL for building components
  """
  alias Strom.{Transformer, Mixer, Renamer, Sink, Source, Splitter}

  defmacro source(name, origin, opts \\ []) do
    quote do
      Source.new(unquote(name), unquote(origin), unquote(opts))
    end
  end

  defmacro sink(name, origin, opts \\ []) do
    quote do
      Sink.new(unquote(name), unquote(origin), unquote(opts))
    end
  end

  defmacro mix(inputs, output, opts \\ []) do
    quote do
      Mixer.new(unquote(inputs), unquote(output), unquote(opts))
    end
  end

  defmacro split(input, outputs, opts \\ []) do
    quote do
      Splitter.new(unquote(input), unquote(outputs), unquote(opts))
    end
  end

  defmacro transform(names, function, acc \\ nil, opts \\ []) do
    quote do
      Transformer.new(unquote(names), unquote(function), unquote(acc), unquote(opts))
    end
  end

  defmacro rename(names) do
    quote do
      Renamer.new(unquote(names))
    end
  end
end
