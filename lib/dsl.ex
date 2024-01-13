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
      %{Mixer.new(unquote(inputs), unquote(output)) | opts: unquote(opts)}
    end
  end

  defmacro split(input, outputs, opts \\ []) do
    quote do
      %{Splitter.new(unquote(input), unquote(outputs)) | opts: unquote(opts)}
    end
  end

  defmacro transform(names, function, acc \\ nil, opts \\ []) do
    quote do
      %{Transformer.new(unquote(names), unquote(function), unquote(acc)) | opts: unquote(opts)}
    end
  end

  defmacro rename(names) do
    quote do
      Renamer.new(unquote(names))
    end
  end
end
