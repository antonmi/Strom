defmodule Strom.Builder do
  alias Strom.Flow
  alias Strom.DSL

  def build(components) do
    components
    |> Enum.map(fn component ->
      case component do
        %DSL.Source{origin: origin} = source ->
          %{source | source: Strom.Source.start(origin)}

        %DSL.Sink{origin: origin} = sink ->
          %{sink | sink: Strom.Sink.start(origin)}

        %DSL.Mixer{} = mixer ->
          %{mixer | mixer: Strom.Mixer.start()}

        %DSL.Splitter{} = splitter ->
          %{splitter | splitter: Strom.Splitter.start()}

        %DSL.Function{function: function} = fun ->
          %{fun | function: Strom.Function.start(function)}

        %DSL.Module{module: module, opts: opts} = mod ->
          module = Strom.Module.start(module, opts)
          %{mod | module: module}
      end
    end)
  end
end
