defmodule Strom.Builder do
  alias Strom.Flow
  alias Strom.DSL

  def build(components, flow_pid) when is_pid(flow_pid) do
    do_build(components, nil, flow_pid)
  end

  defp do_build(components, stream, flow_pid) when is_list(components) do
    components
    |> Enum.reduce(stream, fn component, stream ->
      case component do
        %DSL.Source{origin: origin} ->
          source = Strom.Source.start(origin)
          Flow.add_component(flow_pid, {:source, source})
          Strom.Source.stream(source)

        %DSL.Sink{origin: origin} ->
          sink = Strom.Sink.start(origin)
          Flow.add_component(flow_pid, {:sink, sink})
          Strom.Sink.stream(stream, sink)

        %DSL.Mixer{sources: sources} ->
          sources = Enum.map(sources, &do_build(&1, nil, flow_pid))
          mixer = Strom.Mixer.start(sources)
          Flow.add_component(flow_pid, {:mixer, mixer})
          Strom.Mixer.stream(mixer)

        %DSL.Function{function: function} ->
          function.(stream)

        %DSL.Module{module: module, opts: opts} ->
          state = apply(module, :start, [opts])
          Flow.add_component(flow_pid, {:module, {module, state}})

          if DSL.Module.is_pipeline_module?(module) do
            apply(module, :stream, [stream])
          else
            apply(module, :stream, [stream, state])
          end

        %DSL.Splitter{branches: branches} ->
          partitions = Map.keys(branches)

          splitter = Strom.Splitter.start(stream, partitions)
          Flow.add_component(flow_pid, {:splitter, splitter})

          splitter
          |> Strom.Splitter.stream()
          |> Enum.with_index(fn str, index ->
            partition = Enum.at(partitions, index)
            branch = Map.fetch!(branches, partition)
            do_build(branch, str, flow_pid)
          end)

        %DSL.Run{} ->
          Flow.add_stream(flow_pid, stream)
          stream
      end
    end)
  end
end
