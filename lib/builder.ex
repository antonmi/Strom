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

        %DSL.Pipeline{pipeline: pipeline} ->
          :ok = pipeline.start()
          Flow.add_component(flow_pid, {:pipeline, pipeline})
          pipeline.stream(stream)

        %DSL.Transform{function: function} ->
          function.(stream)

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

  #  defp do_build(components, {stream, flow}) do
  #    components
  #    |> Enum.reduce({stream, flow}, fn(component, {stream, flow}) ->
  #      case component do
  #        %DSL.Source{source: source} ->
  #          stream = Strom.Source.stream(source)
  #          {stream, flow}
  #
  #        %DSL.Sink{sink: sink} ->
  #          stream = Strom.Sink.stream(stream, sink)
  #          {stream, flow}
  #
  #        %DSL.Mixer{sources: sources} ->
  #          {stream, flow} = do_build(sources, {nil, flow})
  #          stream =
  #            stream
  #            |> Strom.Mixer.new()
  #            |> Strom.Mixer.stream()
  #          {stream, flow}
  #        %DSL.Pipeline{pipeline: pipeline} ->
  #          pipeline.start()
  #          stream = pipeline.stream(stream)
  #          {stream, flow}
  #        %DSL.Splitter{branches: branches} ->
  #          partitions = Map.keys(branches)
  #          stream
  #          |> Strom.Splitter.new(partitions)
  #          |> Strom.Splitter.stream()
  #          |> Enum.with_index(fn str, index ->
  #            partition = Enum.at(partitions, index)
  #            branch = Map.fetch!(branches, partition)
  #            {stream, flow} = do_build(branch, {stream, flow})
  #          end)
  #          {stream, flow}
  #      end
  #    end)
  #  end
end
