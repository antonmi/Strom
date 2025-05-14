defmodule Strom.Composite.Manipulations do
  @moduledoc "Utility Module. There are functions for manipulating components in a composite"

  alias Strom.Composite
  alias Strom.Composite.StartStop
  alias Strom.GenMix

  @spec insert(list(Strom.component()), integer(), list(Strom.component()), atom()) ::
          {list(Strom.component()), list(Strom.component()), Strom.flow()}
  def insert(components, index, new_components, name) when is_list(new_components) do
    component_after = Enum.at(components, index)
    gm_after = GenMix.state(component_after.pid)

    new_components = StartStop.start_components(new_components, name)
    flow = Composite.call_flow(new_components, gm_after.input_streams)

    {_gm_pid, new_tasks} =
      GenMix.start_tasks(component_after.pid, Map.take(flow, component_after.inputs))

    GenMix.transfer_tasks(component_after.pid, new_tasks, :old)

    components =
      components
      |> List.insert_at(index, new_components)
      |> List.flatten()

    {components, [], Map.drop(flow, component_after.inputs)}
  end

  @spec replace(list(Strom.component()), integer(), integer(), list(Strom.component()), atom()) ::
          {list(Strom.component()), list(Strom.component()), Strom.flow()}
  def replace(components, index_from, index_to, new_components, name)
      when is_list(new_components) do
    {{new_components, deleted_components, subflow}, _} =
      Enum.reduce(components, {{[], [], %{}}, 0}, fn component,
                                                     {{acc, deleted_acc, subflow}, index} ->
        cond do
          index == index_from ->
            input_streams = Strom.GenMix.state(component.pid).input_streams
            new_components = StartStop.start_components(new_components, name)
            flow = Composite.call_flow(new_components, input_streams)
            component_after = Enum.at(components, index_to + 1)

            {_gm_pid, new_tasks} =
              GenMix.start_tasks(component_after.pid, Map.take(flow, component_after.inputs))

            GenMix.transfer_tasks(component.pid, new_tasks, :all)

            {{acc ++ Enum.reverse(new_components), [component | deleted_acc],
              Map.drop(flow, component_after.inputs)}, index + 1}

          index > index_from and index <= index_to ->
            GenServer.cast(component.pid, {:gen_mix, :stopping})
            {{acc, [component | deleted_acc], subflow}, index + 1}

          true ->
            {{[component | acc], deleted_acc, subflow}, index + 1}
        end
      end)

    {Enum.reverse(new_components), Enum.reverse(deleted_components), subflow}
  end
end
