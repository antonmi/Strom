defmodule Strom.Composite.Manipulations do
  @moduledoc "Utility Module. There are functions for manipulating components in a composite"

  alias Strom.Composite
  alias Strom.Composite.StartStop
  alias Strom.GenMix

  def delete(components, index_from, index_to) do
    {{new_components, deleted_components}, _} =
      Enum.reduce(components, {{[], []}, 0}, fn component, {{acc, deleted_acc}, index} ->
        cond do
          index == index_from ->
            next_component = Enum.at(components, index_to + 1)
            input_streams = Strom.GenMix.state(component.pid).input_streams

            {:ok, _pid, new_tasks} =
              GenServer.call(next_component.pid, {:start_tasks, input_streams})

            GenServer.call(component.pid, {:transfer_tasks, new_tasks})
            GenServer.cast(component.pid, {:gen_mix, :stopping})
            {{acc, [component | deleted_acc]}, index + 1}

          index > index_from and index <= index_to ->
            GenServer.cast(component.pid, {:gen_mix, :stopping})
            {{acc, [component | deleted_acc]}, index + 1}

          true ->
            {{[component | acc], deleted_acc}, index + 1}
        end
      end)

    {Enum.reverse(new_components), deleted_components}
  end

  def insert(components, index, new_components, name) when is_list(new_components) do
    component_after = Enum.at(components, index)
    gm_after = GenMix.state(component_after.pid)

    new_components = StartStop.start_components(new_components, name)
    flow = Composite.reduce_flow(new_components, gm_after.input_streams)

    GenServer.call(component_after.pid, {:start_tasks, flow})

    # TODO
    # take only streams that are in inputs for the component_after
    # return the flow
    components
    |> List.insert_at(index, new_components)
    |> List.flatten()
  end
end
