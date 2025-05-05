defmodule Strom.Composite.StartStop do
  @moduledoc "Utility functions for starting and stopping composites"

  alias Strom.Composite
  alias Strom.Renamer

  @spec start(Composite.t()) :: pid()
  def start(%Composite{} = composite) do
    supervisor_name = Composite.supervisor_name(composite.name)
    component_supervisor_name = Composite.component_supervisor_name(composite.name)
    task_supervisor_name = Composite.task_supervisor_name(composite.name)
    registry_name = Composite.registry_name(composite.name)

    {:ok, _supervisor_pid} =
      DynamicSupervisor.start_child(
        Strom.DynamicSupervisor,
        %{
          id: supervisor_name,
          start: {DynamicSupervisor, :start_link, [[name: supervisor_name]]},
          restart: :temporary
        }
      )

    {:ok, _supervisor_pid} =
      DynamicSupervisor.start_child(
        supervisor_name,
        %{
          id: component_supervisor_name,
          start: {DynamicSupervisor, :start_link, [[name: component_supervisor_name]]},
          restart: :temporary
        }
      )

    {:ok, _task_supervisor_pid} =
      DynamicSupervisor.start_child(
        supervisor_name,
        %{
          id: task_supervisor_name,
          start: {DynamicSupervisor, :start_link, [[name: task_supervisor_name]]},
          restart: :temporary
        }
      )

    {:ok, _registry_pid} =
      DynamicSupervisor.start_child(
        supervisor_name,
        %{
          id: registry_name,
          start: {Registry, :start_link, [[keys: :unique, name: registry_name]]},
          restart: :temporary
        }
      )

    {:ok, pid} =
      DynamicSupervisor.start_child(
        Strom.DynamicSupervisor,
        %{id: Composite, start: {Composite, :start_link, [composite]}, restart: :temporary}
      )

    Process.link(pid)
    pid
  end

  def stop(%Composite{name: name}) do
    pid = Process.whereis(name)
    Process.unlink(pid)
    GenServer.call(name, :stop_components)
    GenServer.call(name, :stop)
  end

  def start_components(components, name) do
    components
    |> Enum.reduce([], fn
      %{__struct__: Renamer} = component, acc ->
        [Renamer.start(component) | acc]

      %{__struct__: module} = component, acc ->
        component = %{component | composite: {name, make_ref()}}
        component = module.start(component)
        Process.monitor(component.pid)
        [component | acc]
    end)
    |> Enum.reverse()
  end

  def generate_name(components) do
    components
    |> Enum.map(fn %{__struct__: struct} -> to_string(struct) end)
    |> Enum.map_join("", &String.at(&1, 13))
    |> String.slice(0..15)
    |> then(&(&1 <> "_" <> timestamp_postfix()))
    |> String.to_atom()
  end

  defp timestamp_postfix do
    :erlang.system_time()
    |> rem(round(1.0e9))
    |> to_string()
  end
end
