defmodule Strom.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: Strom.DynamicSupervisor},
      {PartitionSupervisor,
       child_spec: DynamicSupervisor, name: Strom.ComponentSupervisor, restart: :temporary},
      {PartitionSupervisor,
       child_spec: Task.Supervisor, name: Strom.TaskSupervisor, restart: :temporary}
    ]

    opts = [strategy: :one_for_one, name: Strom.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
