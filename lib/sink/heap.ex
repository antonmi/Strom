defmodule Strom.Sink.Heap do
  @behaviour Strom.Sink

  defstruct agent: nil

  def new(), do: %__MODULE__{}

  @impl true
  def start(%__MODULE__{} = heap) do
    case Agent.start_link(fn -> [] end) do
      {:ok, pid} ->
        %{heap | agent: pid}

      {:error, {:already_started, pid}} ->
        %{heap | agent: pid}
    end
  end

  @impl true
  def call(%__MODULE__{agent: agent} = heap, datum) do
    Agent.update(agent, fn data ->
      [datum | data]
    end)

    heap
  end

  @impl true
  def stop(%__MODULE__{agent: agent} = heap) do
    Agent.stop(agent)
    %{heap | agent: nil}
  end

  def data(%__MODULE__{agent: agent}) do
    agent
    |> Agent.get(& &1)
    |> Enum.reverse()
  end
end
