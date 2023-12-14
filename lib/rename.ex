defmodule Strom.Rename do
  use GenServer

  defstruct names: nil, pid: nil

  def start(names) do
    state = %__MODULE__{names: names}

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = state), do: {:ok, %{state | pid: self()}}

  def call(flow, %__MODULE__{}, names) when is_map(names) do
    Enum.reduce(names, flow, fn {name, new_name}, acc ->
      acc
      |> Map.put(new_name, Map.fetch!(acc, name))
      |> Map.delete(name)
    end)
  end

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}
end
