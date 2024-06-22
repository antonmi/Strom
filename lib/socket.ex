defmodule Strom.Socket do
  @moduledoc false

  use GenServer

  defstruct pid: nil, name: nil, done: false

  @type t() :: %__MODULE__{}

  @spec new(Strom.stream_name()) :: __MODULE__.t()
  def new(name) do
    %__MODULE__{name: name}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{} = plug) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, plug}},
        %{id: __MODULE__, start: {__MODULE__, :start_link, [plug]}, restart: :temporary}
      )

    :sys.get_state(pid)
  end

  def start_link(%__MODULE__{} = plug) do
    GenServer.start_link(__MODULE__, plug)
  end

  @impl true
  def init(%__MODULE__{name: name} = plug) do
    self_pid = self()
    global_name = {:strom, name, :plug}

    case :global.whereis_name(global_name) do
      :undefined ->
        :yes = :global.register_name(global_name, self_pid)

      pid when is_pid(pid) ->
        :ok = :global.unregister_name(global_name)
        :global.register_name(global_name, self_pid)
    end

    {:ok, %{plug | pid: self_pid}}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name, pid: pid}) do
    stream = Map.fetch!(flow, name)
    GenServer.cast(pid, {:call, stream})
    Map.delete(flow, name)
  end

  @impl true
  def handle_cast({:call, stream}, %__MODULE__{name: name} = plug) do
    Enum.each(stream, fn event ->
      try_to_send(name, {:new_data, event})
    end)

    try_to_send(name, :done)

    {:noreply, %{plug | done: true}}
  end

  @impl true
  def handle_info(:continue_plug, %__MODULE__{done: done, name: name} = plug) do
    if done do
      try_to_send(name, :done)
    end

    {:noreply, plug}
  end

  defp try_to_send(name, msg) do
    case :global.whereis_name({:strom, name, :socket}) do
      pid when is_pid(pid) ->
        :ok = GenServer.call(pid, msg)

      :undefined ->
        receive do
          :continue_plug ->
            flush(:continue_plug)
            try_to_send(name, msg)
        end
    end
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid, name: name}) do
    :global.unregister_name({:strom, name, :plug})
    GenServer.call(pid, :stop)
  end

  defp flush(message) do
    receive do
      ^message ->
        flush(message)
    after
      0 -> :ok
    end
  end
end
