defmodule Strom.Plug do
  @moduledoc false

  use GenServer

  defstruct pid: nil,
            name: nil,
            data: [],
            done: false,
            waiting_client: nil

  @type t() :: %__MODULE__{}

  @spec new(Strom.stream_name()) :: __MODULE__.t()
  def new(name) do
    %__MODULE__{name: name}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{} = socket) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, socket}},
        %{id: __MODULE__, start: {__MODULE__, :start_link, [socket]}, restart: :temporary}
      )

    :sys.get_state(pid)
  end

  def start_link(%__MODULE__{} = socket) do
    GenServer.start_link(__MODULE__, socket)
  end

  @impl true
  def init(%__MODULE__{name: name} = socket) do
    self_pid = self()
    global_name = {:strom, name, :socket}

    case :global.whereis_name(global_name) do
      :undefined ->
        :yes = :global.register_name(global_name, self_pid)

      pid when is_pid(pid) ->
        :ok = :global.unregister_name(global_name)
        :global.register_name(global_name, self_pid)
    end

    {:ok, %{socket | pid: self_pid}}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name, pid: pid}) do
    :global.send({:strom, name, :plug}, :continue_plug)

    stream =
      Stream.resource(
        fn -> nil end,
        fn nil ->
          case GenServer.call(pid, :get_data) do
            {:data, data} ->
              {data, nil}

            :halt ->
              {:halt, nil}

            :pause ->
              receive do
                :continue_client ->
                  {[], nil}
              end
          end
        end,
        fn nil -> nil end
      )

    Map.put(flow, name, stream)
  end

  @impl true
  def handle_call({:new_data, event}, {_pid, _ref}, %__MODULE__{data: data} = socket) do
    if socket.waiting_client do
      send(socket.waiting_client, :continue_client)
    end

    socket = %{socket | data: data ++ [event], waiting_client: nil}
    {:reply, :ok, socket}
  end

  def handle_call(:done, {_pid, _ref}, %__MODULE__{} = socket) do
    if socket.waiting_client do
      send(socket.waiting_client, :continue_client)
    end

    {:reply, :ok, %{socket | done: true}}
  end

  def handle_call(:get_data, {pid, _ref}, %__MODULE__{data: data, done: done} = socket) do
    {reply, socket} =
      case data do
        [] ->
          if done do
            {:halt, socket}
          else
            {:pause, %{socket | waiting_client: pid}}
          end

        data ->
          {{:data, data}, %{socket | data: []}}
      end

    {:reply, reply, socket}
  end

  @impl true
  def handle_info(:continue, socket) do
    # ignore unexpected :continue
    {:noreply, socket}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid, name: name}) do
    :global.unregister_name({:strom, name, :socket})
    GenServer.call(pid, :stop)
  end
end
