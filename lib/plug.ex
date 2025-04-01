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
        :global.re_register_name(global_name, self_pid)
    end

    {:ok, %{plug | pid: self_pid}}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name, pid: pid}) do
    try_to_continue_socket(name)

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

  defp try_to_continue_socket(name) do
    case :global.whereis_name({:strom, name, :socket}) do
      pid when is_pid(pid) ->
        send(pid, :continue_socket)

      :undefined ->
        :do_nothing
    end
  end

  @impl true
  def handle_call({:new_data, event}, {_pid, _ref}, %__MODULE__{data: data} = plug) do
    if plug.waiting_client do
      send(plug.waiting_client, :continue_client)
    end

    plug = %{plug | data: data ++ [event], waiting_client: nil}
    {:reply, :ok, plug}
  end

  def handle_call(:done, {_pid, _ref}, %__MODULE__{} = plug) do
    if plug.waiting_client do
      send(plug.waiting_client, :continue_client)
    end

    {:reply, :ok, %{plug | done: true}}
  end

  def handle_call(:get_data, {pid, _ref}, %__MODULE__{data: data, done: done} = plug) do
    {reply, plug} =
      case data do
        [] ->
          if done do
            {:halt, plug}
          else
            {:pause, %{plug | waiting_client: pid}}
          end

        data ->
          {{:data, data}, %{plug | data: []}}
      end

    {:reply, reply, plug}
  end

  def handle_call(:stop, _from, %__MODULE__{} = plug) do
    {:stop, :normal, :ok, plug}
  end

  @impl true
  def handle_info(:continue, plug) do
    # ignore unexpected :continue
    {:noreply, plug}
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid, name: name}) do
    :global.unregister_name({:strom, name, :plug})
    GenServer.call(pid, :stop)
  end
end
