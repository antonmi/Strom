defmodule Strom.DuplexMix do
  @moduledoc """
  Generic functionality used by other components.
  """
  use GenServer

  @chunk 1
  @buffer 1000

  defstruct pid: nil,
            composite: nil,
            process_chunk: nil,
            inputs: {[], []},
            outputs: {%{}, %{}},
            accs: {%{}, %{}},
            opts: [],
            chunk: @chunk,
            buffer: @buffer,
            no_wait: false,
            input_streams: {%{}, %{}},
            tasks: {%{}, %{}},
            clients: {%{}, %{}},
            tasks_started: {false, false},
            tasks_run: {false, false},
            stopping: false,
            before_stop: nil,
            data: {%{}, %{}},
            data_size: {0, 0},
            waiting_tasks: {%{}, %{}},
            waiting_clients: {%{}, %{}}

  alias Strom.Composite
  alias Strom.DuplexMix.Streams
  alias Strom.DuplexMix.Tasks

  @type t() :: %__MODULE__{}

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{process_chunk: process_chunk, opts: opts, composite: composite} = gm)
      when is_list(opts) do
    gm = %{
      gm
      | process_chunk: if(process_chunk, do: process_chunk, else: &process_chunk/4),
        chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer),
        no_wait: Keyword.get(opts, :no_wait, false)
    }

    partitions = PartitionSupervisor.partitions(Strom.ComponentSupervisor)
    partition_key = Enum.random(1..partitions)

    child_name =
      case composite do
        nil ->
          {:via, PartitionSupervisor, {Strom.ComponentSupervisor, partition_key}}

        name ->
          Composite.component_supervisor_name(name)
      end

    {:ok, pid} =
      DynamicSupervisor.start_child(
        child_name,
        %{id: __MODULE__, start: {__MODULE__, :start_link, [gm]}, restart: :temporary}
      )

    %{gm | pid: pid}
  end

  def start_link(%__MODULE__{} = gm) do
    GenServer.start_link(__MODULE__, gm)
  end

  @impl true
  @spec init(__MODULE__.t()) :: {:ok, __MODULE__.t()}
  def init(%__MODULE__{} = gm) do
    {:ok, %{gm | pid: self()}}
  end

  @spec start_tasks(pid(), {Strom.flow(), Strom.flow()}) :: {pid(), {map(), map()}}
  def start_tasks(gm_pid, {left_input_streams, right_input_streams}) do
    # TODO maybe in one call? Check Manipulations
    GenServer.call(gm_pid, {:start_left_tasks, left_input_streams})
    GenServer.call(gm_pid, {:start_right_tasks, right_input_streams})
  end

  @spec run_tasks(pid(), atom(), :left | :right) :: :ok
  def run_tasks(gm_pid, output_name, side) do
    case side do
      :left -> GenServer.call(gm_pid, {:run_left_tasks, output_name})
      :right -> GenServer.call(gm_pid, {:run_right_tasks, output_name})
    end
  end

  @spec call(Strom.duplex_flow(), map()) :: Strom.duplex_flow() | no_return()
  def call({left_flow, right_flow}, gm) do
    Streams.call({left_flow, right_flow}, gm)
  end

  @spec state(pid()) :: __MODULE__.t()
  def state(pid), do: GenServer.call(pid, :state)

  @spec stop(any()) :: any()
  def stop(gm) do
    GenServer.call(gm.pid, :stop)
  end

  @spec process_chunk(atom(), list(), Strom.flow(), any()) ::
          {{Strom.flow(), boolean(), any()}, {Strom.flow(), boolean(), any()}}
  def process_chunk(input_stream_name, chunk, outputs, nil) do
    # TODO make it within one enumeration
    outputs_forward =
      Enum.reduce(outputs, {%{}, false, nil}, fn {output_name, output_stream_fun},
                                                 {acc, any?, nil} ->
        data_forward =
          Enum.filter(chunk, fn event ->
            case output_stream_fun.(event) do
              {true, _} -> true
              {false, _} -> false
            end
          end)

        {Map.put(acc, output_name, data_forward), any? || Enum.any?(data_forward), nil}
      end)

    outputs_backward =
      Enum.reduce(outputs, {%{}, false, nil}, fn {_output_name, output_stream_fun},
                                                 {acc, any?, nil} ->
        data_backward =
          Enum.filter(chunk, fn event ->
            case output_stream_fun.(event) do
              {_, true} -> true
              {_, false} -> false
            end
          end)

        data_backward = Map.get(acc, input_stream_name, []) ++ data_backward
        {Map.put(acc, input_stream_name, data_backward), any? || Enum.any?(data_backward), nil}
      end)

    {outputs_forward, outputs_backward}
  end

  @impl true
  def handle_call({:start_left_tasks, left_input_streams}, _from, %__MODULE__{} = gm)
      when is_map(left_input_streams) do
    {left_tasks, right_tasks} = gm.tasks
    new_left_tasks = Tasks.start_tasks(left_input_streams, gm, :left)
    left_tasks = Map.merge(left_tasks, new_left_tasks)

    {:reply, {gm.pid, new_left_tasks},
     %{
       gm
       | tasks_started: put_elem(gm.tasks_started, 0, true),
         tasks_run: put_elem(gm.tasks_run, 0, false),
         tasks: {left_tasks, right_tasks},
         input_streams: put_elem(gm.input_streams, 0, left_input_streams)
     }}
  end

  def handle_call({:start_right_tasks, right_input_streams}, _from, %__MODULE__{} = gm)
      when is_map(right_input_streams) do
    {left_tasks, right_tasks} = gm.tasks
    new_right_tasks = Tasks.start_tasks(right_input_streams, gm, :right)
    right_tasks = Map.merge(right_tasks, new_right_tasks)

    {:reply, {gm.pid, new_right_tasks},
     %{
       gm
       | tasks_started: put_elem(gm.tasks_started, 1, true),
         tasks_run: put_elem(gm.tasks_run, 1, false),
         tasks: {left_tasks, right_tasks},
         input_streams: put_elem(gm.input_streams, 1, right_input_streams)
     }}
  end

  def handle_call(
        {:run_left_tasks, output_name},
        {client_pid, _ref},
        %__MODULE__{tasks_started: {true, _}, tasks_run: {false, right_running}} = gm
      ) do
    Tasks.run_tasks(elem(gm.tasks, 0), elem(gm.accs, 0))
    {left_clients, right_clients} = gm.clients
    left_clients = Map.put(left_clients, client_pid, output_name)

    {:reply, :ok,
     %{gm | tasks_run: {true, right_running}, clients: {left_clients, right_clients}}}
  end

  def handle_call(
        {:run_left_tasks, output_name},
        {client_pid, _ref},
        %__MODULE__{tasks_started: {true, _}, tasks_run: {true, _}} = gm
      ) do
    {left_clients, right_clients} = gm.clients
    left_clients = Map.put(left_clients, client_pid, output_name)
    {:reply, :ok, %{gm | clients: {left_clients, right_clients}}}
  end

  def handle_call(
        {:run_right_tasks, output_name},
        {client_pid, _ref},
        %__MODULE__{tasks_started: {_, true}, tasks_run: {_, false}} = gm
      ) do
    Tasks.run_tasks(elem(gm.tasks, 1), elem(gm.accs, 1))
    {left_clients, right_clients} = gm.clients
    right_clients = Map.put(right_clients, client_pid, output_name)
    {:reply, :ok, %{gm | tasks_run: {true, true}, clients: {left_clients, right_clients}}}
  end

  def handle_call(
        {:run_right_tasks, output_name},
        {client_pid, _ref},
        %__MODULE__{tasks_started: {_, true}, tasks_run: {_, true}} = gm
      ) do
    {left_clients, right_clients} = gm.clients
    right_clients = Map.put(right_clients, client_pid, output_name)
    {:reply, :ok, %{gm | clients: {left_clients, right_clients}}}
  end

  def handle_call(:stop, _from, %__MODULE__{} = gm) do
    gm = %{gm | stopping: true}

    if ready_to_stop?(gm) do
      if gm.before_stop, do: gm.before_stop.()

      {:stop, :normal, :ok, gm}
    else
      {:reply, :ok, gm}
    end
  end

  def handle_call(:state, _from, %__MODULE__{} = gm) do
    {:reply, gm, gm}
  end

  @impl true
  def handle_cast({:gen_mix, :stopping}, gm) do
    after_action(%{gm | stopping: true})
  end

  def handle_cast(
        {:put_data_from_left, {input_name, task_pid}, {new_data, new_acc}},
        %__MODULE__{} = gm
      ) do
    {all_data, waiting_clients, total_count} =
      Streams.new_data(
        new_data,
        [input_name | Map.keys(elem(gm.outputs, 0))],
        elem(gm.data, 0),
        elem(gm.waiting_clients, 0)
      )

    waiting_tasks =
      if total_count < gm.buffer do
        send(task_pid, {:task, input_name, :continue})
        elem(gm.waiting_tasks, 0)
      else
        Map.put(elem(gm.waiting_tasks, 0), task_pid, input_name)
      end

    gm = %{
      gm
      | data: {all_data, elem(gm.data, 1)},
        accs: {Map.put(elem(gm.accs, 0), input_name, new_acc), elem(gm.accs, 1)},
        data_size: {total_count, elem(gm.data_size, 1)},
        waiting_clients: {waiting_clients, elem(gm.waiting_clients, 1)},
        waiting_tasks: {waiting_tasks, elem(gm.waiting_tasks, 1)}
    }

    after_action(gm)
  end

  def handle_cast(
        {:put_data_from_right, {input_name, task_pid}, {new_data, new_acc}},
        %__MODULE__{} = gm
      ) do
    {all_data, waiting_clients, total_count} =
      Streams.new_data(
        new_data,
        [input_name | Map.keys(elem(gm.outputs, 1))],
        elem(gm.data, 1),
        elem(gm.waiting_clients, 1)
      )

    waiting_tasks =
      if total_count < gm.buffer do
        send(task_pid, {:task, input_name, :continue})
        elem(gm.waiting_tasks, 1)
      else
        Map.put(elem(gm.waiting_tasks, 1), task_pid, input_name)
      end

    gm = %{
      gm
      | data: {elem(gm.data, 0), all_data},
        accs: {elem(gm.accs, 0), Map.put(elem(gm.accs, 1), input_name, new_acc)},
        data_size: {elem(gm.data_size, 0), total_count},
        waiting_clients: {elem(gm.waiting_clients, 0), waiting_clients},
        waiting_tasks: {elem(gm.waiting_tasks, 0), waiting_tasks}
    }

    after_action(gm)
  end

  def handle_cast({:get_left_data, {output_name, client_pid}}, %__MODULE__{} = gm) do
    {left_data, right_data} = gm.data
    output_data = Map.get(left_data, output_name, [])
    {initital_left_data_size, _initital_right_data_size} = gm.data_size

    gm =
      case {output_data, map_size(elem(gm.tasks, 0))} do
        # no tasks left, send done
        {[], 0} ->
          send(client_pid, {:client, output_name, :done})
          %{gm | data: {Map.put(left_data, output_name, []), right_data}}

        # no data, but there are running tasks, wait.
        {[], _} ->
          {left_waiting_clients, right_waiting_clients} = gm.waiting_clients

          %{
            gm
            | waiting_clients:
                {Map.put(left_waiting_clients, client_pid, output_name), right_waiting_clients}
          }

        # there is data, send it to the client
        {output_data, _} ->
          send(client_pid, {:client, output_name, {:data, output_data}})

          %{
            gm
            | data: {Map.put(left_data, output_name, []), right_data},
              data_size: {elem(gm.data_size, 0) - length(output_data), elem(gm.data_size, 1)}
          }
      end

    any_data_taken = elem(gm.data_size, 0) < initital_left_data_size

    waiting_tasks =
      if any_data_taken and elem(gm.data_size, 0) < gm.buffer do
        {left_waiting_tasks, right_waiting_tasks} = gm.waiting_tasks
        send_continue_to_tasks(left_waiting_tasks)

        {left_waiting_tasks, right_waiting_tasks}
      else
        gm.waiting_tasks
      end

    gm = %{gm | waiting_tasks: waiting_tasks}
    after_action(gm)
  end

  def handle_cast({:get_right_data, {output_name, client_pid}}, %__MODULE__{} = gm) do
    {left_data, right_data} = gm.data
    output_data = Map.get(right_data, output_name, [])
    {_initital_left_data_size, initital_right_data_size} = gm.data_size

    gm =
      case {output_data, map_size(elem(gm.tasks, 1))} do
        # no tasks left, send done
        {[], 0} ->
          send(client_pid, {:client, output_name, :done})
          %{gm | data: {left_data, Map.put(right_data, output_name, [])}}

        # no data, but there are running tasks, wait.
        {[], _} ->
          {left_waiting_clients, right_waiting_clients} = gm.waiting_clients

          %{
            gm
            | waiting_clients:
                {left_waiting_clients, Map.put(right_waiting_clients, client_pid, output_name)}
          }

        # there is data, send it to the client
        {output_data, _} ->
          send(client_pid, {:client, output_name, {:data, output_data}})

          %{
            gm
            | data: {left_data, Map.put(right_data, output_name, [])},
              data_size: {elem(gm.data_size, 0), elem(gm.data_size, 1) - length(output_data)}
          }
      end

    any_data_taken = elem(gm.data_size, 1) < initital_right_data_size

    waiting_tasks =
      if any_data_taken and elem(gm.data_size, 1) < gm.buffer do
        {left_waiting_tasks, right_waiting_tasks} = gm.waiting_tasks
        send_continue_to_tasks(right_waiting_tasks)

        {left_waiting_tasks, right_waiting_tasks}
      else
        gm.waiting_tasks
      end

    gm = %{gm | waiting_tasks: waiting_tasks}
    after_action(gm)
  end

  defp after_action(gm) do
    cond do
      ready_to_stop?(gm) ->
        {left_data, left_data_size} =
          send_data_to_clients(elem(gm.data, 0), elem(gm.data_size, 0), elem(gm.clients, 0))

        {right_data, right_data_size} =
          send_data_to_clients(elem(gm.data, 1), elem(gm.data_size, 1), elem(gm.clients, 1))

        send_done_to_clients(elem(gm.clients, 0))
        send_done_to_clients(elem(gm.clients, 1))

        if gm.before_stop, do: gm.before_stop.()

        {:stop, :normal,
         %{
           gm
           | data: {left_data, right_data},
             data_size: {left_data_size, right_data_size},
             tasks_started: {false, false},
             tasks_run: {false, false},
             waiting_clients: {%{}, %{}},
             clients: {%{}, %{}}
         }}

      ready_to_finish?(gm) ->
        {left_data, left_data_size} =
          send_data_to_clients(
            elem(gm.data, 0),
            elem(gm.data_size, 0),
            elem(gm.waiting_clients, 0)
          )

        {right_data, right_data_size} =
          send_data_to_clients(
            elem(gm.data, 1),
            elem(gm.data_size, 1),
            elem(gm.waiting_clients, 1)
          )

        send_done_to_clients(elem(gm.waiting_clients, 0))
        send_done_to_clients(elem(gm.waiting_clients, 1))

        {:noreply,
         %{
           gm
           | data: {left_data, right_data},
             data_size: {left_data_size, right_data_size},
             tasks_run: {false, false},
             waiting_clients: {%{}, %{}}
         }}

      true ->
        {:noreply, gm}
    end
  end

  defp send_continue_to_tasks(tasks) do
    Enum.each(tasks, fn {task_pid, input_name} ->
      send(task_pid, {:task, input_name, :continue})
    end)
  end

  defp send_done_to_clients(clients) do
    Enum.each(clients, fn {client_pid, output_name} ->
      send(client_pid, {:client, output_name, :done})
    end)
  end

  defp send_data_to_clients(data, data_size, clients) do
    Enum.reduce(clients, {data, data_size}, fn {client_pid, output_name}, {data, count} ->
      case Map.get(data, output_name, []) do
        [] ->
          {data, count}

        data_for_client ->
          send(client_pid, {:client, output_name, {:data, data_for_client}})
          {Map.put(data, output_name, []), count - length(data_for_client)}
      end
    end)
  end

  def ready_to_finish?(gm) do
    map_size(elem(gm.tasks, 0)) == 0 and
      map_size(elem(gm.waiting_tasks, 0)) == 0 and
      map_size(elem(gm.tasks, 1)) == 0 and
      map_size(elem(gm.waiting_tasks, 1)) == 0
  end

  defp ready_to_stop?(gm) do
    ready_to_finish?(gm) and gm.stopping
  end

  @impl true
  def handle_info({ref, pid}, gm) do
    # Task finished when started with TaskSupervisor
    Process.demonitor(ref, [:flush])
    task_finished(pid, gm)
  end

  def handle_info({:DOWN, ref, :process, pid, :normal}, gm) do
    # Task finished when started in Composite
    Process.demonitor(ref, [:flush])
    task_finished(pid, gm)
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, gm) do
    # The task failed
    tasks =
      case Tasks.handle_task_error(gm, pid) do
        {:left, task_pid, name} ->
          {left_tasks, right_tasks} = gm.tasks

          left_tasks =
            left_tasks
            |> Map.delete(pid)
            |> Map.put(task_pid, name)

          {left_tasks, right_tasks}

        {:right, task_pid, name} ->
          {left_tasks, right_tasks} = gm.tasks

          right_tasks =
            right_tasks
            |> Map.delete(pid)
            |> Map.put(task_pid, name)

          {left_tasks, right_tasks}
      end

    {:noreply, %{gm | tasks: tasks}}
  end

  defp task_finished(pid, %__MODULE__{no_wait: false} = gm) do
    gm = %{
      gm
      | tasks: {Map.delete(elem(gm.tasks, 0), pid), Map.delete(elem(gm.tasks, 1), pid)},
        waiting_tasks:
          {Map.delete(elem(gm.waiting_tasks, 0), pid), Map.delete(elem(gm.waiting_tasks, 1), pid)}
    }

    after_action(gm)
  end

  defp task_finished(_pid, %__MODULE__{no_wait: true} = gm) do
    send_data_to_clients(elem(gm.data, 0), elem(gm.data_size, 0), elem(gm.clients, 0))
    send_data_to_clients(elem(gm.data, 1), elem(gm.data_size, 1), elem(gm.clients, 1))
    send_done_to_clients(elem(gm.clients, 0))
    send_done_to_clients(elem(gm.clients, 1))

    {:noreply,
     %{
       gm
       | tasks_run: {false, false},
         tasks: {%{}, %{}}
     }}
  end
end
