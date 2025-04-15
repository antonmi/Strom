defmodule Strom.GenMix do
  @moduledoc """
  Generic functionality used by `Strom.Mixer` and `Strom.Splitter`.
  """

  use GenServer

  @chunk 1
  @buffer 1000

  defstruct pid: nil,
            inputs: %{},
            outputs: %{},
            opts: [],
            chunk: @chunk,
            buffer: @buffer,
            no_wait: false,
            input_streams: %{},
            tasks: %{},
            data: %{},
            waiting_clients: %{},
            waiting_tasks: %{}

  def start(%__MODULE__{opts: opts} = gen_mix) when is_list(opts) do
    gen_mix = %{
      gen_mix
      | chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer),
        no_wait: Keyword.get(opts, :no_wait, false)
    }

    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, gen_mix}},
        %{id: __MODULE__, start: {__MODULE__, :start_link, [gen_mix]}, restart: :temporary}
      )

    %{gen_mix | pid: pid}
  end

  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state)
  end

  @impl true
  def init(%__MODULE__{} = gen_mix) do
    {:ok, %{gen_mix | pid: self()}}
  end

  def call(flow, gen_mix) do
    input_streams =
      Enum.reduce(gen_mix.inputs, %{}, fn {name, fun}, acc ->
        Map.put(acc, {name, fun}, Map.fetch!(flow, name))
      end)

    tasks = GenServer.call(gen_mix.pid, {:run_inputs, input_streams})

    sub_flow =
      gen_mix.outputs
      |> Enum.reduce(%{}, fn {name, _fun}, flow ->
        stream =
          Stream.resource(
            fn ->
              tasks
            end,
            fn tasks ->
              if map_size(tasks) > 0 do
                task = Enum.random(Map.values(tasks))
                send(task.pid, {:ask, name, self()})

                receive do
                  {^name, {:done, stream_name}, events} ->
                    {events, Map.delete(tasks, stream_name)}

                  {^name, events} ->
                    {events, tasks}
                    # after
                end
              else
                {:halt, %{}}
              end
            end,
            fn %{} -> %{} end
          )

        Map.put(flow, name, stream)
      end)

    flow
    |> Map.drop(Map.keys(gen_mix.inputs))
    |> Map.merge(sub_flow)
  end

  def stop(gen_mix) do
    GenServer.call(gen_mix.pid, :stop)
  end

  defp run_inputs(streams, gen_mix) do
    Enum.reduce(streams, %{}, fn {{name, fun}, stream}, acc ->
      task = async_run_stream({name, fun}, stream, gen_mix)
      Map.put(acc, name, task)
    end)
  end

  defp async_run_stream({input_stream_name, fun}, stream, gen_mix) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        stream
        |> Stream.chunk_every(gen_mix.chunk)
        |> Stream.transform(
          fn -> %{} end,
          fn chunk, stored ->
            {chunk, _} = Enum.split_with(chunk, fun)

            stored =
              Enum.reduce(gen_mix.outputs, stored, fn {name, fun}, acc ->
                {new_events, _} = Enum.split_with(chunk, fun)
                stored_events = Map.get(stored, name, [])
                Map.put(acc, name, stored_events ++ new_events)
              end)

            receive do
              {:ask, name, client_pid} ->
                send(client_pid, {name, Map.fetch!(stored, name)})
                stored = Map.put(stored, name, [])
                {[], stored}
            after
              1 ->
                events_count =
                  Enum.reduce(stored, 0, fn {_k, v}, counter -> counter + length(v) end)

                if events_count < gen_mix.buffer do
                  {[], stored}
                else
                  receive do
                    {:ask, name, client_pid} ->
                      send(client_pid, {name, Map.fetch!(stored, name)})
                      stored = Map.put(stored, name, [])
                      {[], stored}
                  end
                end
            end
          end,
          fn stored ->
            IO.inspect(stored, label: :stored)

            Enum.each(stored, fn {name, events} ->
              receive do
                {:ask, ^name, client_pid} ->
                  send(client_pid, {name, {:done, input_stream_name}, Map.get(stored, name, [])})
              end
            end)
          end
        )
        |> Stream.run()

        {:task_done, input_stream_name}
      end
    )
  end

  defp flush(message) do
    receive do
      ^message ->
        flush(message)
    after
      0 -> :ok
    end
  end

  @impl true
  def handle_call({:run_inputs, streams_to_call}, _from, gen_mix) do
    tasks = run_inputs(streams_to_call, gen_mix)

    {:reply, tasks, %{gen_mix | tasks: tasks, input_streams: streams_to_call}}
  end

  def handle_call(:stop, _from, %__MODULE__{} = gen_mix) do
    Enum.each(gen_mix.tasks, fn {_name, task} ->
      DynamicSupervisor.terminate_child(Strom.TaskSupervisor, task.pid)
    end)

    {:stop, :normal, :ok, gen_mix}
  end

  def handle_call({:get_data, name}, {pid, _ref}, gen_mix) do
    data = Map.get(gen_mix.data, name, [])

    total_count =
      Enum.reduce(gen_mix.data, 0, fn {_name, data}, count -> count + length(data) end)

    waiting_tasks =
      if total_count <= gen_mix.buffer and gen_mix.no_wait != :first_stream_done do
        Enum.each(gen_mix.waiting_tasks, fn {_name, task_pid} ->
          send(task_pid, :continue_task)
        end)

        %{}
      else
        gen_mix.waiting_tasks
      end

    gen_mix = %{gen_mix | data: Map.put(gen_mix.data, name, []), waiting_tasks: waiting_tasks}

    cond do
      length(data) == 0 and
          (map_size(gen_mix.tasks) == 0 or gen_mix.no_wait == :first_stream_done) ->
        {:reply, :done, gen_mix}

      length(data) == 0 ->
        waiting_clients = Map.put(gen_mix.waiting_clients, name, pid)
        {:reply, :pause, %{gen_mix | waiting_clients: waiting_clients}}

      true ->
        {:reply, {:data, data}, gen_mix}
    end
  end

  @impl true
  def handle_cast({:new_data, name, new_data, task_pid}, %__MODULE__{} = gen_mix) do
    {all_mix_data, total_count} =
      Enum.reduce(new_data, {gen_mix.data, 0}, fn {name, data}, {all_mix_data, count} ->
        prev_data = Map.get(all_mix_data, name, [])
        all_data = prev_data ++ data
        {Map.put(all_mix_data, name, all_data), count + length(all_data)}
      end)

    Enum.each(gen_mix.waiting_clients, fn {_name, client_pid} ->
      send(client_pid, :continue_client)
    end)

    waiting_tasks =
      if total_count < gen_mix.buffer and gen_mix.no_wait != :first_stream_done do
        task = Map.get(gen_mix.tasks, name)
        send(task.pid, :continue_task)
        gen_mix.waiting_tasks
      else
        Map.put(gen_mix.waiting_tasks, name, task_pid)
      end

    gen_mix = %{gen_mix | data: all_mix_data, waiting_clients: %{}, waiting_tasks: waiting_tasks}

    {:noreply, gen_mix}
  end

  @impl true
  def handle_info({_task_ref, {:task_done, name}}, gen_mix) do
    gen_mix = %{gen_mix | tasks: Map.delete(gen_mix.tasks, name)}

    Enum.each(gen_mix.waiting_clients, fn {_name, client_pid} ->
      send(client_pid, :continue_client)
    end)

    gen_mix =
      if gen_mix.no_wait do
        %{gen_mix | no_wait: :first_stream_done}
      else
        gen_mix
      end

    {:noreply, %{gen_mix | waiting_clients: %{}}}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, gen_mix) do
    # do nothing
    {:noreply, gen_mix}
  end

  def handle_info({:DOWN, _task_ref, :process, task_pid, _not_normal}, gen_mix) do
    {name, _task} = Enum.find(gen_mix.tasks, fn {_name, task} -> task.pid == task_pid end)

    {{^name, function}, stream} =
      Enum.find(gen_mix.input_streams, fn {{n, _}, _} -> n == name end)

    new_task = async_run_stream({name, function}, stream, gen_mix)
    tasks = Map.put(gen_mix.tasks, name, new_task)

    {:noreply, %{gen_mix | tasks: tasks}}
  end
end
