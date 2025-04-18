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
            tasks_started: false,
            asks: %{},
            data: %{},
            data_size: 0,
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

    sub_flow =
      gen_mix.outputs
      |> Enum.reduce(%{}, fn {output_stream_name, _fun}, flow ->
        stream =
          Stream.resource(
            fn ->
              GenServer.call(gen_mix.pid, {:client_runs_stream, input_streams})
            end,
            fn gen_mix_pid ->
              GenServer.cast(gen_mix_pid, {:ask, output_stream_name, self()})

              receive do
                {^output_stream_name, :done} ->
                  {:halt, gen_mix_pid}

                {^output_stream_name, events} ->
                  {events, gen_mix_pid}
              end
            end,
            fn gen_mix_pid -> gen_mix_pid end
          )

        Map.put(flow, output_stream_name, stream)
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

  defp async_run_stream({input_stream_name, input_stream_fun}, stream, gen_mix) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        stream
        |> Stream.chunk_every(gen_mix.chunk)
        |> Stream.each(fn chunk ->
          {chunk, _} = Enum.split_with(chunk, input_stream_fun)

          gen_mix.outputs
          |> Enum.reduce({%{}, false}, fn {output_stream_name, output_stream_fun}, {acc, any?} ->
            {data, _} = Enum.split_with(chunk, output_stream_fun)
            {Map.put(acc, output_stream_name, data), any? || Enum.any?(data)}
          end)
          |> case do
            {new_data, true} ->
              GenServer.cast(gen_mix.pid, {:new_data, input_stream_name, new_data, self()})

              receive do
                :continue_task ->
                  :ok
              end

            {_new_data, false} ->
              :ok
          end
        end)
        |> Stream.run()

        {:task_done, input_stream_name}
      end
    )
  end

  @impl true
  def handle_call(
        {:client_runs_stream, streams_to_call},
        _from,
        %__MODULE__{tasks_started: false} = gen_mix
      ) do
    tasks =
      run_inputs(streams_to_call, gen_mix)

    {:reply, gen_mix.pid,
     %{gen_mix | tasks_started: true, tasks: tasks, input_streams: streams_to_call}}
  end

  def handle_call(
        {:client_runs_stream, _streams_to_call},
        _from,
        %__MODULE__{tasks_started: true} = gen_mix
      ) do
    {:reply, gen_mix.pid, gen_mix}
  end

  def handle_call(:stop, _from, %__MODULE__{} = gen_mix) do
    Enum.each(Map.values(gen_mix.tasks), &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1.pid))

    {:stop, :normal, :ok, gen_mix}
  end

  @impl true
  def handle_cast({:new_data, input_stream_name, new_data, task_pid}, %__MODULE__{} = gen_mix) do
    {all_mix_data, remaining_asks, total_count} =
      Enum.reduce(new_data, {gen_mix.data, gen_mix.asks, 0}, fn {output_stream_name, data},
                                                                {all_mix_data, asks, count} ->
        prev_data = Map.get(all_mix_data, output_stream_name, [])
        data_for_output = prev_data ++ data

        {data_for_output, asks} =
          case {data_for_output, Map.get(asks, output_stream_name)} do
            {[], _} ->
              {data_for_output, asks}

            {data_for_output, nil} ->
              {data_for_output, asks}

            {data_for_output, client_pid} ->
              send(client_pid, {output_stream_name, data_for_output})
              {[], Map.delete(asks, output_stream_name)}
          end

        {Map.put(all_mix_data, output_stream_name, data_for_output), asks,
         count + length(data_for_output)}
      end)

    waiting_tasks =
      if total_count < gen_mix.buffer do
        send(task_pid, :continue_task)
        gen_mix.waiting_tasks
      else
        Map.put(gen_mix.waiting_tasks, input_stream_name, task_pid)
      end

    gen_mix = %{
      gen_mix
      | data: all_mix_data,
        data_size: total_count,
        asks: remaining_asks,
        waiting_tasks: waiting_tasks
    }

    {:noreply, gen_mix}
  end

  def handle_cast({:ask, output_stream_name, client_pid}, %__MODULE__{asks: asks} = gen_mix) do
    {asks, new_data, data_size_for_output} =
      case Map.get(gen_mix.data, output_stream_name, []) do
        [] ->
          if map_size(gen_mix.tasks) == 0 do
            send(client_pid, {output_stream_name, :done})
            {Map.delete(asks, output_stream_name), gen_mix.data, 0}
          else
            {Map.put(asks, output_stream_name, client_pid), gen_mix.data, 0}
          end

        events ->
          send(client_pid, {output_stream_name, events})
          {asks, Map.put(gen_mix.data, output_stream_name, []), length(events)}
      end

    new_data_size = gen_mix.data_size - data_size_for_output

    waiting_tasks =
      if new_data_size < gen_mix.buffer do
        Enum.each(Map.values(gen_mix.waiting_tasks), &send(&1, :continue_task))
        %{}
      else
        gen_mix.waiting_tasks
      end

    {:noreply,
     %{
       gen_mix
       | asks: asks,
         data: new_data,
         data_size: new_data_size,
         waiting_tasks: waiting_tasks
     }}
  end

  @impl true
  def handle_info({_task_ref, {:task_done, name}}, gen_mix) do
    tasks =
      if gen_mix.no_wait do
        Enum.each(Map.values(gen_mix.tasks), &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1.pid))
        %{}
      else
        Map.delete(gen_mix.tasks, name)
      end

    asks =
      case map_size(tasks) do
        0 ->
          Enum.each(gen_mix.asks, fn {output_stream_name, client_pid} ->
            send(client_pid, {output_stream_name, :done})
          end)

          %{}

        _more ->
          gen_mix.asks
      end

    {:noreply, %{gen_mix | tasks: tasks, asks: asks}}
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
