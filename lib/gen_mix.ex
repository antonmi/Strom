defmodule Strom.GenMix do
  @moduledoc """
  Generic functionality used by other components.
  """

  use GenServer

  @chunk 1
  @buffer 1000

  defstruct pid: nil,
            process_chunk: nil,
            inputs: [],
            outputs: %{},
            accs: %{},
            opts: [],
            chunk: @chunk,
            buffer: @buffer,
            no_wait: false,
            tasks: %{},
            tasks_started: false,
            tasks_run: false,
            asks: %{},
            data: %{},
            data_size: 0,
            waiting_tasks: %{}

  def start(%__MODULE__{process_chunk: process_chunk, opts: opts} = gen_mix) when is_list(opts) do
    gen_mix = %{
      gen_mix
      | process_chunk: if(process_chunk, do: process_chunk, else: &process_chunk/4),
        chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer),
        no_wait: Keyword.get(opts, :no_wait, false)
    }

    partitions = PartitionSupervisor.partitions(Strom.ComponentSupervisor)
    partition_key = Enum.random(1..partitions)

    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, partition_key}},
        %{id: __MODULE__, start: {__MODULE__, :start_link, [gen_mix]}, restart: :temporary}
      )

    %{gen_mix | pid: pid}
  end

  def start_link(%__MODULE__{} = gen_mix) do
    GenServer.start_link(__MODULE__, gen_mix)
  end

  @impl true
  def init(%__MODULE__{} = gen_mix) do
    {:ok, %{gen_mix | pid: self()}}
  end

  def call(flow, gen_mix) do
    input_streams =
      Enum.reduce(gen_mix.inputs, %{}, fn name, acc ->
        Map.put(acc, name, Map.fetch!(flow, name))
      end)

    gen_mix_pid = GenServer.call(gen_mix.pid, {:start_tasks, input_streams})
    sub_flow = build_sub_flow(gen_mix.outputs, gen_mix_pid)

    flow
    |> Map.drop(gen_mix.inputs)
    |> Map.merge(sub_flow)
  end

  defp build_sub_flow(outputs, gen_mix_pid) do
    Enum.reduce(outputs, %{}, fn {output_stream_name, _fun}, flow ->
      stream =
        Stream.resource(
          fn ->
            GenServer.call(gen_mix_pid, :run_tasks)
            gen_mix_pid
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
  end

  def stop(gen_mix) do
    GenServer.call(gen_mix.pid, :stop)
  end

  def process_chunk(_input_stream_name, chunk, outputs, nil) do
    outputs
    |> Enum.reduce({%{}, false, nil}, fn {output_stream_name, output_stream_fun},
                                         {acc, any?, nil} ->
      {data, _} = Enum.split_with(chunk, output_stream_fun)
      {Map.put(acc, output_stream_name, data), any? || Enum.any?(data), nil}
    end)
  end

  defp run_stream_in_task(input_stream_name, stream, gen_mix) do
    Task.Supervisor.start_child(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        acc =
          case GenServer.call(gen_mix.pid, {:register_task, input_stream_name, self()}) do
            {true, acc} ->
              acc

            {false, _} ->
              receive do
                {:run_task, acc} ->
                  acc
              end
          end

        stream
        |> Stream.chunk_every(gen_mix.chunk)
        |> Stream.transform(
          fn -> acc end,
          fn chunk, acc ->
            case gen_mix.process_chunk.(input_stream_name, chunk, gen_mix.outputs, acc) do
              {new_data, true, new_acc} ->
                GenServer.cast(gen_mix.pid, {:new_data, input_stream_name, {new_data, new_acc}})

                receive do
                  :continue_task ->
                    :ok
                end

                {[], new_acc}

              {_new_data, false, new_acc} ->
                {[], new_acc}
            end
          end,
          fn acc -> acc end
        )
        |> Stream.run()

        GenServer.cast(gen_mix.pid, {:task_done, input_stream_name})
      end,
      restart: :transient
    )
  end

  @impl true
  def handle_call(
        {:start_tasks, input_streams},
        _from,
        %__MODULE__{tasks_started: false} = gen_mix
      ) do
    tasks =
      Enum.reduce(input_streams, %{}, fn {name, stream}, acc ->
        {:ok, task_pid} = run_stream_in_task(name, stream, gen_mix)

        Map.put(acc, name, task_pid)
      end)

    {:reply, gen_mix.pid, %{gen_mix | tasks_started: true, tasks: tasks}}
  end

  def handle_call(
        {:start_tasks, _input_streams},
        _from,
        %__MODULE__{tasks_started: true} = gen_mix
      ) do
    {:reply, gen_mix.pid, gen_mix}
  end

  def handle_call(:run_tasks, _from, %__MODULE__{tasks_started: true, tasks_run: false} = gen_mix) do
    Enum.each(gen_mix.tasks, fn {name, task_pid} ->
      send(task_pid, {:run_task, Map.get(gen_mix.accs, name)})
    end)

    {:reply, gen_mix.pid, %{gen_mix | tasks_run: true}}
  end

  def handle_call(:run_tasks, _from, %__MODULE__{tasks_run: true} = gen_mix) do
    {:reply, gen_mix.pid, gen_mix}
  end

  def handle_call({:register_task, name, task_pid}, _from, %__MODULE__{} = gen_mix) do
    tasks = Map.put(gen_mix.tasks, name, task_pid)

    {:reply, {gen_mix.tasks_run, Map.get(gen_mix.accs, name)}, %{gen_mix | tasks: tasks}}
  end

  def handle_call(:stop, _from, %__MODULE__{} = gen_mix) do
    Enum.each(
      Map.values(gen_mix.tasks),
      &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1)
    )

    {:stop, :normal, :ok, gen_mix}
  end

  defp process_new_data(gen_mix, new_data) do
    Enum.reduce(new_data, {gen_mix.data, gen_mix.asks, 0}, fn {output_stream_name, data},
                                                              {all_mix_data, asks, count} ->
      data_for_output = Map.get(all_mix_data, output_stream_name, []) ++ data

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
  end

  defp continue_or_wait(gen_mix, input_stream_name, total_count) do
    case {Map.get(gen_mix.tasks, input_stream_name), total_count < gen_mix.buffer} do
      {nil, _} ->
        gen_mix.waiting_tasks

      {task_pid, true} ->
        send(task_pid, :continue_task)
        gen_mix.waiting_tasks

      {task_pid, false} ->
        Map.put(gen_mix.waiting_tasks, input_stream_name, task_pid)
    end
  end

  @impl true
  def handle_cast(
        {:new_data, input_stream_name, {new_data, new_acc}},
        %__MODULE__{} = gen_mix
      ) do
    {all_mix_data, remaining_asks, total_count} = process_new_data(gen_mix, new_data)
    waiting_tasks = continue_or_wait(gen_mix, input_stream_name, total_count)

    {:noreply,
     %{
       gen_mix
       | data: all_mix_data,
         accs: Map.put(gen_mix.accs, input_stream_name, new_acc),
         data_size: total_count,
         asks: remaining_asks,
         waiting_tasks: waiting_tasks
     }}
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

          {Map.delete(asks, output_stream_name), Map.put(gen_mix.data, output_stream_name, []),
           length(events)}
      end

    new_data_size = gen_mix.data_size - data_size_for_output

    waiting_tasks =
      if new_data_size < gen_mix.buffer do
        Enum.each(gen_mix.waiting_tasks, fn {_name, task_pid} ->
          send(task_pid, :continue_task)
        end)

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

  def handle_cast({:task_done, input_stream_name}, %__MODULE__{} = gen_mix) do
    {tasks, waiting_tasks} =
      if gen_mix.no_wait do
        Enum.each(
          Map.values(gen_mix.tasks),
          &DynamicSupervisor.terminate_child(Strom.TaskSupervisor, &1)
        )

        {%{}, %{}}
      else
        {Map.delete(gen_mix.tasks, input_stream_name),
         Map.delete(gen_mix.waiting_tasks, input_stream_name)}
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

    {:noreply, %{gen_mix | tasks: tasks, waiting_tasks: waiting_tasks, asks: asks}}
  end
end
