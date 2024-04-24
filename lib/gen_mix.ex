defmodule Strom.GenMix do
  @moduledoc """
  Generic functionality used by `Strom.Mixer` and `Strom.Splitter`.
  """

  use GenServer

  @chunk 1
  @buffer 1000

  defstruct pid: nil,
            inputs: [],
            outputs: [],
            opts: [],
            chunk: @chunk,
            buffer: @buffer,
            input_streams: %{},
            tasks: %{},
            data: %{},
            waiting_clients: %{}

  def start(%__MODULE__{opts: opts} = gen_mix) when is_list(opts) do
    gen_mix = %{
      gen_mix
      | chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer)
    }

    start_link(gen_mix)
  end

  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state)
  end

  @impl true
  def init(%__MODULE__{} = mix) do
    {:ok, %{mix | pid: self()}}
  end

  def call(flow, pid) do
    GenServer.call(pid, {:call, flow})
  end

  def stop(pid) do
    GenServer.call(pid, :stop)
  end

  defp run_inputs(streams, mix) do
    Enum.reduce(streams, %{}, fn {{name, fun}, stream}, acc ->
      task = async_run_stream({name, fun}, stream, mix)
      Map.put(acc, name, task)
    end)
  end

  defp async_run_stream({name, fun}, stream, mix) do
    Task.Supervisor.async_nolink(Strom.TaskSupervisor, fn ->
      stream
      |> Stream.chunk_every(mix.chunk)
      |> Stream.each(fn chunk ->
        {chunk, _} = Enum.split_with(chunk, fun)

        new_data =
          Enum.reduce(mix.outputs, %{}, fn {name, fun}, acc ->
            {data, _} = Enum.split_with(chunk, fun)
            Map.put(acc, name, data)
          end)

        GenServer.cast(mix.pid, {:new_data, name, new_data})

        receive do
          :continue_task ->
            flush(:continue_task)
        end
      end)
      |> Stream.run()
    end)
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
  def handle_call({:call, flow}, _from, %__MODULE__{} = mix) do
    input_streams =
      Enum.reduce(mix.inputs, %{}, fn {name, fun}, acc ->
        Map.put(acc, {name, fun}, Map.fetch!(flow, name))
      end)

    tasks = run_inputs(input_streams, mix)

    sub_flow =
      mix.outputs
      |> Enum.reduce(%{}, fn {name, _fun}, flow ->
        stream =
          Stream.resource(
            fn ->
              nil
            end,
            fn nil ->
              case GenServer.call(mix.pid, {:get_data, name}, :infinity) do
                {:data, data} ->
                  {data, nil}

                :done ->
                  {:halt, nil}

                :pause ->
                  receive do
                    :continue_client ->
                      flush(:continue_client)
                      {[], nil}
                  end
              end
            end,
            fn nil -> nil end
          )

        Map.put(flow, name, stream)
      end)

    flow =
      flow
      |> Map.drop(Map.keys(mix.inputs))
      |> Map.merge(sub_flow)

    {:reply, flow, %{mix | tasks: tasks, input_streams: input_streams}}
  end

  def handle_call(:stop, _from, %__MODULE__{} = mix) do
    {:stop, :normal, :ok, mix}
  end

  def handle_call({:get_data, name}, {pid, _ref}, mix) do
    data = Map.get(mix.data, name, [])
    mix = %{mix | data: Map.put(mix.data, name, [])}

    total_count = Enum.reduce(mix.data, 0, fn {_name, data}, count -> count + length(data) end)

    if total_count <= mix.buffer do
      Enum.each(mix.tasks, fn {_, task} -> send(task.pid, :continue_task) end)
    end

    cond do
      length(data) == 0 and map_size(mix.tasks) == 0 ->
        {:reply, :done, mix}

      length(data) == 0 ->
        waiting_clients = Map.put(mix.waiting_clients, name, pid)
        {:reply, :pause, %{mix | waiting_clients: waiting_clients}}

      true ->
        {:reply, {:data, data}, mix}
    end
  end

  @impl true
  def handle_cast({:new_data, name, new_data}, %__MODULE__{} = mix) do
    {all_mix_data, total_count} =
      Enum.reduce(new_data, {mix.data, 0}, fn {name, data}, {all_mix_data, count} ->
        prev_data = Map.get(all_mix_data, name, [])
        all_data = prev_data ++ data
        {Map.put(all_mix_data, name, all_data), count + length(all_data)}
      end)

    Enum.each(mix.waiting_clients, fn {_name, client_pid} ->
      send(client_pid, :continue_client)
    end)

    if total_count < mix.buffer do
      task = Map.fetch!(mix.tasks, name)
      send(task.pid, :continue_task)
    end

    mix = %{mix | data: all_mix_data, waiting_clients: %{}}

    {:noreply, mix}
  end

  @impl true
  def handle_info({_task_ref, :ok}, mix) do
    # do nothing for now
    {:noreply, mix}
  end

  def handle_info({:DOWN, _task_ref, :process, task_pid, :normal}, mix) do
    {name, _task} = Enum.find(mix.tasks, fn {_name, task} -> task.pid == task_pid end)
    mix = %{mix | tasks: Map.delete(mix.tasks, name)}

    Enum.each(mix.waiting_clients, fn {_name, client_pid} ->
      send(client_pid, :continue_client)
    end)

    {:noreply, %{mix | waiting_clients: %{}}}
  end

  def handle_info({:DOWN, _task_ref, :process, task_pid, _not_normal}, mix) do
    {name, _task} = Enum.find(mix.tasks, fn {_name, task} -> task.pid == task_pid end)
    {{^name, function}, stream} = Enum.find(mix.input_streams, fn {{n, _}, _} -> n == name end)
    new_task = async_run_stream({name, function}, stream, mix)
    tasks = Map.put(mix.tasks, name, new_task)

    {:noreply, %{mix | tasks: tasks}}
  end
end
