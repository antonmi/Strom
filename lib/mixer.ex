defmodule Strom.Mixer do
  use GenServer

  @buffer 1000

  defstruct streams: %{},
            pid: nil,
            running: false,
            data: %{},
            buffer: @buffer,
            no_data_counter: 0,
            tasks: %{},
            consumer: nil


  def start(opts \\ []) when is_list(opts) do
    state = %__MODULE__{
      buffer: Keyword.get(opts, :buffer, @buffer)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def init(%__MODULE__{} = mixer) do
    {:ok, %{mixer | pid: self()}}
  end

  def call(flow, %__MODULE__{} = mixer, to_mix, name) when is_map(flow) and is_list(to_mix) do
    to_mix =
      Enum.reduce(to_mix, %{}, fn name, acc ->
        Map.put(acc, name, fn _el -> true end)
      end)

    call(flow, mixer, to_mix, name)
  end

  def call(flow, %__MODULE__{} = mixer, to_mix, name) when is_map(flow) and is_map(to_mix) do
    streams_to_mix =
      Enum.reduce(to_mix, %{}, fn {name, fun}, acc ->
        Map.put(acc, {name, fun}, Map.fetch!(flow, name))
      end)

    :ok = GenServer.call(mixer.pid, {:run_streams, streams_to_mix})

    new_stream =
      Stream.resource(
        fn -> GenServer.call(mixer.pid, {:register_consumer, self()}) end,
        fn mixer ->

          case GenServer.call(mixer.pid, :get_data) do
            {:ok, {data, no_data_counter}} ->
##              maybe_wait(no_data_counter, 0)
#              if rem(no_data_counter, 10) == 9 do
              if length(data) == 0 do
                                  IO.inspect(no_data_counter, label: "no_data_counter_mixer: #{name}")
                receive do
                  :continue ->
                    flush()
                end
              end
              {data, mixer}

            {:error, :done} ->
              {:halt, mixer}
          end
        end,
        fn mixer -> mixer end
      )

    flow
    |> Map.drop(Map.keys(to_mix))
    |> Map.put(name, new_stream)
  end

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp run_streams(streams, pid, buffer) do
    Enum.reduce(streams, %{}, fn {{name, fun}, stream}, acc ->
      task = async_run_stream({name, fun}, stream, buffer, pid)
      Map.put(acc, {name, fun}, task)
    end)
  end

  defp async_run_stream({name, fun}, stream, buffer, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(buffer)
      |> Stream.each(fn chunk ->
        {chunk, _} = Enum.split_with(chunk, fun)
          GenServer.cast(pid, {:new_data, {name, fun}, chunk})
          receive do
            :continue ->
              flush()
          end
      end)
      |> Stream.run()

      GenServer.call(pid, {:done, {name, fun}})
    end)
    |> IO.inspect(label: "mixer task")
  end

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  defp maybe_wait(current, allowed) do
    if current > allowed do
      diff = current - allowed
      to_sleep = trunc(:math.pow(2, diff))
      Process.sleep(to_sleep)
      to_sleep
    end
  end

  def handle_cast({:new_data, {name, fun}, chunk}, %__MODULE__{data: prev_data} = mixer) do
    if mixer.consumer, do: send(mixer.consumer, :continue)
    prev_data_from_stream = Map.get(prev_data, {name, fun}, [])
    data_from_stream = prev_data_from_stream ++ chunk

    data =  Map.put(prev_data, {name, fun}, data_from_stream)

    {:noreply, %{mixer | data: data}}
  end

  def handle_call({:run_streams, streams_to_mix}, _from, %__MODULE__{} = mixer) do
    tasks = run_streams(streams_to_mix, mixer.pid, mixer.buffer)

    {:reply, :ok, %{mixer | running: true, streams: streams_to_mix, tasks: tasks}}
  end

  def handle_call({:done, {name, fun}}, _from, %__MODULE__{streams: streams} = mixer) do
    streams = Map.delete(streams, {name, fun})
    {:reply, :ok, %{mixer | streams: streams, running: false}}
  end

  def handle_call(:get_data, _from, %__MODULE__{data: data, streams: streams} = mixer) do
    all_data = Enum.reduce(data, [], fn {_, d}, acc -> acc ++ d end)

    mixer.tasks
    |> Enum.shuffle()
    |> Enum.each(fn {{name, fun}, task} ->
      send(task.pid, :continue)
    end)

    if length(all_data) == 0 && map_size(streams) == 0 do
      {:reply, {:error, :done}, mixer}
    else
      data = Enum.reduce(data, %{}, fn {name, _}, acc -> Map.put(acc, name, []) end)
      no_data_counter = if length(all_data) == 0, do: mixer.no_data_counter + 1, else: 0

      mixer = %{
        mixer
        | data: data,
          no_data_counter: no_data_counter
      }

      {:reply, {:ok, {all_data, no_data_counter}}, mixer}
    end
  end

  def handle_call({:register_consumer, pid},_from,%__MODULE__{consumer: consumer} = mixer) do
    mixer = %{mixer | consumer: pid}
    {:reply, mixer, mixer}
  end

  def handle_call(:stop, _from, %__MODULE__{} = mixer) do
    {:stop, :normal, :ok, %{mixer | running: false}}
  end

  def handle_call(:__state__, _from, mixer), do: {:reply, mixer, mixer}

  def handle_info({_task_ref, :ok}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end
end
