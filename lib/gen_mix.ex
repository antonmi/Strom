defmodule Strom.GenMix do
  use GenServer

  @buffer 1000

  defstruct pid: nil,
            inputs: [],
            outputs: [],
            opts: [],
            flow_pid: nil,
            sup_pid: nil,
            running: false,
            buffer: @buffer,
            producers: %{},
            consumers: %{}

  alias Strom.GenMix.Consumer

  def start(opts \\ [])

  def start(%__MODULE__{opts: opts} = gen_mix) when is_list(opts) do
    gen_mix = %{
      gen_mix
      | buffer: Keyword.get(opts, :buffer, @buffer)
    }

    if gen_mix.sup_pid do
      DynamicSupervisor.start_child(gen_mix.sup_pid, {__MODULE__, gen_mix})
    else
      start_link(gen_mix)
    end
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

  def stop(pid, sup_pid) do
    if sup_pid do
      :ok
    else
      GenServer.call(pid, :stop)
    end
  end

  defp run_inputs(streams, pid, buffer) do
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

      GenServer.cast(pid, {:done, {name, fun}})
    end)
  end

  defp flush do
    receive do
      _ -> flush()
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

    {sub_flow, mix} =
      mix.outputs
      |> Enum.reduce({%{}, mix}, fn {name, fun}, {flow, mix} ->
        consumer = Consumer.start({name, fun}, mix.pid)

        mix = %{mix | consumers: Map.put(mix.consumers, {name, fun}, consumer)}

        stream = Consumer.call(consumer)
        {Map.put(flow, name, stream), mix}
      end)

    producers = run_inputs(input_streams, mix.pid, mix.buffer)

    flow =
      flow
      |> Map.drop(Map.keys(mix.inputs))
      |> Map.merge(sub_flow)

    {:reply, flow, %{mix | running: true, producers: producers}}
  end


  def handle_call(:stop, _from, %__MODULE__{} = mix) do
    {:stop, :normal, :ok, %{mix | running: false}}
  end

  @impl true
  def handle_cast({:new_data, {_name, _fun}, chunk}, %__MODULE__{} = mix) do
    Enum.each(mix.consumers, fn {_, cons} ->
      GenServer.cast(cons.pid, {:put_data, chunk})
      GenServer.cast(cons.pid, :continue)
    end)

    {:noreply, mix}
  end

  def handle_cast({:done, {name, fun}}, %__MODULE__{} = mix) do
    mix = %{mix | producers: Map.delete(mix.producers, {name, fun})}

    if map_size(mix.producers) == 0 do
      Enum.each(mix.consumers, fn {_, cons} ->
        GenServer.cast(cons.pid, :continue)
        GenServer.cast(cons.pid, :stop)
      end)
    end

    {:noreply, mix}
  end

  def handle_cast({:consumer_got_data, {_name, _fun}}, %__MODULE__{} = mix) do
    Enum.each(mix.producers, fn {_, task} ->
      send(task.pid, :continue)
    end)

    {:noreply, mix}
  end

  @impl true
  def handle_info({_task_ref, :ok}, mix) do
    # do nothing for now
    {:noreply, mix}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mix) do
    # do nothing for now
    {:noreply, mix}
  end
end
