defmodule Strom.Transformer do
  @moduledoc """
  Transforms a stream or several streams.
  It works as Stream.map/2 or Stream.transform/3.

      ## `map` example:
      iex> alias Strom.Transformer
      iex> transformer = :numbers |> Transformer.new(&(&1*2)) |> Transformer.start()
      iex> flow = %{numbers: [1, 2, 3]}
      iex> %{numbers: stream} = Transformer.call(flow, transformer)
      iex> Enum.to_list(stream)
      [2, 4, 6]

      ## `transform` example:
      iex> alias Strom.Transformer
      iex> fun = fn el, acc -> {[el, acc], acc + 10} end
      iex> transformer = :numbers |> Transformer.new(fun, 10) |> Transformer.start()
      iex> flow = %{numbers: [1, 2, 3]}
      iex> %{numbers: stream} = Transformer.call(flow, transformer)
      iex> Enum.to_list(stream)
      [1, 10, 2, 20, 3, 30]

      ## it can be applied to several streams:
      iex> alias Strom.Transformer
      iex> transformer = [:s1, :s2] |> Transformer.new(&(&1*2)) |> Transformer.start()
      iex> flow = %{s1: [1, 2, 3], s2: [4, 5, 6]}
      iex> %{s1: s1, s2: s2} = Transformer.call(flow, transformer)
      iex> {Enum.to_list(s1), Enum.to_list(s2)}
      {[2, 4, 6], [8, 10, 12]}
  """

  use GenServer

  @chunk 1

  defstruct pid: nil,
            opts: [],
            chunk: @chunk,
            input_streams: %{},
            function: nil,
            acc: nil,
            names: [],
            tasks: %{},
            clients: %{}

  @type t() :: %__MODULE__{}
  @type event() :: any()
  @type acc() :: any()

  @type func() ::
          (event() -> event())
          | (event(), acc() -> {[event()], acc()})

  @spec new(Strom.stream_name(), func(), acc(), list()) :: __MODULE__.t()
  def new(names, function, acc \\ nil, opts \\ []) when is_function(function) and is_list(opts) do
    function =
      if is_function(function, 1) do
        fn el, nil -> {[function.(el)], nil} end
      else
        function
      end

    %__MODULE__{
      function: function,
      acc: acc,
      names: names,
      opts: opts
    }
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{opts: opts} = transformer) do
    transformer = %{transformer | chunk: Keyword.get(opts, :chunk, @chunk)}

    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, transformer}},
        %{id: __MODULE__, start: {__MODULE__, :start_link, [transformer]}, restart: :temporary}
      )

    # TODO more explicit
    :sys.get_state(pid)
  end

  def start_link(%__MODULE__{} = transformer) do
    GenServer.start_link(__MODULE__, transformer)
  end

  @impl true
  def init(%__MODULE__{} = transformer) do
    {:ok, %{transformer | pid: self()}}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{names: names, function: function} = transformer)
      when is_map(flow) and is_function(function, 2) do
    names = if is_list(names), do: names, else: [names]

    input_streams =
      Enum.reduce(names, %{}, fn name, streams ->
        Map.put(streams, name, Map.fetch!(flow, name))
      end)

    tasks = GenServer.call(transformer.pid, {:run_inputs, input_streams})

    sub_flow =
      names
      |> Enum.reduce(%{}, fn name, flow ->
        stream =
          Stream.resource(
            fn ->
              GenServer.call(transformer.pid, {:register_client, name, self()})
              tasks
            end,
            fn tasks ->
              task = Map.fetch!(tasks, name)

              {tasks, task} =
                if Process.alive?(task.pid) do
                  {tasks, task}
                else
                  tasks = GenServer.call(transformer.pid, :tasks)
                  task = Map.fetch!(tasks, name)
                  {tasks, task}
                end

              send(task.pid, {:get_data, self()})

              receive do
                {^name, :done} ->
                  {:halt, tasks}

                {^name, data} ->
                  {data, tasks}
              end
            end,
            fn tasks -> tasks end
          )

        Map.put(flow, name, stream)
      end)

    flow
    |> Map.drop(names)
    |> Map.merge(sub_flow)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid}) do
    GenServer.call(pid, :stop)
  end

  defp run_inputs(streams, transformer) do
    Enum.reduce(streams, %{}, fn {name, stream}, streams_acc ->
      task = async_run_stream({name, stream}, transformer)
      Map.put(streams_acc, name, task)
    end)
  end

  defp async_run_stream({name, stream}, transformer) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        stream
        |> Stream.chunk_every(transformer.chunk)
        |> Stream.transform(transformer.acc, fn chunk, acc ->
          {chunk, new_acc} =
            Enum.reduce(chunk, {[], acc}, fn el, {events, acc} ->
              {new_events, acc} = transformer.function.(el, acc)
              {events ++ new_events, acc}
            end)

          receive do
            {:get_data, client_pid} ->
              send(client_pid, {name, chunk})
          end

          # TODO update acc in transformer
          # or even update it on each function call above
          # might be configurable

          {[], new_acc}
        end)
        |> Stream.run()

        receive do
          {:get_data, client_pid} ->
            send(client_pid, {name, :done})
        end

        {:task_done, name}
      end
    )
  end

  @impl true
  def handle_call({:run_inputs, streams_to_call}, _from, %__MODULE__{} = transformer) do
    tasks = run_inputs(streams_to_call, transformer)

    {:reply, tasks, %{transformer | tasks: tasks, input_streams: streams_to_call}}
  end

  @impl true
  def handle_call(:tasks, _from, %__MODULE__{tasks: tasks} = transformer) do
    {:reply, tasks, transformer}
  end

  @impl true
  def handle_call(
        {:register_client, name, pid},
        _from,
        %__MODULE__{clients: clients} = transformer
      ) do
    transformer = %{transformer | clients: Map.put(clients, name, pid)}
    {:reply, clients, transformer}
  end

  def handle_call(:stop, _from, %__MODULE__{} = transformer) do
    Enum.each(transformer.tasks, fn {_name, task_pid} ->
      DynamicSupervisor.terminate_child(Strom.TaskSupervisor, task_pid)
    end)

    {:stop, :normal, :ok, transformer}
  end

  @impl true
  def handle_info({_task_ref, {:task_done, name}}, transformer) do
    tasks = Map.delete(transformer.tasks, name)

    {:noreply, %{transformer | tasks: tasks}}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, transformer) do
    {:noreply, transformer}
  end

  def handle_info({:DOWN, _task_ref, :process, task_pid, _not_normal}, transformer) do
    {name, _task} = Enum.find(transformer.tasks, fn {_name, task} -> task.pid == task_pid end)
    stream = Map.fetch!(transformer.input_streams, name)

    new_task = async_run_stream({name, stream}, transformer)
    tasks = Map.put(transformer.tasks, name, new_task)

    send(transformer.clients[name], {name, []})
    {:noreply, %{transformer | tasks: tasks}}
  end
end
