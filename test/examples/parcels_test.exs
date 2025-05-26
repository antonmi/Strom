defmodule Strom.Examples.ParcelsTest do
  use ExUnit.Case

  alias Strom.Composite

  defmodule GenData do
    import Strom.DSL

    alias Strom.Sink.WriteLines

    defmodule BuildEvent do
      def call(:tick, last_order) do
        occurred_at = DateTime.add(last_order[:occurred_at], :rand.uniform(10), :second)
        to_ship = :rand.uniform(5)
        order_number = last_order[:order_number] + 1

        order = %{
          type: "ORDER_CREATED",
          occurred_at: occurred_at,
          order_number: order_number,
          to_ship: to_ship
        }

        {parcels, _} =
          Enum.reduce(1..to_ship, {[], order[:occurred_at]}, fn _i, {acc, occurred_at} ->
            occurred_at = DateTime.add(occurred_at, :rand.uniform(3 * 24 * 3600), :second)

            parcel = %{
              type: "PARCEL_SHIPPED",
              occurred_at: occurred_at,
              order_number: order_number
            }

            {[parcel | acc], occurred_at}
          end)

        {[order | parcels], order}
      end
    end

    def order_to_string(order) do
      "#{order[:type]},#{DateTime.to_iso8601(order[:occurred_at])},#{order[:order_number]},#{order[:to_ship]}"
    end

    def parcel_to_string(parcel) do
      "#{parcel[:type]},#{DateTime.to_iso8601(parcel[:occurred_at])},#{parcel[:order_number]}"
    end

    @chunk 1000

    def components() do
      partitions = %{
        orders: &(&1[:type] == "ORDER_CREATED"),
        parcels: &(&1[:type] == "PARCEL_SHIPPED")
      }

      acc = %{
        occurred_at: DateTime.add(DateTime.now!("Etc/UTC"), -(3600 * 24 * 30), :second),
        order_number: 0
      }

      [
        transform(:stream, &BuildEvent.call/2, acc, chunk: @chunk),
        split(:stream, partitions),
        transform(:orders, &__MODULE__.order_to_string/1, nil, chunk: @chunk),
        transform(:parcels, &__MODULE__.parcel_to_string/1, nil, chunk: @chunk),
        sink(:orders, WriteLines.new("test/examples/parcels/orders.csv")),
        sink(:parcels, WriteLines.new("test/examples/parcels/parcels.csv"), sync: true)
      ]
    end
  end

  defmodule ParcelsFlow do
    alias Strom.Source.ReadLines
    alias Strom.Sink.WriteLines

    import Strom.DSL

    @seconds_in_week 3600 * 24 * 7

    def build_order(event) do
      #            if Enum.random(1..10) == 3, do: Process.sleep(1)
      list = String.split(event, ",")
      {:ok, occurred_at, _} = DateTime.from_iso8601(Enum.at(list, 1))

      %{
        type: Enum.at(list, 0),
        occurred_at: occurred_at,
        order_number: String.to_integer(Enum.at(list, 2)),
        to_ship: String.to_integer(Enum.at(list, 3))
      }
    end

    def build_parcel(event) do
      #      if Enum.random(1..100) == 3, do: raise "error"
      list = String.split(event, ",")
      {:ok, occurred_at, _} = DateTime.from_iso8601(Enum.at(list, 1))

      %{
        type: Enum.at(list, 0),
        occurred_at: occurred_at,
        order_number: String.to_integer(Enum.at(list, 2))
      }
    end

    def order_seen(event, memo) do
      order_number = event[:order_number]

      case event[:type] do
        "ORDER_CREATED" ->
          {[event], order_number}

        "PARCEL_SHIPPED" ->
          case order_number > memo do
            true ->
              {[{:not_seen, event}], memo}

            false ->
              {[event], memo}
          end
      end
    end

    def force_order({:not_seen, parcel}, memo) do
      order_number = parcel[:order_number]
      #            IO.inspect(map_size(memo), label: :not_seen)
      parcels = Map.get(memo, order_number, [])
      {[], Map.put(memo, order_number, parcels ++ [parcel])}
    end

    def force_order(event, memo) do
      order_number = event[:order_number]

      case event[:type] do
        "ORDER_CREATED" ->
          stored_parcels = Map.get(memo, order_number, [])
          {[event | stored_parcels], Map.delete(memo, order_number)}

        "PARCEL_SHIPPED" ->
          {[event], memo}
      end
    end

    def decide(event, agent) do
      order_number = event[:order_number]

      case event[:type] do
        "ORDER_CREATED" ->
          Agent.update(agent, fn memo ->
            Map.put(memo, order_number, {event[:to_ship], event[:occurred_at]})
          end)

          {[], agent}

        "PARCEL_SHIPPED" ->
          case Agent.get(agent, fn memo -> Map.get(memo, order_number) end) do
            # THRESHOLD_EXCEEDED was sent already
            nil ->
              {[], agent}

            {1, order_occurred_at} ->
              good_or_bad =
                if DateTime.diff(event[:occurred_at], order_occurred_at, :second) >
                     @seconds_in_week do
                  %{
                    type: "THRESHOLD_EXCEEDED",
                    order_number: order_number,
                    occurred_at: event[:occurred_at]
                  }
                else
                  %{
                    type: "ALL_PARCELS_SHIPPED",
                    order_number: order_number,
                    occurred_at: event[:occurred_at]
                  }
                end

              Agent.update(agent, fn memo -> Map.delete(memo, order_number) end)
              {[good_or_bad], agent}

            {amount, order_occurred_at} when amount > 1 ->
              if DateTime.diff(event[:occurred_at], order_occurred_at, :second) >
                   @seconds_in_week do
                bad = %{
                  type: "THRESHOLD_EXCEEDED",
                  order_number: order_number,
                  occurred_at: event[:occurred_at]
                }

                Agent.update(agent, fn memo -> Map.delete(memo, order_number) end)
                {[bad], agent}
              else
                Agent.update(agent, fn memo ->
                  Map.put(memo, order_number, {amount - 1, order_occurred_at})
                end)

                {[], agent}
              end
          end
      end
    end

    def to_string(event) do
      "#{event[:type]},#{event[:order_number]},#{event[:occurred_at]}"
    end

    @chunk 1000

    def components() do
      {:ok, agent} = Agent.start_link(fn -> %{} end)

      [
        source(:orders, ReadLines.new("test/examples/parcels/orders.csv"),
          chunk: @chunk,
          label: "read_orders"
        ),
        transform([:orders], &__MODULE__.build_order/1, nil, chunk: @chunk, label: "build_order"),
        source(:parcels, ReadLines.new("test/examples/parcels/parcels.csv"),
          chunk: @chunk,
          label: "read_parcels"
        ),
        transform([:parcels], &__MODULE__.build_parcel/1, nil,
          chunk: @chunk,
          label: "build_parcel"
        ),
        mix([:orders, :parcels], :mixed, chunk: @chunk, label: "mix orders and parcels"),
        transform([:mixed], &ParcelsFlow.order_seen/2, 0, chunk: @chunk, label: "order_seen"),
        transform([:mixed], &ParcelsFlow.force_order/2, %{}, chunk: @chunk, label: "force_order"),
        transform([:mixed], &ParcelsFlow.decide/2, agent, chunk: @chunk, label: "decide"),
        split(
          :mixed,
          %{
            threshold_exceeded: &(&1[:type] == "THRESHOLD_EXCEEDED"),
            all_parcels_shipped: &(&1[:type] == "ALL_PARCELS_SHIPPED")
          },
          chunk: @chunk
        ),
        transform([:threshold_exceeded, :all_parcels_shipped], &__MODULE__.to_string/1, nil,
          chunk: @chunk,
          label: "to_string"
        ),
        sink(:threshold_exceeded, WriteLines.new("test/examples/parcels/threshold_exceeded.csv"),
          label: "write threshold exceeded"
        ),
        sink(
          :all_parcels_shipped,
          WriteLines.new("test/examples/parcels/all_parcels_shipped.csv"),
          sync: true,
          label: "write all parcels shipped"
        )
      ]
    end
  end

  @orders_count 100

  @tag timeout: :infinity
  test "generate_data" do
    gen_data =
      GenData.components()
      |> Composite.new()
      |> Composite.start()

    stream =
      Stream.resource(
        fn -> 0 end,
        fn
          @orders_count ->
            {:halt, @orders_count}

          counter ->
            {[:tick], counter + 1}
        end,
        fn counter -> counter end
      )

    Composite.call(%{stream: stream}, gen_data)
    Composite.stop(gen_data)
  end

  @tag timeout: :infinity
  test "solve" do
    parcels_flow =
      ParcelsFlow.components()
      |> Composite.new()
      |> Composite.start()

    Composite.call(%{}, parcels_flow)

    shipped_length =
      "test/examples/parcels/all_parcels_shipped.csv"
      |> File.read!()
      |> String.split("\n")
      |> length()
      |> then(&(&1 - 1))

    threshold_length =
      "test/examples/parcels/threshold_exceeded.csv"
      |> File.read!()
      |> String.split("\n")
      |> length()
      |> then(&(&1 - 1))

    assert shipped_length + threshold_length == @orders_count

    Composite.stop(parcels_flow)
  end
end
