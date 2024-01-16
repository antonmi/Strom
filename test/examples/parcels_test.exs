defmodule Strom.Examples.ParcelsTest do
  use ExUnit.Case
  @moduletag timeout: :infinity

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
            occurred_at = DateTime.add(occurred_at, :rand.uniform(2 * 24 * 3600), :second)

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
        transform(:stream, &BuildEvent.call/2, acc),
        split(:stream, partitions),
        transform(:orders, &__MODULE__.order_to_string/1),
        transform(:parcels, &__MODULE__.parcel_to_string/1),
        sink(:orders, WriteLines.new("test/examples/parcels/orders.csv")),
        sink(:parcels, WriteLines.new("test/examples/parcels/parcels.csv"), true)
      ]
    end
  end

  defmodule ParcelsFlow do
    alias Strom.Source.ReadLines
    alias Strom.Sink.WriteLines

    import Strom.DSL

    @seconds_in_week 3600 * 24 * 7

    def build_order(event) do
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
      list = String.split(event, ",")
      {:ok, occurred_at, _} = DateTime.from_iso8601(Enum.at(list, 1))

      %{
        type: Enum.at(list, 0),
        occurred_at: occurred_at,
        order_number: String.to_integer(Enum.at(list, 2))
      }
    end

    def force_order(event, memo) do
      order_number = event[:order_number]

      case event[:type] do
        "PARCEL_SHIPPED" ->
          case Map.get(memo, order_number) do
            nil ->
              {[], Map.put(memo, order_number, [event])}

            true ->
              {[event], memo}

            parcels ->
              {[], Map.put(memo, order_number, parcels ++ [event])}
          end

        "ORDER_CREATED" ->
          case Map.get(memo, order_number) do
            nil ->
              {[event], Map.put(memo, order_number, true)}

            parcels ->
              {[event | parcels], Map.put(memo, order_number, true)}
          end
      end
    end

    def decide(event, memo) do
      order_number = event[:order_number]

      case event[:type] do
        "ORDER_CREATED" ->
          memo = Map.put(memo, order_number, {event[:to_ship], event[:occurred_at]})
          {[], memo}

        "PARCEL_SHIPPED" ->
          case Map.get(memo, order_number) do
            # THRESHOLD_EXCEEDED was sent already
            nil ->
              {[], memo}

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

              memo = Map.delete(memo, order_number)
              {[good_or_bad], memo}

            {amount, order_occurred_at} when amount > 1 ->
              if DateTime.diff(event[:occurred_at], order_occurred_at, :second) >
                   @seconds_in_week do
                bad = %{
                  type: "THRESHOLD_EXCEEDED",
                  order_number: order_number,
                  occurred_at: event[:occurred_at]
                }

                memo = Map.delete(memo, order_number)
                {[bad], memo}
              else
                memo = Map.put(memo, order_number, {amount - 1, order_occurred_at})
                {[], memo}
              end
          end
      end
    end

    def to_string(event) do
      "#{event[:type]},#{event[:order_number]},#{event[:occurred_at]}"
    end

    def components() do
      [
        source(:orders, ReadLines.new("test/examples/parcels/orders.csv")),
        transform([:orders], &__MODULE__.build_order/1),
        source(:parcels, ReadLines.new("test/examples/parcels/parcels.csv")),
        transform([:parcels], &__MODULE__.build_parcel/1),
        mix([:orders, :parcels], :mixed),
        transform([:mixed], &ParcelsFlow.force_order/2, %{}),
        transform([:mixed], &ParcelsFlow.decide/2, %{}),
        split(:mixed, %{
          threshold_exceeded: &(&1[:type] == "THRESHOLD_EXCEEDED"),
          all_parcels_shipped: &(&1[:type] == "ALL_PARCELS_SHIPPED")
        }),
        transform([:threshold_exceeded, :all_parcels_shipped], &__MODULE__.to_string/1),
        sink(:threshold_exceeded, WriteLines.new("test/examples/parcels/threshold_exceeded.csv")),
        sink(
          :all_parcels_shipped,
          WriteLines.new("test/examples/parcels/all_parcels_shipped.csv"),
          true
        )
      ]
    end
  end

  @parcels_count 100

  test "generate_data" do
    gen_data =
      GenData.components()
      |> Composite.new()
      |> Composite.start()

    Composite.call(%{stream: List.duplicate(:tick, @parcels_count)}, gen_data)
    Composite.stop(gen_data)
  end

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

    assert shipped_length + threshold_length == @parcels_count

    Composite.stop(parcels_flow)
  end
end
