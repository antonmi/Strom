defmodule Strom.Examples.ParcelsDataTest do
  use ExUnit.Case

  defmodule GenData do
    use Strom.DSL

    defmodule BuildEvent do
      def start(_opts) do
        %{
          occurred_at: DateTime.add(DateTime.now!("Etc/UTC"), -(3600 * 24 * 30), :second),
          order_number: 0
        }
      end

      def stop(_opts, _acc), do: :ok

      def call(:tick, last_order, _opts) do
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
            occurred_at = DateTime.add(occurred_at, :rand.uniform(2) * 24 * 3600, :second)

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

    def topology(_) do
      partitions = %{
        orders: &(&1[:type] == "ORDER_CREATED"),
        parcels: &(&1[:type] == "PARCEL_SHIPPED")
      }

      [
        module(:stream, BuildEvent),
        splitter(:stream, partitions),
        function(:orders, &__MODULE__.order_to_string/1),
        function(:parcels, &__MODULE__.parcel_to_string/1),
        sink(:orders, %Strom.Sink.WriteLines{path: "test_data/orders.csv"}),
        sink(:parcels, %Strom.Sink.WriteLines{path: "test_data/parcels.csv"}, true)
      ]
    end
  end

  test "test" do
    #    GenData.start()
    #    GenData.call(%{stream: Stream.cycle([:tick])})
  end
end
