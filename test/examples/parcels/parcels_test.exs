defmodule Strom.Examples.ParcelsTest do
  use ExUnit.Case

  defmodule ParcelsFlow do
    alias Strom.Source.ReadLines

    use Strom.DSL

    @seconds_in_week 3600 * 24 * 7

    def build_order(event) do
      list = String.split(event, ",")
      type = Enum.at(list, 0)
      {:ok, occurred_at, _} = DateTime.from_iso8601(Enum.at(list, 1))
      order_number = String.to_integer(Enum.at(list, 2))
      Process.sleep(1)

      %{
        type: type,
        occurred_at: occurred_at,
        order_number: order_number,
        to_ship: String.to_integer(Enum.at(list, 3))
      }
    end

    def build_parcel(event) do
      list = String.split(event, ",")
      type = Enum.at(list, 0)
      {:ok, occurred_at, _} = DateTime.from_iso8601(Enum.at(list, 1))
      order_number = String.to_integer(Enum.at(list, 2))

      %{type: type, occurred_at: occurred_at, order_number: order_number}
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

    def topology(_opts) do
      partitions = %{
        threshold_exceeded: &(&1[:type] == "THRESHOLD_EXCEEDED"),
        all_parcels_shipped: &(&1[:type] == "ALL_PARCELS_SHIPPED")
      }

      [
        source(:orders, %ReadLines{path: "test/examples/parcels/orders.csv"}),
        transform([:orders], &__MODULE__.build_order/1),
        source(:parcels, %ReadLines{path: "test/examples/parcels/parcels.csv"}),
        transform([:parcels], &__MODULE__.build_parcel/1),
        mix([:orders, :parcels], :mixed),
        transform([:mixed], &ParcelsFlow.force_order/2, %{}),
        transform([:mixed], &ParcelsFlow.decide/2, %{}),
        split(:mixed, partitions),
        transform([:threshold_exceeded, :all_parcels_shipped], &__MODULE__.to_string/1)
        #        sink(:threshold_exceeded, %WriteLines{path: "test_data/threshold_exceeded.csv"}),
        #        sink(:all_parcels_shipped, %WriteLines{path: "test_data/all_parcels_shipped.csv"}, true)
      ]
    end
  end

  test "flow" do
    #    :observer.start()
    ParcelsFlow.start()

    %{threshold_exceeded: threshold_exceeded, all_parcels_shipped: all_parcels_shipped} =
      ParcelsFlow.call(%{})

    assert length(Enum.to_list(threshold_exceeded)) == 2
    assert length(Enum.to_list(all_parcels_shipped)) == 1
  end
end
