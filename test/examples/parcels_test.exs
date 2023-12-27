defmodule Strom.Examples.ParcelsTest do
  use ExUnit.Case

  defmodule ParcelsFlow do
    alias Strom.Source.ReadLines
    alias Strom.Sink.WriteLines

    use Strom.DSL

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
              memo = Map.put(memo, order_number, [event])
              {[], memo}

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

    @seconds_in_week 3600 * 24 * 7

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

        :end ->
          IO.inspect(memo, limit: :infinity, label: ":end")
          {[], memo}
      end
    end

    def to_string(event) do
      "#{event[:type]},#{event[:order_number]},#{event[:occurred_at]}"
    end

    def buffer(event) do
      {event[:order_number], 1000}
    end

    def topology(_opts) do
      partitions = %{
        threshold_exceeded: &(&1[:type] == "THRESHOLD_EXCEEDED"),
        all_parcels_shipped: &(&1[:type] == "ALL_PARCELS_SHIPPED")
      }

      [
        source(:orders, %ReadLines{path: "test_data/orders.csv"}),
        function(:orders, &__MODULE__.build_order/1),
        source(:parcels, %ReadLines{path: "test_data/parcels.csv"}),
        function(:parcels, &__MODULE__.build_parcel/1),
        mixer([:orders, :parcels], :mixed),
        transform([:mixed], &ParcelsFlow.force_order/2, %{}),
        source(:mixed, [%{type: :end}]),
        transform([:mixed], &ParcelsFlow.decide/2, %{}),
        splitter(:mixed, partitions),
        function([:threshold_exceeded, :all_parcels_shipped], &__MODULE__.to_string/1),
        sink(:threshold_exceeded, %WriteLines{path: "test_data/threshold_exceeded.csv"}),
        sink(:all_parcels_shipped, %WriteLines{path: "test_data/all_parcels_shipped.csv"}, true)
      ]
    end
  end

  def expected_results do
    [
      %{
        order_number: 111,
        type: "ALL_PARCELS_SHIPPED",
        occurred_at: ~U[2017-04-21T08:00:00.000Z]
      },
      %{
        order_number: 222,
        type: "THRESHOLD_EXCEEDED",
        occurred_at: ~U[2017-04-30 08:00:00.000Z]
      },
      %{
        order_number: 333,
        type: "THRESHOLD_EXCEEDED",
        occurred_at: ~U[2017-05-01 08:00:00.000Z]
      }
    ]
  end

  @tag timeout: 3000_000
  test "flow" do
    #    :observer.start()
    ParcelsFlow.start()
    ParcelsFlow.call(%{})
    #
    #        assert Enum.sort(Enum.to_list(mixed)) == Enum.sort(expected_results())
  end
end
