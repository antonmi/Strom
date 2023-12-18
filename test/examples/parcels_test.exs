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

    defmodule ForceOrder do
      def start(_opts), do: {%{}, 0, 0}
      def stop(_, _opts), do: :ok

      def call(event, memo, _) do
        {memo, last_order_number, last_parcel_number} = memo

        IO.inspect(length(Map.keys(memo)), label: "-----------------> memo length")
        order_number = event[:order_number]

        case event[:type] do
          "PARCEL_SHIPPED" ->
            IO.inspect({last_order_number, last_parcel_number}, label: "PARCEL_SHIPPED > ")

            case Map.get(memo, order_number) do
              nil ->
                memo = Map.put(memo, order_number, [event])
                {[], {memo, last_order_number, order_number}}

              parcels ->
                {parcels ++ [event],
                 {Map.put(memo, order_number, []), last_order_number, order_number}}
            end

          "ORDER_CREATED" ->
            IO.inspect({last_order_number, last_parcel_number}, label: "ORDER_CREATED > ")

            case Map.get(memo, order_number) do
              nil ->
                {[event], {Map.put(memo, order_number, []), order_number, last_parcel_number}}

              parcels ->
                {[event | parcels],
                 {Map.put(memo, order_number, []), order_number, last_parcel_number}}
            end
        end
      end
    end

    defmodule CheckExpired do
      @seconds_in_week 3600 * 24 * 7

      def start(_opts), do: []
      def stop(_, _opts), do: :ok

      def call(event, memo, _) do
        order_number = event[:order_number]

        case event[:type] do
          "ORDER_CREATED" ->
            {expired_events, still_valid} = check_expired(event, memo)

            memo = [{order_number, event[:occurred_at]} | still_valid]
            {expired_events ++ [event], memo}

          "PARCEL_SHIPPED" ->
            {expired_events, still_valid} = check_expired(event, memo)

            {expired_events ++ [event], still_valid}
        end
      end

      def check_expired(event, memo) do
        {expired, still_valid} =
          Enum.split_while(Enum.reverse(memo), fn {_, order_time} ->
            DateTime.diff(event[:occurred_at], order_time, :second) > @seconds_in_week
          end)

        expired_events =
          Enum.map(expired, fn {order_number, time} ->
            %{type: "THRESHOLD_EXCEEDED", order_number: order_number, occurred_at: time}
          end)

        {expired_events, still_valid}
      end
    end

    defmodule CheckCount do
      def start(_opts), do: %{}
      def stop(_, _opts), do: :ok

      def call(event, memo, _) do
        order_number = event[:order_number]

        case event[:type] do
          "ORDER_CREATED" ->
            # putting order time here, it's always less than parcels time
            memo = Map.put(memo, order_number, {event[:to_ship], event[:occurred_at]})
            {[], memo}

          "PARCEL_SHIPPED" ->
            case Map.get(memo, order_number) do
              # was deleted in THRESHOLD_EXCEEDED
              nil ->
                {[], memo}

              {1, last_occurred_at} ->
                last_occurred_at = latest_occurred_at(event[:occurred_at], last_occurred_at)

                ok_event = %{
                  type: "ALL_PARCELS_SHIPPED",
                  order_number: order_number,
                  occurred_at: last_occurred_at
                }

                memo = Map.put(memo, order_number, :all_parcels_shipped)
                {[ok_event], memo}

              {amount, last_occurred_at} when amount > 1 ->
                last_occurred_at = latest_occurred_at(event[:occurred_at], last_occurred_at)
                memo = Map.put(memo, order_number, {amount - 1, last_occurred_at})
                {[], memo}
            end

          "THRESHOLD_EXCEEDED" ->
            case Map.get(memo, order_number) do
              :all_parcels_shipped ->
                {[], Map.delete(memo, order_number)}

              _count ->
                {[event], Map.delete(memo, order_number)}
            end
        end
      end

      def latest_occurred_at(occurred_at, last_occurred_at) do
        case DateTime.compare(occurred_at, last_occurred_at) do
          :gt ->
            occurred_at

          _ ->
            last_occurred_at
        end
      end
    end

    def to_string(event) do
      "#{event[:type]},#{event[:order_number]},#{event[:occurred_at]}"
      |> IO.inspect()
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
        mixer([:orders, :parcels], :mixed, buffer: [100, 300]),
        #        mixer([:orders, :parcels], :mixed, buffer: &__MODULE__.buffer/1),
        module(:mixed, ForceOrder),
        module(:mixed, CheckExpired),
        module(:mixed, CheckCount),
        splitter(:mixed, partitions, buffer: 1000),
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
        occurred_at: ~U[2017-04-20 09:00:00.000Z]
      },
      %{
        order_number: 333,
        type: "THRESHOLD_EXCEEDED",
        occurred_at: ~U[2017-04-21 09:00:00.000Z]
      }
    ]
  end

  @tag timeout: 3000_000
  test "flow" do
    #    :observer.start()
    #    ParcelsFlow.start()
    #    ParcelsFlow.call(%{})

    #    assert Enum.sort(Enum.to_list(mixed)) == Enum.sort(expected_results())
  end
end
