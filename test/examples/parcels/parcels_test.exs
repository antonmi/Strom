defmodule Strom.Examples.Parcels.BuildPipeline do
  use ALF.DSL

  @components [
    stage(:build_event)
  ]

  def build_event(event, _) do
    list = String.split(event, ",")
    type = Enum.at(list, 0)
    {:ok, occurred_at, _} = DateTime.from_iso8601(Enum.at(list, 1))
    order_number = String.to_integer(Enum.at(list, 2))

    case type do
      "ORDER_CREATED" ->
        %{
          type: type,
          occurred_at: occurred_at,
          order_number: order_number,
          to_ship: String.to_integer(Enum.at(list, 3))
        }

      "PARCEL_SHIPPED" ->
        %{type: type, occurred_at: occurred_at, order_number: order_number}
    end
  end
end

defmodule Strom.Examples.Parcels.OrderingPipeline do
  use ALF.DSL

  @components [
    composer(:check_order, memo: MapSet.new()),
    composer(:wait, memo: %{})
  ]

  def check_order(event, order_numbers, _) do
    order_number = event[:order_number]

    case event[:type] do
      "ORDER_CREATED" ->
        {[event], MapSet.put(order_numbers, order_number)}

      "PARCEL_SHIPPED" ->
        if MapSet.member?(order_numbers, order_number) do
          {[event], order_numbers}
        else
          {[Map.put(event, :wait, order_number)], order_numbers}
        end
    end
  end

  def wait(event, waiting, _) do
    case event[:type] do
      "ORDER_CREATED" ->
        order_number = event[:order_number]
        {[event | Map.get(waiting, order_number, [])], Map.delete(waiting, order_number)}

      "PARCEL_SHIPPED" ->
        if event[:wait] do
          other_waiting = Map.get(waiting, event[:wait], [])
          {[], Map.put(waiting, event[:wait], [event | other_waiting])}
        else
          {[event], waiting}
        end
    end
  end
end

defmodule Strom.Examples.Parcels.Pipeline do
  use ALF.DSL

  @components [
    composer(:check_expired, memo: []),
    composer(:check_count, memo: %{})
  ]

  @seconds_in_week 3600 * 24 * 7

  def check_expired(event, memo, _) do
    order_number = event[:order_number]

    case event[:type] do
      "ORDER_CREATED" ->
        memo = [{order_number, event[:occurred_at]} | memo]
        {[event], memo}

      "PARCEL_SHIPPED" ->
        {expired, still_valid} =
          Enum.split_while(Enum.reverse(memo), fn {_, order_time} ->
            DateTime.diff(event[:occurred_at], order_time, :second) > @seconds_in_week
          end)

        expired_events =
          Enum.map(expired, fn {order_number, time} ->
            %{type: "THRESHOLD_EXCEEDED", order_number: order_number, occurred_at: time}
          end)

        {expired_events ++ [event], still_valid}
    end
  end

  def check_count(event, memo, _) do
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

defmodule Strom.Examples.Parcels.ParcelsTest do
  use ExUnit.Case, async: true

  alias Strom.Examples.Parcels.BuildPipeline
  alias Strom.Examples.Parcels.OrderingPipeline
  alias Strom.Examples.Parcels.Pipeline
  alias Strom.Source.ReadLines

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

  describe "with several pipelines" do
    defmodule SeveralPipelinesFlow do
      use Strom.DSL

      @topology [
        source(:parcels, %ReadLines{path: "test/examples/parcels/parcels.csv"}),
        source(:orders, %ReadLines{path: "test/examples/parcels/orders.csv"}),
        mixer([:orders, :parcels], :mixed),
        module(:mixed, BuildPipeline),
        module(:mixed, OrderingPipeline),
        module(:mixed, Pipeline)
      ]
    end

    test "with several pipelines" do
      SeveralPipelinesFlow.start()
      %{mixed: mixed} = SeveralPipelinesFlow.call(%{})

      assert Enum.sort(Enum.to_list(mixed)) == Enum.sort(expected_results())
    end
  end
end
