import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.twofinance.twofinance_exchange import TwoFinanceExchange
from hummingbot.connector.exchange.twofinance.twofinance_matchengine_schemas import MatchEngineEvent
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


class TwoFinanceExchangeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.exchange = TwoFinanceExchange(
            twofinance_matchengine_bearer_token="token",
            twofinance_engine_id="engine-btc-usdt",
            twofinance_wallet_id=7,
            trading_pairs=["BTC-USDT"],
            trading_required=False,
            ack_timeout=0,
        )
        self.exchange._symbol_metadata["BTC-USDT"] = {"symbol_id": 1, "symbol": "BTC-USDT"}

    async def test_build_order_command_maps_hummingbot_order(self):
        self.exchange._symbol_metadata["BTC-USDT"]["exchange_symbol"] = "BTC/USDT"
        command = await self.exchange._build_order_command(
            order_id="HBOT-2F-1",
            trading_pair="BTC-USDT",
            amount=Decimal("0.25"),
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT_MAKER,
            price=Decimal("100"),
            time_in_force=None,
        )

        payload = command.to_payload()
        self.assertEqual(payload["side"], "SELL")
        self.assertEqual(payload["order_type"], "LIMIT")
        self.assertNotIn("time_in_force", payload)
        self.assertEqual(payload["symbol_id"], 1)
        self.assertEqual(payload["market"], "BTC-USDT")

    async def test_place_order_resolves_exchange_id_from_state_after_queue_ack(self):
        self.exchange._ack_timeout = 0.2
        self.exchange._matchengine_client.send_command = AsyncMock()
        self.exchange._matchengine_client.wait_for_ack = AsyncMock(
            return_value=SimpleNamespace(accepted=True, order_id=None, reason=None)
        )
        self.exchange._matchengine_client.wait_for_exchange_order_id = AsyncMock(return_value=None)
        self.exchange._api_get = AsyncMock(
            side_effect=[{"client_order_id": "HBOT-2F-POLL", "status": "OPEN"},
                         {"client_order_id": "HBOT-2F-POLL", "order_id": 73, "status": "OPEN"}]
        )

        exchange_order_id, _ = await self.exchange._place_order(
            "HBOT-2F-POLL", "BTC-USDT", Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("10")
        )

        self.assertEqual(exchange_order_id, "73")
        self.assertEqual(self.exchange._matchengine_client.orders_by_exchange_id["73"], "HBOT-2F-POLL")

    async def test_place_order_fails_closed_when_exchange_id_is_never_confirmed(self):
        self.exchange._matchengine_client.send_command = AsyncMock()
        self.exchange._matchengine_client.wait_for_ack = AsyncMock(
            return_value=SimpleNamespace(accepted=True, order_id=None, reason=None)
        )
        self.exchange._matchengine_client.wait_for_exchange_order_id = AsyncMock(return_value=None)
        self.exchange._api_get = AsyncMock(return_value={"status": "OPEN"})

        with self.assertRaisesRegex(IOError, "did not confirm its exchange order id"):
            await self.exchange._place_order(
                "HBOT-2F-NO-ID", "BTC-USDT", Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("10")
            )

    async def test_place_cancel_waits_for_and_propagates_rejection(self):
        self.exchange._ack_timeout = 0.1
        self.exchange._matchengine_client.send_command = AsyncMock()
        self.exchange._matchengine_client.wait_for_ack = AsyncMock(
            return_value=SimpleNamespace(accepted=False, reason="ORDER_NOT_FOUND")
        )
        tracked_order = InFlightOrder(
            client_order_id="HBOT-2F-CANCEL",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("10"),
            exchange_order_id="404",
            creation_timestamp=1,
        )

        self.assertFalse(await self.exchange._place_cancel("HBOT-2F-CANCEL-REQUEST", tracked_order))
        self.exchange._matchengine_client.wait_for_ack.assert_awaited_once_with(
            "HBOT-2F-CANCEL-REQUEST", 0.1
        )

    async def test_format_trading_rules(self):
        rules = await self.exchange._format_trading_rules(
            {
                "data": {
                    "trading_rules": {
                        "BTC-USDT": {
                            "symbol": "BTC-USDT",
                            "min_order_size": "0.001",
                            "tick_size": "0.01",
                            "step_size": "0.0001",
                            "min_notional": "10",
                        }
                    }
                }
            }
        )

        self.assertEqual(rules[0].trading_pair, "BTC-USDT")
        self.assertEqual(rules[0].min_order_size, Decimal("0.001"))
        self.assertEqual(rules[0].min_price_increment, Decimal("0.01"))
        self.assertEqual(rules[0].min_base_amount_increment, Decimal("0.0001"))
        self.assertEqual(rules[0].min_notional_size, Decimal("10"))

    async def test_consumes_canonical_market_directory(self):
        directory = {
            "schema": "2finance.market_directory.v1",
            "generated_at": "2026-09-06T12:00:00Z",
            "markets": [
                {
                    "schema": "2finance.market_definition.v1",
                    "market_id": "BTC/USDT",
                    "symbol": "BTC/USDT",
                    "engine_id": "engine-btc-directory",
                    "symbol_id": 9,
                    "route_epoch": 4,
                    "status": "active",
                    "tick_size": "0.01",
                    "step_size": "0.0001",
                    "limits": {"amount": {"min": "0.001"}, "notional": {"min": "10"}},
                    "endpoints": {"websocket": "wss://ws.test/engines/engine-btc-directory"},
                }
            ],
        }

        rules = await self.exchange._format_trading_rules(directory)
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(directory)
        command = await self.exchange._build_order_command(
            order_id="HBOT-2F-DIRECTORY",
            trading_pair="BTC-USDT",
            amount=Decimal("0.01"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
            time_in_force="GTC",
        )

        self.assertEqual(rules[0].min_price_increment, Decimal("0.01"))
        self.assertEqual(command.engine_id, "engine-btc-directory")
        self.assertEqual(command.symbol_id, 9)
        self.assertEqual(command.route_epoch, 4)
        self.assertEqual(self.exchange._engine_id, "engine-btc-directory")
        self.assertEqual(
            self.exchange._matchengine_client.ws_url,
            "wss://ws.test/engines/engine-btc-directory",
        )

    async def test_format_trading_rules_normalizes_exchange_pair(self):
        rules = await self.exchange._format_trading_rules(
            {
                "data": {
                    "trading_rules": [
                        {
                            "name": "BTC/USDT",
                            "min_order_size": "0.001",
                            "tick_size": "0.01",
                            "step_size": "0.0001",
                            "min_notional": "10",
                        }
                    ]
                }
            }
        )

        self.assertEqual(rules[0].trading_pair, "BTC-USDT")

    def test_initialize_trading_pair_symbols_normalizes_state_api_symbols(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            {
                "data": {
                    "symbols": [
                        {
                            "symbol_id": 1,
                            "name": "BTC/USDT",
                        }
                    ]
                }
            }
        )

        self.assertEqual(self.exchange._symbol_metadata["BTC-USDT"]["exchange_symbol"], "BTC/USDT")

    def test_order_update_from_event(self):
        event = MatchEngineEvent.from_payload(
            {
                "schema": "matchengine.event.v2",
                "version": 2,
                "sequence": 1,
                "event_id": "engine:1",
                "event_type": "ORDER_ACCEPTED",
                "symbol_id": 1,
                "market": "BTC-USDT",
                "payload": {"client_order_id": "HBOT-2F-1", "order_id": 99, "order_status": 1},
            }
        )

        update = self.exchange._order_update_from_event(event)

        self.assertEqual(update.client_order_id, "HBOT-2F-1")
        self.assertEqual(update.exchange_order_id, "99")
        self.assertEqual(update.new_state, OrderState.OPEN)

    def test_trade_update_from_event_uses_exchange_order_mapping(self):
        order = InFlightOrder(
            client_order_id="HBOT-2F-1",
            trading_pair="BTC-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
            exchange_order_id="99",
            creation_timestamp=1,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        self.exchange._matchengine_client.orders_by_exchange_id["99"] = "HBOT-2F-1"
        event = MatchEngineEvent.from_payload(
            {
                "schema": "matchengine.event.v2",
                "version": 2,
                "sequence": 2,
                "event_id": "engine:2",
                "event_type": "TRADE_EXECUTED",
                "symbol_id": 1,
                "market": "BTC-USDT",
                "payload": {
                    "trade_id": "t-1",
                    "taker_order_id": 99,
                    "price": "100",
                    "quantity": "0.5",
                    "fee_asset": "USDT",
                    "fee_amount": "0.01",
                },
            }
        )

        trade_update = self.exchange._trade_update_from_event(event)

        self.assertEqual(trade_update.client_order_id, "HBOT-2F-1")
        self.assertEqual(trade_update.fill_base_amount, Decimal("0.5"))
        self.assertEqual(trade_update.fill_quote_amount, Decimal("50.0"))
        self.assertEqual(trade_update.fee.flat_fees[0].token, "USDT")
        self.assertEqual(trade_update.fee.flat_fees[0].amount, Decimal("0.01"))

    def test_trade_update_from_canonical_v3_uses_gross_amounts_and_side_fee(self):
        order = InFlightOrder(
            client_order_id="HBOT-OCTO-1",
            trading_pair="OCTO-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("2"),
            price=Decimal("1.25"),
            exchange_order_id="77",
            creation_timestamp=1,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        self.exchange._matchengine_client.orders_by_exchange_id["77"] = "HBOT-OCTO-1"
        event = MatchEngineEvent.from_payload(
            {
                "schema": "matchengine.event.v3",
                "version": 3,
                "sequence": 3,
                "event_id": "spot-octo-usdc:3",
                "event_type": "TRADE_EXECUTED",
                "symbol_id": 8,
                "market": "OCTO/USDC",
                "payload": {
                    "trade_id": "trade-3",
                    "buyer_order_id": 77,
                    "maker_order_id": 77,
                    "taker_order_id": 88,
                    "price": "1.250000",
                    "gross_base_amount": "2.000000",
                    "gross_quote_amount": "2.500000",
                    "base_asset": "OCTO",
                    "quote_asset": "USDC",
                    "buyer_fee_asset": "OCTO",
                    "buyer_fee_amount": "0.024000",
                    "seller_fee_asset": "USDC",
                    "seller_fee_amount": "0.036000",
                },
            }
        )

        trade_update = self.exchange._trade_update_from_event(event)

        self.assertEqual(trade_update.fill_base_amount, Decimal("2.000000"))
        self.assertEqual(trade_update.fill_quote_amount, Decimal("2.500000"))
        self.assertEqual(trade_update.fee.flat_fees[0].token, "OCTO")
        self.assertEqual(trade_update.fee.flat_fees[0].amount, Decimal("0.024000"))

    def test_trade_update_from_historical_v2_sell_uses_seller_quote_fee(self):
        order = InFlightOrder(
            client_order_id="HBOT-OCTO-SELL-1",
            trading_pair="OCTO-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("2"),
            price=Decimal("1.25"),
            exchange_order_id="88",
            creation_timestamp=1,
        )
        self.exchange._order_tracker.start_tracking_order(order)
        self.exchange._matchengine_client.orders_by_exchange_id["88"] = "HBOT-OCTO-SELL-1"
        event = MatchEngineEvent.from_payload(
            {
                "schema": "matchengine.event.v2",
                "version": 2,
                "sequence": 4,
                "event_id": "spot-octo-usdc:4",
                "event_type": "TRADE_EXECUTED",
                "symbol_id": 8,
                "market": "OCTO/USDC",
                "payload": {
                    "trade_id": "trade-4",
                    "buyer_order_id": 77,
                    "seller_order_id": 88,
                    "maker_order_id": 77,
                    "taker_order_id": 88,
                    "price": "1.250000",
                    "gross_base_amount": "2.000000",
                    "gross_quote_amount": "2.500000",
                    "base_asset": "OCTO",
                    "quote_asset": "USDC",
                    "fee_asset": {"buyer_asset_id": 10, "seller_asset_id": 20},
                    "fee_amount": "0.060000",
                    "buyer_fee_amount": "0.024000",
                    "seller_fee_amount": "0.036000",
                },
            }
        )

        trade_update = self.exchange._trade_update_from_event(event)

        self.assertEqual(trade_update.exchange_order_id, "88")
        self.assertEqual(trade_update.fill_base_amount, Decimal("2.000000"))
        self.assertEqual(trade_update.fill_quote_amount, Decimal("2.500000"))
        self.assertEqual(trade_update.fee.flat_fees[0].token, "USDC")
        self.assertEqual(trade_update.fee.flat_fees[0].amount, Decimal("0.036000"))


if __name__ == "__main__":
    unittest.main()
