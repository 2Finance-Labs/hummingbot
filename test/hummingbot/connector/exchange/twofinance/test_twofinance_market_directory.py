import unittest

from hummingbot.connector.exchange.twofinance.twofinance_market_directory import parse_market_directory


def market(engine_id="engine-btc", symbol="BTC/USDT", market_id="BTC/USDT"):
    return {
        "schema": "2finance.market_definition.v1",
        "market_id": market_id,
        "symbol": symbol,
        "engine_id": engine_id,
        "symbol_id": 1,
        "route_epoch": 7,
        "status": "active",
        "tick_size": "0.01",
        "step_size": "0.00000001",
        "limits": {
            "amount": {"min": "0.00001", "max": "100"},
            "notional": {"min": "5", "max": "1000000"},
        },
        "endpoints": {"websocket": "wss://ws.2finance.test/engines/engine-btc"},
    }


class TwoFinanceMarketDirectoryTests(unittest.TestCase):
    def test_parses_canonical_route_and_rules(self):
        definitions = parse_market_directory(
            {"schema": "2finance.market_directory.v1", "generated_at": "2026-09-06T12:00:00Z", "markets": [market()]}
        )

        definition = definitions[0]
        self.assertEqual(definition.engine_id, "engine-btc")
        self.assertEqual(definition.route_epoch, 7)
        self.assertEqual(str(definition.tick_size), "0.01")
        self.assertEqual(str(definition.min_notional), "5")

    def test_rejects_engine_with_more_than_one_symbol(self):
        with self.assertRaisesRegex(ValueError, "one engine controls multiple symbols"):
            parse_market_directory(
                {
                    "schema": "2finance.market_directory.v1",
                    "generated_at": "2026-09-06T12:00:00Z",
                    "markets": [market(), market(symbol="ETH/USDT", market_id="ETH/USDT")],
                }
            )


if __name__ == "__main__":
    unittest.main()
