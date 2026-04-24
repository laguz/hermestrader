import unittest
from unittest.mock import MagicMock, patch, call
from bot.trade_manager import TradeManager, TradeAction

class TestTradeManager(unittest.TestCase):
    def setUp(self):
        self.tradier_mock = MagicMock()
        self.db_mock = MagicMock()
        self.trade_manager = TradeManager(self.tradier_mock, self.db_mock)

    def test_init(self):
        self.assertEqual(self.trade_manager.tradier, self.tradier_mock)
        self.assertEqual(self.trade_manager.db, self.db_mock)
        self.assertIsNotNone(self.trade_manager.lock)

    def test_register_strategy_db_none(self):
        self.trade_manager.db = None
        # Should return immediately, not throw
        self.trade_manager.register_strategy("test_strat")

    def test_register_strategy_success(self):
        strategy_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = strategy_col_mock

        self.trade_manager.register_strategy("test_strat")

        self.db_mock.__getitem__.assert_called_with('strategies')
        strategy_col_mock.update_one.assert_called_once()
        args, kwargs = strategy_col_mock.update_one.call_args
        self.assertEqual(args[0], {"_id": "test_strat"})
        self.assertEqual(kwargs['upsert'], True)
        self.assertIn("$setOnInsert", args[1])
        self.assertEqual(args[1]["$setOnInsert"]["strategy_id"], "test_strat")

    @patch('bot.trade_manager.logger.error')
    def test_register_strategy_exception(self, mock_logger_error):
        strategy_col_mock = MagicMock()
        strategy_col_mock.update_one.side_effect = Exception("Test Exception")
        self.db_mock.__getitem__.return_value = strategy_col_mock

        self.trade_manager.register_strategy("test_strat")
        mock_logger_error.assert_called_once()
        self.assertIn("Error registering strategy test_strat", mock_logger_error.call_args[0][0])

    def test_get_my_trades_db_none(self):
        self.trade_manager.db = None
        self.assertEqual(self.trade_manager.get_my_trades("test_strat"), [])

    def test_get_my_trades_success(self):
        active_trades_col_mock = MagicMock()
        active_trades_col_mock.find.return_value = [{"_id": 1}, {"_id": 2}]
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        result = self.trade_manager.get_my_trades("test_strat", status="CLOSED")

        self.assertEqual(result, [{"_id": 1}, {"_id": 2}])
        self.db_mock.__getitem__.assert_called_with('active_trades')
        active_trades_col_mock.find.assert_called_once_with({"strategy": "test_strat", "status": "CLOSED"})

    @patch('bot.trade_manager.logger.error')
    def test_get_my_trades_exception(self, mock_logger_error):
        active_trades_col_mock = MagicMock()
        active_trades_col_mock.find.side_effect = Exception("Test Exception")
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        result = self.trade_manager.get_my_trades("test_strat")
        self.assertEqual(result, [])
        mock_logger_error.assert_called_once()

    def test_get_tracked_symbols_db_none(self):
        self.trade_manager.db = None
        self.assertEqual(self.trade_manager._get_tracked_symbols(), set())

    def test_get_tracked_symbols_success(self):
        active_trades_col_mock = MagicMock()
        auto_trades_col_mock = MagicMock()

        def mock_getitem(key):
            if key == 'active_trades': return active_trades_col_mock
            if key == 'auto_trades': return auto_trades_col_mock
            return MagicMock()

        self.db_mock.__getitem__.side_effect = mock_getitem

        active_trades_col_mock.find.return_value = [
            {"short_leg": "AAPL210101C100"},
            {"long_leg": "AAPL210101P100"},
            {"legs_info": [{"option_symbol": "TSLA210101C100"}]},
            {"strategy": "wheel", "symbol": "MSFT"},
            {"strategy": "wheel", "symbol": "SPY210101C100"} # Should not be added as base symbol because it has digits
        ]

        auto_trades_col_mock.find.return_value = [
            {"short_leg": "AMZN210101C100"}
        ]

        symbols = self.trade_manager._get_tracked_symbols()

        self.assertIn("AAPL210101C100", symbols)
        self.assertIn("AAPL210101P100", symbols)
        self.assertIn("TSLA210101C100", symbols)
        self.assertIn("MSFT", symbols)
        self.assertIn("AMZN210101C100", symbols)
        self.assertNotIn("SPY210101C100", symbols)


    def test_get_tracked_symbols_legs_info_not_list(self):
        active_trades_col_mock = MagicMock()
        auto_trades_col_mock = MagicMock()

        def mock_getitem(key):
            if key == 'active_trades': return active_trades_col_mock
            if key == 'auto_trades': return auto_trades_col_mock
            return MagicMock()

        self.db_mock.__getitem__.side_effect = mock_getitem

        active_trades_col_mock.find.return_value = [
            {"legs_info": {"option_symbol": "TSLA210101C100"}} # Not a list, should be skipped safely
        ]
        auto_trades_col_mock.find.return_value = []

        symbols = self.trade_manager._get_tracked_symbols()
        self.assertNotIn("TSLA210101C100", symbols)
        self.assertEqual(len(symbols), 0)

    def test_get_tracked_symbols_missing_strategy(self):
        active_trades_col_mock = MagicMock()
        auto_trades_col_mock = MagicMock()

        def mock_getitem(key):
            if key == 'active_trades': return active_trades_col_mock
            if key == 'auto_trades': return auto_trades_col_mock
            return MagicMock()

        self.db_mock.__getitem__.side_effect = mock_getitem

        active_trades_col_mock.find.return_value = [
            {"status": "OPEN"} # Missing 'strategy', should not raise KeyError
        ]
        auto_trades_col_mock.find.return_value = []

        symbols = self.trade_manager._get_tracked_symbols()
        self.assertEqual(len(symbols), 0)

    def test_get_unmanaged_orphans_empty(self):
        self.tradier_mock.get_positions.return_value = []
        self.assertEqual(self.trade_manager.get_unmanaged_orphans(), [])

    def test_get_unmanaged_orphans_with_orphans(self):
        self.tradier_mock.get_positions.return_value = [
            {'symbol': 'AAPL', 'quantity': 100, 'cost_basis': 150.0, 'date_acquired': '2021-01-01'},
            {'symbol': 'TSLA', 'quantity': 50, 'cost_basis': 200.0, 'date_acquired': '2021-01-02'}
        ]

        with patch.object(self.trade_manager, '_get_tracked_symbols', return_value={'AAPL'}):
            orphans = self.trade_manager.get_unmanaged_orphans()

            self.assertEqual(len(orphans), 1)
            self.assertEqual(orphans[0]['symbol'], 'TSLA')
            self.assertEqual(orphans[0]['quantity'], 50)


    def test_get_unmanaged_orphans_no_symbol_key(self):
        self.tradier_mock.get_positions.return_value = [
            {'quantity': 100, 'cost_basis': 150.0, 'date_acquired': '2021-01-01'} # Missing 'symbol'
        ]

        with patch.object(self.trade_manager, '_get_tracked_symbols', return_value=set()):
            orphans = self.trade_manager.get_unmanaged_orphans()

            # Since there is no 'symbol' key, it evaluates to None and is ignored
            self.assertEqual(len(orphans), 0)

    @patch('bot.trade_manager.logger.error')
    def test_get_unmanaged_orphans_exception(self, mock_logger_error):
        self.tradier_mock.get_positions.side_effect = Exception("Test Exception")
        self.assertEqual(self.trade_manager.get_unmanaged_orphans(), [])
        mock_logger_error.assert_called_once()

    def test_execute_strategy_order_success(self):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"id": "ORDER123", "status": "ok"}

        active_trades_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=150.0,
            side="buy",
            quantity=100,
            strategy_params={"short_leg": "leg1", "long_leg": "leg2"}
        )
        response = self.trade_manager.execute_strategy_order(action)

        self.assertEqual(response, {"id": "ORDER123", "status": "ok"})
        self.tradier_mock.place_order.assert_called_once_with(
            account_id="ACC123",
            symbol="AAPL",
            side="buy",
            quantity=100,
            order_type="credit",
            duration="day",
            price=150.0,
            order_class="equity",
            legs=[],
            tag="test_strat"
        )

        active_trades_col_mock.insert_one.assert_called_once()
        insert_args = active_trades_col_mock.insert_one.call_args[0][0]
        self.assertEqual(insert_args["symbol"], "AAPL")
        self.assertEqual(insert_args["strategy"], "test_strat")
        self.assertEqual(insert_args["order_id"], "ORDER123")
        self.assertEqual(insert_args["short_leg"], "leg1")
        self.assertEqual(insert_args["long_leg"], "leg2")


    def test_execute_strategy_order_market_order(self):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"id": "ORDER123", "status": "ok"}

        active_trades_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=None,
            side="buy",
            quantity=100
        )
        response = self.trade_manager.execute_strategy_order(action)

        self.assertEqual(response, {"id": "ORDER123", "status": "ok"})
        self.tradier_mock.place_order.assert_called_once_with(
            account_id="ACC123",
            symbol="AAPL",
            side="buy",
            quantity=100,
            order_type="market",
            duration="day",
            price=None,
            order_class="equity",
            legs=[],
            tag="test_strat"
        )
        active_trades_col_mock.insert_one.assert_called_once()

    def test_execute_strategy_order_debit_order(self):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"id": "ORDER123", "status": "ok"}

        active_trades_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=-1.0,
            side="buy",
            quantity=100
        )
        response = self.trade_manager.execute_strategy_order(action)

        self.assertEqual(response, {"id": "ORDER123", "status": "ok"})
        self.tradier_mock.place_order.assert_called_once_with(
            account_id="ACC123",
            symbol="AAPL",
            side="buy",
            quantity=100,
            order_type="debit",
            duration="day",
            price=-1.0,
            order_class="equity",
            legs=[],
            tag="test_strat"
        )
        active_trades_col_mock.insert_one.assert_called_once()

    def test_execute_strategy_order_custom_tag(self):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"id": "ORDER123", "status": "ok"}

        active_trades_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=150.0,
            side="buy",
            quantity=100,
            tag="custom_tag"
        )
        response = self.trade_manager.execute_strategy_order(action)

        self.assertEqual(response, {"id": "ORDER123", "status": "ok"})
        self.tradier_mock.place_order.assert_called_once_with(
            account_id="ACC123",
            symbol="AAPL",
            side="buy",
            quantity=100,
            order_type="credit",
            duration="day",
            price=150.0,
            order_class="equity",
            legs=[],
            tag="custom_tag"
        )
        active_trades_col_mock.insert_one.assert_called_once()

    def test_execute_strategy_order_db_none(self):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"id": "ORDER123", "status": "ok"}

        self.trade_manager.db = None

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=150.0,
            side="buy",
            quantity=100
        )
        response = self.trade_manager.execute_strategy_order(action)

        self.assertEqual(response, {"id": "ORDER123", "status": "ok"})
        self.tradier_mock.place_order.assert_called_once()
        self.db_mock.__getitem__.assert_not_called()

    def test_execute_strategy_order_error_response(self):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"error": "Invalid order"}

        active_trades_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=150.0,
            side="buy",
            quantity=100
        )
        response = self.trade_manager.execute_strategy_order(action)

        self.assertEqual(response, {"error": "Invalid order"})
        active_trades_col_mock.insert_one.assert_not_called()

    @patch('bot.trade_manager.logger.error')
    def test_execute_strategy_order_db_exception(self, mock_logger_error):
        self.tradier_mock.account_id = "ACC123"
        self.tradier_mock.place_order.return_value = {"id": "ORDER123", "status": "ok"}

        active_trades_col_mock = MagicMock()
        active_trades_col_mock.insert_one.side_effect = Exception("Test Exception")
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        action = TradeAction(
            strategy_id="test_strat",
            symbol="AAPL",
            order_class="equity",
            legs=[],
            price=150.0,
            side="buy",
            quantity=100
        )
        self.trade_manager.execute_strategy_order(action)

        mock_logger_error.assert_called_once()

    def test_reconcile_orphans_with_orphans(self):
        mock_log_func = MagicMock()
        with patch.object(self.trade_manager, 'get_unmanaged_orphans', return_value=[{'symbol': 'AAPL', 'quantity': 100}]):
            self.trade_manager.reconcile_orphans(log_func=mock_log_func)

            mock_log_func.assert_any_call("🔄 Reconciling active portfolio positions against strategy DB...")
            mock_log_func.assert_any_call("⚠️ Untracked position detected: AAPL (qty: 100)")
            mock_log_func.assert_any_call("Found 1 untracked position(s). View them in the Orphans panel.")

    def test_reconcile_orphans_no_orphans(self):
        mock_log_func = MagicMock()
        with patch.object(self.trade_manager, 'get_unmanaged_orphans', return_value=[]):
            self.trade_manager.reconcile_orphans(log_func=mock_log_func)

            mock_log_func.assert_any_call("✅ All positions are tracked by strategies.")

    def test_reconcile_orphans_exception(self):
        mock_log_func = MagicMock()
        with patch.object(self.trade_manager, 'get_unmanaged_orphans', side_effect=Exception("Test Exception")):
            self.trade_manager.reconcile_orphans(log_func=mock_log_func)

            mock_log_func.assert_any_call("Error during Orphan Reconciliation: Test Exception")

    def test_mark_trade_closed_db_none(self):
        self.trade_manager.db = None
        self.trade_manager.mark_trade_closed("TRADE123")

    def test_mark_trade_closed_success(self):
        active_trades_col_mock = MagicMock()
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        self.trade_manager.mark_trade_closed("TRADE123", limit_price=10.0, response_id="RESP123")

        self.db_mock.__getitem__.assert_called_with('active_trades')
        active_trades_col_mock.update_one.assert_called_once()
        args, kwargs = active_trades_col_mock.update_one.call_args
        self.assertEqual(args[0], {"_id": "TRADE123"})
        self.assertEqual(args[1]["$set"]["status"], "CLOSED")
        self.assertEqual(args[1]["$set"]["exit_price"], 10.0)
        self.assertEqual(args[1]["$set"]["close_order_id"], "RESP123")
        self.assertIn("close_date", args[1]["$set"])

    @patch('bot.trade_manager.logger.error')
    def test_mark_trade_closed_exception(self, mock_logger_error):
        active_trades_col_mock = MagicMock()
        active_trades_col_mock.update_one.side_effect = Exception("Test Exception")
        self.db_mock.__getitem__.return_value = active_trades_col_mock

        self.trade_manager.mark_trade_closed("TRADE123")

        mock_logger_error.assert_called_once()

if __name__ == '__main__':
    unittest.main()
