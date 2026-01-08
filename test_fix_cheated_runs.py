#!/usr/bin/env python3
"""
Unit tests for fix_cheated_runs.py

Tests cover:
- Input parameter validation
- Checkpoint validation and reachability
- Connection checks
- Final checkpoint time retrieval
- Finding cheated runs (with finished_map filter)
- Transaction handling for updates
- CSV operations
- Revert functionality

Run with: python -m pytest test_fix_cheated_runs.py -v
or: python -m unittest test_fix_cheated_runs.py -v
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import sys
from fix_cheated_runs import CheatRunFixer, ValidationError


class TestCheatRunFixer(unittest.TestCase):
    """Test cases for CheatRunFixer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.fixer = CheatRunFixer(
            host='localhost',
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        # Mock the connection
        self.fixer.connection = MagicMock()

    def tearDown(self):
        """Clean up after tests."""
        self.fixer.connection = None

    # Test input validation (Safety check 2)

    def test_validate_input_parameters_valid(self):
        """Test that valid parameters pass validation."""
        # Should not raise exception
        self.fixer.validate_input_parameters(1, 2, 10.0)
        self.fixer.validate_input_parameters(100, 200, 0.5)
        self.fixer.validate_input_parameters(1, 999, 3600)

    def test_validate_input_parameters_negative_from_cp(self):
        """Test that negative from_cp_id raises ValidationError."""
        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_input_parameters(-1, 2, 10.0)
        self.assertIn("from_cp_id must be positive", str(context.exception))

    def test_validate_input_parameters_zero_from_cp(self):
        """Test that zero from_cp_id raises ValidationError."""
        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_input_parameters(0, 2, 10.0)
        self.assertIn("from_cp_id must be positive", str(context.exception))

    def test_validate_input_parameters_negative_end_cp(self):
        """Test that negative to_cp_id raises ValidationError."""
        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_input_parameters(1, -2, 10.0)
        self.assertIn("to_cp_id must be positive", str(context.exception))

    def test_validate_input_parameters_same_checkpoints(self):
        """Test that same from and end checkpoint raises ValidationError."""
        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_input_parameters(5, 5, 10.0)
        self.assertIn("must be different", str(context.exception))

    def test_validate_input_parameters_too_small_ref_time(self):
        """Test that too small ref_time raises ValidationError."""
        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_input_parameters(1, 2, 0.01)
        self.assertIn("must be > 0.05 seconds", str(context.exception))

    def test_validate_input_parameters_too_large_ref_time(self):
        """Test that too large ref_time raises ValidationError."""
        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_input_parameters(1, 2, 5000)
        self.assertIn("must be <= 3600 seconds", str(context.exception))

    # Test checkpoint validation (Safety check 1)

    def test_validate_checkpoints_both_exist_same_map(self):
        """Test validation when both checkpoints exist on same map."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Mock checkpoint existence check
        mock_cursor.fetchall.side_effect = [
            [{'cp_id': 1, 'mapid': 10}, {'cp_id': 2, 'mapid': 10}],  # Both exist on map 10
            [{'child_cp_id': 2}],  # cp 1 connects to cp 2
        ]

        mapid, reachable = self.fixer.validate_checkpoints(1, 2)

        self.assertEqual(mapid, 10)
        self.assertTrue(reachable)

    def test_validate_checkpoints_from_missing(self):
        """Test validation when from checkpoint doesn't exist."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Only end checkpoint exists
        mock_cursor.fetchall.return_value = [{'cp_id': 2, 'mapid': 10}]

        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_checkpoints(1, 2)
        self.assertIn("from_cp_id 1", str(context.exception))

    def test_validate_checkpoints_end_missing(self):
        """Test validation when end checkpoint doesn't exist."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Only from checkpoint exists
        mock_cursor.fetchall.return_value = [{'cp_id': 1, 'mapid': 10}]

        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_checkpoints(1, 2)
        self.assertIn("to_cp_id 2", str(context.exception))

    def test_validate_checkpoints_different_maps(self):
        """Test validation when checkpoints are on different maps."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Checkpoints on different maps
        mock_cursor.fetchall.return_value = [
            {'cp_id': 1, 'mapid': 10},
            {'cp_id': 2, 'mapid': 20}
        ]

        with self.assertRaises(ValidationError) as context:
            self.fixer.validate_checkpoints(1, 2)
        self.assertIn("different maps", str(context.exception))

    def test_validate_checkpoints_not_reachable(self):
        """Test validation when 'to' checkpoint is not reachable from 'from' checkpoint."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Both exist on same map but no connection
        mock_cursor.fetchall.side_effect = [
            [{'cp_id': 1, 'mapid': 10}, {'cp_id': 2, 'mapid': 10}],
            [],  # No children for cp 1
        ]

        with patch('builtins.print') as mock_print:
            mapid, reachable = self.fixer.validate_checkpoints(1, 2)

            self.assertEqual(mapid, 10)
            self.assertFalse(reachable)
            # Check that warning was printed
            mock_print.assert_called()

    # Test time format conversion

    def test_ticks_to_time_format(self):
        """Test conversion from ticks to MM:SS.SS format."""
        self.assertEqual(self.fixer.ticks_to_time_format(0), "00:00.00")
        self.assertEqual(self.fixer.ticks_to_time_format(20), "00:01.00")
        self.assertEqual(self.fixer.ticks_to_time_format(1200), "01:00.00")
        self.assertEqual(self.fixer.ticks_to_time_format(1210), "01:00.50")
        self.assertEqual(self.fixer.ticks_to_time_format(3600 * 20), "60:00.00")

    # Test connection checks (Safety check 6)

    def test_check_connection_connected(self):
        """Test connection check when connected."""
        self.fixer.connection.is_connected.return_value = True
        self.assertTrue(self.fixer.check_connection())

    def test_check_connection_not_connected(self):
        """Test connection check when not connected."""
        self.fixer.connection.is_connected.return_value = False
        self.assertFalse(self.fixer.check_connection())

    def test_check_connection_no_connection(self):
        """Test connection check when connection is None."""
        self.fixer.connection = None
        self.assertFalse(self.fixer.check_connection())

    # Test get_checkpoint_times_for_run

    def test_get_checkpoint_times_for_run(self):
        """Test getting checkpoint times for a run."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {'cp_id': 1, 'time_played': 100},
            {'cp_id': 2, 'time_played': 200},
            {'cp_id': 3, 'time_played': 300}
        ]

        result = self.fixer.get_checkpoint_times_for_run(123)

        self.assertEqual(result, {1: 100, 2: 200, 3: 300})
        mock_cursor.execute.assert_called_once()

    # Test get_following_checkpoints

    def test_get_following_checkpoints(self):
        """Test getting following checkpoints using BFS."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Mock BFS traversal: cp 2 -> 3, 4; cp 3 -> 5; cp 4 -> []; cp 5 -> []
        # fetchall returns list of tuples: [(value,), (value,), ...]
        mock_cursor.fetchall.side_effect = [
            [(3,), (4,)],  # Children of 2
            [(5,)],        # Children of 3
            [],            # Children of 4
            []             # Children of 5
        ]

        result = self.fixer.get_following_checkpoints(2, 10)

        self.assertEqual(result, {2, 3, 4, 5})

    def test_get_following_checkpoints_no_children(self):
        """Test getting following checkpoints when there are no children."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = []

        result = self.fixer.get_following_checkpoints(2, 10)

        self.assertEqual(result, {2})  # Only the checkpoint itself

    # Test get_final_checkpoint_time

    def test_get_final_checkpoint_time_success(self):
        """Test getting final checkpoint time for a finished run."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Mock final checkpoint (isend=1) with time_played = 5000
        mock_cursor.fetchone.return_value = {'time_played': 5000}

        result = self.fixer.get_final_checkpoint_time(run_id=1, mapid=10)

        self.assertEqual(result, 5000)

    def test_get_final_checkpoint_time_not_found(self):
        """Test error when no final checkpoint exists."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # No final checkpoint found
        mock_cursor.fetchone.return_value = None

        with self.assertRaises(ValidationError) as context:
            self.fixer.get_final_checkpoint_time(run_id=1, mapid=10)
        self.assertIn("No final checkpoint", str(context.exception))

    # Test find_cheated_runs

    def test_find_cheated_runs(self):
        """Test finding cheated runs with finished_map filter."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        # Mock cheated run: took 5 seconds (100 ticks) but reference is 10 seconds
        # find_cheated_runs uses fetchall for main query (returns dicts)
        mock_cursor.fetchall.return_value = [{
            'run_id': 1,
            'start_time': 0,
            'end_time': 100,  # 5 seconds at end_cp
            'mapid': 10,
            'playername': 'TestPlayer',
            'player_id': 42,
            'fps': 125
        }]

        # Mock get_map_name (uses fetchone, returns tuple)
        # Mock get_final_checkpoint_time (uses fetchone, returns dict)
        mock_cursor.fetchone.side_effect = [
            ('TestMap',),           # get_map_name
            {'time_played': 1000}   # get_final_checkpoint_time (final time = 50 seconds)
        ]

        results = self.fixer.find_cheated_runs(1, 2, 10.0)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['run_id'], 1)
        self.assertEqual(results[0]['actual_time'], 5.0)
        self.assertEqual(results[0]['ref_time'], 10.0)
        self.assertEqual(results[0]['adjustment_ticks'], 100)  # Need to add 5 seconds
        self.assertEqual(results[0]['old_time_played'], 1000)  # Final checkpoint time
        self.assertEqual(results[0]['end_cp_time'], 100)  # Time at end_cp

    def test_find_cheated_runs_no_cheats(self):
        """Test finding cheated runs when there are none."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = []

        results = self.fixer.find_cheated_runs(1, 2, 10.0)

        self.assertEqual(len(results), 0)

    # Test fix_cheated_run transaction handling (Safety checks 3, 4)

    def test_fix_cheated_run_success(self):
        """Test successful fix of a cheated run with transaction."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor
        self.fixer.connection.is_connected.return_value = True

        # Mock pre-update checkpoint times
        mock_cursor.fetchall.side_effect = [
            [{'cp_id': 2, 'time_played': 200}, {'cp_id': 3, 'time_played': 300}],
            [{'cp_id': 2, 'time_played': 300}, {'cp_id': 3, 'time_played': 400}]
        ]

        mock_cursor.rowcount = 2

        result = self.fixer.fix_cheated_run(
            run_id=1,
            to_cp_id=2,
            mapid=10,
            adjustment_ticks=100,
            following_cps={2, 3}
        )

        self.assertTrue(result)
        self.fixer.connection.start_transaction.assert_called_once()
        self.fixer.connection.commit.assert_called_once()
        self.fixer.connection.rollback.assert_not_called()

    def test_fix_cheated_run_no_checkpoints(self):
        """Test fix with empty checkpoint set."""
        result = self.fixer.fix_cheated_run(
            run_id=1,
            to_cp_id=2,
            mapid=10,
            adjustment_ticks=100,
            following_cps=set()
        )

        self.assertFalse(result)

    def test_fix_cheated_run_connection_lost(self):
        """Test fix when database connection is lost."""
        self.fixer.connection.is_connected.return_value = False

        result = self.fixer.fix_cheated_run(
            run_id=1,
            to_cp_id=2,
            mapid=10,
            adjustment_ticks=100,
            following_cps={2, 3}
        )

        self.assertFalse(result)

    def test_fix_cheated_run_no_rows_affected(self):
        """Test fix when UPDATE affects zero rows."""
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor
        self.fixer.connection.is_connected.return_value = True

        mock_cursor.fetchall.return_value = [
            {'cp_id': 2, 'time_played': 200},
            {'cp_id': 3, 'time_played': 300}
        ]
        mock_cursor.rowcount = 0  # No rows updated

        result = self.fixer.fix_cheated_run(
            run_id=1,
            to_cp_id=2,
            mapid=10,
            adjustment_ticks=100,
            following_cps={2, 3}
        )

        self.assertFalse(result)
        self.fixer.connection.rollback.assert_called_once()

    # Test CSV operations

    def test_save_to_csv(self):
        """Test saving data to CSV file."""
        runs_data = [{
            'run_id': 1,
            'player_id': 42,
            'playername': 'TestPlayer',
            'mapid': 10,
            'map_name': 'TestMap',
            'fps': 125,
            'from_cp_id': 100,
            'to_cp_id': 105,
            'old_time_played': 200,
            'old_time_formatted': '00:10.00',
            'new_time_played': 300,
            'new_time_formatted': '00:15.00',
            'adjustment_seconds': 5.0
        }]

        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            with patch('builtins.print'):
                self.fixer.save_to_csv('test.csv', runs_data)
                mock_file.assert_called_once_with('test.csv', 'w', newline='')


class TestValidationError(unittest.TestCase):
    """Test ValidationError exception."""

    def test_validation_error_message(self):
        """Test that ValidationError can be raised with a message."""
        with self.assertRaises(ValidationError) as context:
            raise ValidationError("Test error message")
        self.assertEqual(str(context.exception), "Test error message")


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for common scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.fixer = CheatRunFixer(
            host='localhost',
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        self.fixer.connection = MagicMock()

    def test_full_validation_flow_success(self):
        """Test complete validation flow with valid data."""
        # Input validation
        self.fixer.validate_input_parameters(1, 2, 10.0)

        # Checkpoint validation
        mock_cursor = MagicMock()
        self.fixer.connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = [
            [{'cp_id': 1, 'mapid': 10}, {'cp_id': 2, 'mapid': 10}],
            [{'child_cp_id': 2}]
        ]

        mapid, reachable = self.fixer.validate_checkpoints(1, 2)
        self.assertEqual(mapid, 10)
        self.assertTrue(reachable)

    def test_full_validation_flow_failure(self):
        """Test complete validation flow with invalid data."""
        # Input validation should fail
        with self.assertRaises(ValidationError):
            self.fixer.validate_input_parameters(1, 1, 10.0)


if __name__ == '__main__':
    unittest.main()
