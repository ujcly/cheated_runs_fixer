#!/usr/bin/env python3
"""
Script to fix cheated runs in checkpoint_statistics table.

A cheated run is one where a player completed a segment (from start_cp to end_cp)
faster than the ref_time, indicating they used a shortcut.

This script adjusts the time_played values for the end_cp and all following checkpoints
to add the missing time needed to reach the ref_time.
"""

import mysql.connector
from mysql.connector import Error
from typing import List, Set, Dict, Tuple, Optional
import sys
import csv
from datetime import datetime
from collections import defaultdict
from sshtunnel import SSHTunnelForwarder


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class CheatRunFixer:
    def __init__(self, host: str, database: str, user: str, password: str,
                 port: int = 3306, ssh_config: Optional[Dict] = None):
        """
        Initialize database connection parameters.

        Args:
            host: MySQL host (localhost if using SSH tunnel)
            database: Database name
            user: MySQL username
            password: MySQL password
            port: MySQL port (default 3306)
            ssh_config: Optional SSH tunnel configuration dict with keys:
                - ssh_host: SSH server hostname
                - ssh_port: SSH server port (default 22)
                - ssh_user: SSH username
                - ssh_key_file: Path to private SSH key file
                - ssh_key_password: Optional password for encrypted key file
                - local_bind_port: Local port for SSH tunnel (optional, auto-assigned if not specified)
                - remote_bind_address: MySQL host on remote server (default 'localhost')
                - remote_bind_port: MySQL port on remote server (default 3306)
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.ssh_config = ssh_config
        self.connection = None
        self.ssh_tunnel = None

    def connect(self):
        """Establish database connection, optionally through SSH tunnel."""
        try:
            # Set up SSH tunnel if configured
            if self.ssh_config:
                print(f"Setting up SSH tunnel to {self.ssh_config['ssh_host']}...")

                ssh_kwargs = {
                    'ssh_address_or_host': (
                        self.ssh_config['ssh_host'],
                        self.ssh_config.get('ssh_port', 22)
                    ),
                    'ssh_username': self.ssh_config['ssh_user'],
                    'ssh_pkey': self.ssh_config['ssh_key_file'],
                    'remote_bind_address': (
                        self.ssh_config.get('remote_bind_address', 'localhost'),
                        self.ssh_config.get('remote_bind_port', 3306)
                    )
                }

                # Add local bind port if specified
                if 'local_bind_port' in self.ssh_config:
                    ssh_kwargs['local_bind_address'] = ('127.0.0.1', self.ssh_config['local_bind_port'])

                # Add key password if provided
                if 'ssh_key_password' in self.ssh_config:
                    ssh_kwargs['ssh_private_key_password'] = self.ssh_config['ssh_key_password']

                self.ssh_tunnel = SSHTunnelForwarder(**ssh_kwargs)
                self.ssh_tunnel.start()

                print(f"✓ SSH tunnel established (local port: {self.ssh_tunnel.local_bind_port})")

                # Connect to MySQL through tunnel
                connect_host = '127.0.0.1'
                connect_port = self.ssh_tunnel.local_bind_port
            else:
                # Direct connection
                connect_host = self.host
                connect_port = self.port

            # Connect to MySQL
            self.connection = mysql.connector.connect(
                host=connect_host,
                port=connect_port,
                database=self.database,
                user=self.user,
                password=self.password
            )

            if self.connection.is_connected():
                print(f"✓ Successfully connected to MySQL database: {self.database}")
                return True

        except Exception as e:
            print(f"✗ Error connecting to MySQL: {e}")
            # Clean up tunnel if connection failed
            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.ssh_tunnel = None
            return False

    def close(self):
        """Close database connection and SSH tunnel."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")

        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            print("SSH tunnel closed")

    def check_connection(self) -> bool:
        """
        Check if database connection is alive.

        Returns:
            True if connection is alive, False otherwise
        """
        if not self.connection:
            return False
        try:
            return self.connection.is_connected()
        except Error:
            return False

    def check_update_privileges(self) -> bool:
        """
        Check if user has UPDATE privileges on checkpoint_statistics table.

        Returns:
            True if user has privileges, False otherwise
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT privilege_type
                FROM information_schema.user_privileges
                WHERE grantee LIKE %s
                    AND privilege_type IN ('UPDATE', 'ALL PRIVILEGES')
            UNION
            SELECT privilege_type
                FROM information_schema.schema_privileges
                WHERE table_schema = %s
                    AND grantee LIKE %s
                    AND privilege_type IN ('UPDATE', 'ALL PRIVILEGES')
            UNION
            SELECT privilege_type
                FROM information_schema.table_privileges
                WHERE table_schema = %s
                    AND table_name = 'checkpoint_statistics'
                    AND grantee LIKE %s
                    AND privilege_type IN ('UPDATE', 'ALL PRIVILEGES')
            """
            user_pattern = f"'{self.user}'%"
            cursor.execute(query, (user_pattern, self.database, user_pattern,
                                   self.database, user_pattern))
            results = cursor.fetchall()
            cursor.close()
            return len(results) > 0
        except Error as e:
            print(f"Warning: Could not verify UPDATE privileges: {e}")
            cursor.close()
            # Assume we have privileges if we can't check
            return True

    def validate_input_parameters(self, from_cp_id: int, to_cp_id: int,
                                   ref_time: float) -> None:
        """
        Validate input parameters.

        Args:
            from_cp_id: Starting checkpoint ID
            to_cp_id: Ending checkpoint ID
            ref_time: Reference time in seconds

        Raises:
            ValidationError: If any parameter is invalid
        """
        # Check that IDs are positive
        if from_cp_id <= 0:
            raise ValidationError(f"from_cp_id must be positive, got {from_cp_id}")
        if to_cp_id <= 0:
            raise ValidationError(f"to_cp_id must be positive, got {to_cp_id}")

        # Check that start and end are different
        if from_cp_id == to_cp_id:
            raise ValidationError("from_cp_id and to_cp_id must be different")

        # Check that ref_time is reasonable (between 0.05s and 1 hour)
        if ref_time <= 0.05:
            raise ValidationError(f"ref_time must be > 0.05 seconds, got {ref_time}")
        if ref_time > 3600:
            raise ValidationError(f"ref_time must be <= 3600 seconds (1 hour), got {ref_time}")

    def validate_checkpoints(self, from_cp_id: int, to_cp_id: int) -> Tuple[int, bool]:
        """
        Validate that checkpoints exist and are related.

        Args:
            from_cp_id: Starting checkpoint ID
            to_cp_id: Ending checkpoint ID

        Returns:
            Tuple of (mapid, is_reachable) where is_reachable indicates if end_cp
            is reachable from start_cp

        Raises:
            ValidationError: If checkpoints don't exist or are on different maps
        """
        cursor = self.connection.cursor(dictionary=True)

        # Check if both checkpoints exist
        query = "SELECT cp_id, mapid FROM checkpoints WHERE cp_id IN (%s, %s)"
        cursor.execute(query, (from_cp_id, to_cp_id))
        results = cursor.fetchall()

        if len(results) != 2:
            found_ids = [r['cp_id'] for r in results]
            missing = []
            if from_cp_id not in found_ids:
                missing.append(f"from_cp_id {from_cp_id}")
            if to_cp_id not in found_ids:
                missing.append(f"to_cp_id {to_cp_id}")
            cursor.close()
            raise ValidationError(f"Checkpoint(s) not found: {', '.join(missing)}")

        # Check if they are on the same map
        mapids = {r['cp_id']: r['mapid'] for r in results}
        start_mapid = mapids[from_cp_id]
        end_mapid = mapids[to_cp_id]

        if start_mapid != end_mapid:
            cursor.close()
            raise ValidationError(
                f"Checkpoints are on different maps: "
                f"start_cp {from_cp_id} on map {start_mapid}, "
                f"end_cp {to_cp_id} on map {end_mapid}"
            )

        # Check if end_cp is reachable from start_cp using BFS
        mapid = start_mapid
        visited = set()
        to_visit = [from_cp_id]
        reachable = False

        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            if current == to_cp_id:
                reachable = True
                break
            visited.add(current)

            query = """
                SELECT child_cp_id
                FROM checkpoint_connections
                WHERE cp_id = %s AND mapid = %s
            """
            cursor.execute(query, (current, mapid))
            children = cursor.fetchall()

            for row in children:
                child_id = row['child_cp_id']
                if child_id not in visited:
                    to_visit.append(child_id)

        cursor.close()

        if not reachable:
            print(f"Warning: end_cp {to_cp_id} is not reachable from start_cp {from_cp_id} "
                  f"via checkpoint_connections. This may indicate a data issue.")

        return mapid, reachable

    def get_following_checkpoints(self, to_cp_id: int, mapid: int) -> Set[int]:
        """
        Get all checkpoints that follow the to_cp_id using checkpoint_connections.
        Uses breadth-first search to traverse the checkpoint graph.

        Args:
            to_cp_id: The checkpoint ID to start from
            mapid: The map ID for filtering connections

        Returns:
            Set of checkpoint IDs that follow to_cp_id (including to_cp_id itself)
        """
        cursor = self.connection.cursor()
        following = {to_cp_id}  # Include the end_cp itself
        to_visit = [to_cp_id]
        visited = set()

        while to_visit:
            current_cp = to_visit.pop(0)
            if current_cp in visited:
                continue
            visited.add(current_cp)

            # Find all child checkpoints
            query = """
                SELECT child_cp_id
                FROM checkpoint_connections
                WHERE cp_id = %s AND mapid = %s
            """
            cursor.execute(query, (current_cp, mapid))
            children = cursor.fetchall()

            for (child_cp_id,) in children:
                if child_cp_id not in visited:
                    following.add(child_cp_id)
                    to_visit.append(child_cp_id)

        cursor.close()
        return following

    def ticks_to_time_format(self, ticks: int) -> str:
        """
        Convert ticks to MM:SS.SS format.

        Args:
            ticks: Time in ticks (20 ticks = 1 second)

        Returns:
            Formatted time string (MM:SS.SS)
        """
        total_seconds = ticks / 20
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes:02d}:{seconds:05.2f}"

    def get_map_name(self, mapid: int) -> str:
        """
        Get map name from mapids table.

        Args:
            mapid: The map ID

        Returns:
            Map name or 'Unknown' if not found
        """
        cursor = self.connection.cursor()
        try:
            query = "SELECT mapname FROM mapids WHERE mapid = %s"
            cursor.execute(query, (mapid,))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else 'Unknown'
        except Error:
            cursor.close()
            return 'Unknown'

    def get_final_checkpoint_time(self, run_id: int, mapid: int) -> int:
        """
        Get the time_played for the final checkpoint (isend=1) in a run.

        Args:
            run_id: The run ID
            mapid: The map ID

        Returns:
            time_played value for final checkpoint

        Raises:
            ValidationError: If no final checkpoint found (data integrity issue)
        """
        cursor = self.connection.cursor(dictionary=True)
        try:
            query = """
                SELECT cs.time_played
                FROM checkpoint_statistics cs
                JOIN checkpoints c ON cs.cp_id = c.cp_id
                WHERE cs.run_id = %s
                    AND c.mapid = %s
                    AND c.isend = 1
                ORDER BY cs.time_played DESC
                LIMIT 1
            """
            cursor.execute(query, (run_id, mapid))
            result = cursor.fetchone()
            cursor.close()

            if not result:
                raise ValidationError(
                    f"No final checkpoint (isend=1) found for run_id {run_id}, "
                    f"mapid {mapid}. Data integrity issue!"
                )

            return result['time_played']

        except Error as e:
            cursor.close()
            raise ValidationError(f"Database error getting final checkpoint: {e}")

    def find_cheated_runs(self, from_cp_id: int, to_cp_id: int, ref_time: float) -> List[Dict]:
        """
        Find all runs where the time from start_cp to end_cp is less than ref_time.

        Args:
            from_cp_id: Starting checkpoint ID
            to_cp_id: Ending checkpoint ID
            ref_time: Reference time in seconds (legal minimum time)

        Returns:
            List of dicts with run details
        """
        cursor = self.connection.cursor(dictionary=True)

        ref_time_ticks = ref_time * 20  # Convert seconds to ticks

        # Query to find only cheated runs (time difference < ref_time)
        # Only consider finished runs (finished_map = 1)
        query = """
            SELECT
                start.run_id,
                start.time_played as start_time,
                end.time_played as end_time,
                pr.mapid,
                pr.playername,
                pr.player_id,
                pr.fps
            FROM checkpoint_statistics start
            JOIN checkpoint_statistics end ON start.run_id = end.run_id
            JOIN player_runs pr ON start.run_id = pr.run_id
            WHERE start.cp_id = %s
                AND end.cp_id = %s
                AND end.time_played > start.time_played
                AND (end.time_played - start.time_played) < %s
                AND pr.finished_map = 1
        """

        cursor.execute(query, (from_cp_id, to_cp_id, ref_time_ticks))
        results = cursor.fetchall()

        cheated_runs = []

        for row in results:
            time_diff_ticks = row['end_time'] - row['start_time']
            time_diff_seconds = time_diff_ticks / 20
            adjustment_ticks = int(ref_time_ticks - time_diff_ticks)

            # Get map name
            map_name = self.get_map_name(row['mapid'])

            # Get final checkpoint time (total run time)
            try:
                final_time = self.get_final_checkpoint_time(row['run_id'], row['mapid'])
            except ValidationError as e:
                print(f"Warning: Skipping run_id {row['run_id']}: {e}")
                continue

            cheated_runs.append({
                'run_id': row['run_id'],
                'player_id': row['player_id'],
                'playername': row['playername'],
                'mapid': row['mapid'],
                'map_name': map_name,
                'fps': row['fps'],
                'end_cp_time': row['end_time'],  # Time at end_cp (for reference)
                'old_time_played': final_time,  # Total run time (final checkpoint)
                'old_time_formatted': self.ticks_to_time_format(final_time),
                'actual_time': time_diff_seconds,
                'ref_time': ref_time,
                'adjustment_ticks': adjustment_ticks,
                'adjustment_seconds': adjustment_ticks / 20
            })

        cursor.close()

        # Sort by fps ascending, then by old_time_played (final time)
        cheated_runs.sort(key=lambda x: (x['fps'], x['old_time_played']))

        return cheated_runs

    def get_checkpoint_times_for_run(self, run_id: int) -> Dict[int, int]:
        """
        Get all checkpoint times for a specific run.

        Args:
            run_id: The run ID

        Returns:
            Dictionary mapping cp_id to time_played
        """
        cursor = self.connection.cursor(dictionary=True)
        query = """
            SELECT cp_id, time_played
            FROM checkpoint_statistics
            WHERE run_id = %s
            ORDER BY time_played
        """
        cursor.execute(query, (run_id,))
        results = cursor.fetchall()
        cursor.close()

        return {row['cp_id']: row['time_played'] for row in results}

    def fix_cheated_run(self, run_id: int, to_cp_id: int, mapid: int,
                        adjustment_ticks: int, following_cps: Set[int]) -> bool:
        """
        Adjust time_played for end_cp and all following checkpoints in a specific run.
        Uses explicit transaction with pre-update verification and post-update validation.

        Args:
            run_id: The run ID to fix
            to_cp_id: The ending checkpoint ID
            mapid: The map ID
            adjustment_ticks: Amount to add to time_played (in ticks)
            following_cps: Set of checkpoint IDs to update

        Returns:
            True if successful, False otherwise
        """
        if not following_cps:
            print(f"Error: No checkpoints to update for run {run_id}")
            return False

        # Check connection before proceeding
        if not self.check_connection():
            print(f"Error: Database connection lost for run {run_id}")
            return False

        cursor = self.connection.cursor(dictionary=True)

        try:
            # Start explicit transaction
            self.connection.start_transaction(isolation_level='READ COMMITTED')

            # Pre-update verification: Get current times
            current_times = self.get_checkpoint_times_for_run(run_id)

            if not current_times:
                print(f"Error: No checkpoint data found for run {run_id}")
                self.connection.rollback()
                cursor.close()
                return False

            # Perform the update
            placeholders = ','.join(['%s'] * len(following_cps))
            query = f"""
                UPDATE checkpoint_statistics
                SET time_played = time_played + %s
                WHERE run_id = %s
                    AND cp_id IN ({placeholders})
            """

            params = [adjustment_ticks, run_id] + list(following_cps)
            cursor.execute(query, params)

            rows_affected = cursor.rowcount

            if rows_affected == 0:
                print(f"Warning: No rows updated for run {run_id}")
                self.connection.rollback()
                cursor.close()
                return False

            # Post-update verification: Verify the changes were applied correctly
            query = f"""
                SELECT cp_id, time_played
                FROM checkpoint_statistics
                WHERE run_id = %s AND cp_id IN ({placeholders})
            """
            params = [run_id] + list(following_cps)
            cursor.execute(query, params)
            updated_rows = cursor.fetchall()

            # Verify each checkpoint was updated correctly
            for row in updated_rows:
                cp_id = row['cp_id']
                new_time = row['time_played']
                expected_time = current_times[cp_id] + adjustment_ticks

                if new_time != expected_time:
                    print(f"Error: Post-update verification failed for run {run_id}, "
                          f"cp {cp_id}: expected {expected_time}, got {new_time}")
                    self.connection.rollback()
                    cursor.close()
                    return False

            # Commit the transaction
            self.connection.commit()
            cursor.close()
            return True

        except Error as e:
            print(f"Error updating run {run_id}: {e}")
            self.connection.rollback()
            cursor.close()
            return False

    def save_to_csv(self, filename: str, runs_data: List[Dict]):
        """
        Save updated run data to CSV file.

        Args:
            filename: Output CSV filename
            runs_data: List of run dictionaries with update information
        """
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = [
                'run_id', 'player_id', 'player_name', 'mapid', 'map_name', 'fps',
                'from_cp_id', 'to_cp_id',
                'old_time_played', 'old_time_formatted', 'new_time_played',
                'new_time_formatted', 'adjustment_seconds'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for run in runs_data:
                writer.writerow({
                    'run_id': run['run_id'],
                    'player_id': run['player_id'],
                    'player_name': run['playername'],
                    'mapid': run['mapid'],
                    'map_name': run['map_name'],
                    'fps': run['fps'],
                    'from_cp_id': run['from_cp_id'],
                    'to_cp_id': run['to_cp_id'],
                    'old_time_played': run['old_time_played'],
                    'old_time_formatted': run['old_time_formatted'],
                    'new_time_played': run['new_time_played'],
                    'new_time_formatted': run['new_time_formatted'],
                    'adjustment_seconds': f"{run['adjustment_seconds']:.2f}"
                })

        print(f"\n✓ Data exported to: {filename}")

    def print_summary(self, runs_data: List[Dict]):
        """
        Print summary of affected players grouped by player and fps.

        Args:
            runs_data: List of run dictionaries
        """
        # Group by player_id and fps
        player_stats = defaultdict(lambda: defaultdict(int))
        player_info = {}

        for run in runs_data:
            player_id = run['player_id']
            playername = run['playername']
            fps = run['fps']

            player_info[player_id] = playername
            player_stats[player_id][fps] += 1

        print("\n" + "=" * 80)
        print("AFFECTED PLAYERS SUMMARY")
        print("=" * 80)

        # Sort by player_id
        for player_id in sorted(player_stats.keys()):
            playername = player_info[player_id]
            fps_data = player_stats[player_id]

            # Build fps string
            fps_parts = []
            for fps in sorted(fps_data.keys()):
                count = fps_data[fps]
                fps_parts.append(f"fps:{fps}, runs:{count}")

            fps_str = ", ".join(fps_parts)
            print(f"{playername}(id:{player_id}, {fps_str})")

    def revert_from_csv(self, filename: str) -> bool:
        """
        Revert changes from a CSV file.

        Args:
            filename: CSV filename with update history

        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filename, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)

            if not rows:
                print("No data found in CSV file")
                return False

            print(f"\nFound {len(rows)} run(s) to revert")
            print("=" * 80)

            reverted = 0

            for row in rows:
                run_id = int(row['run_id'])
                mapid = int(row['mapid'])
                to_cp_id = int(row['to_cp_id'])
                old_time = int(row['old_time_played'])
                new_time = int(row['new_time_played'])
                adjustment = new_time - old_time

                # Recalculate which checkpoints were affected using the same logic
                following_cps = self.get_following_checkpoints(to_cp_id, mapid)

                if not following_cps:
                    print(f"✗ No checkpoints found for run_id {run_id}")
                    continue

                # Start transaction for this revert
                cursor = self.connection.cursor()
                try:
                    self.connection.start_transaction(isolation_level='READ COMMITTED')

                    # Subtract the adjustment from the exact checkpoints that were modified
                    placeholders = ','.join(['%s'] * len(following_cps))
                    query = f"""
                        UPDATE checkpoint_statistics
                        SET time_played = time_played - %s
                        WHERE run_id = %s
                            AND cp_id IN ({placeholders})
                    """

                    params = [adjustment, run_id] + list(following_cps)
                    cursor.execute(query, params)

                    rows_affected = cursor.rowcount
                    self.connection.commit()
                    cursor.close()

                    reverted += 1
                    print(f"✓ Reverted run_id {run_id} (player: {row['player_name']}, "
                          f"-{adjustment/20:.2f}s, {rows_affected} checkpoints)")

                except Error as e:
                    print(f"✗ Failed to revert run_id {run_id}: {e}")
                    self.connection.rollback()
                    cursor.close()

            print(f"\n✓ Successfully reverted {reverted}/{len(rows)} run(s)")
            return True

        except FileNotFoundError:
            print(f"Error: File '{filename}' not found")
            return False
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return False

    def fix_cheated_runs(self, from_cp_id: int, to_cp_id: int, ref_time: float,
                         dry_run: bool = True):
        """
        Main method to detect and fix cheated runs.

        Args:
            from_cp_id: Starting checkpoint ID
            to_cp_id: Ending checkpoint ID
            ref_time: Reference time in seconds
            dry_run: If True, only report what would be changed without making changes
        """
        # Safety check 2: Validate input parameters
        print("\n[Safety Check] Validating input parameters...")
        try:
            self.validate_input_parameters(from_cp_id, to_cp_id, ref_time)
            print("✓ Input parameters valid")
        except ValidationError as e:
            print(f"✗ Validation error: {e}")
            return 1

        # Safety check 1: Validate checkpoints exist and are related
        print("[Safety Check] Validating checkpoints...")
        try:
            mapid, is_reachable = self.validate_checkpoints(from_cp_id, to_cp_id)
            print(f"✓ Checkpoints valid (map ID: {mapid}, reachable: {is_reachable})")
        except ValidationError as e:
            print(f"✗ Validation error: {e}")
            return 1

        # Safety check 6: Check database state
        if not dry_run:
            print("[Safety Check] Checking database connection and privileges...")
            if not self.check_connection():
                print("✗ Database connection lost")
                return 1
            if not self.check_update_privileges():
                print("Warning: Could not verify UPDATE privileges")
                return 1
            else:
                print("✓ Database connection and privileges OK")

        print(f"\nAnalyzing runs from CP {from_cp_id} to CP {to_cp_id}")
        print(f"Reference time: {ref_time} seconds ({ref_time * 20} ticks)")
        print(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (changes will be committed)'}")
        print("-" * 80)

        # Find cheated runs
        cheated_runs = self.find_cheated_runs(from_cp_id, to_cp_id, ref_time)

        if not cheated_runs:
            print("\nNo cheated runs found!")
            return 1

        print(f"\nFound {len(cheated_runs)} cheated run(s):\n")

        # Prepare data for CSV export
        csv_data = []

        for i, run in enumerate(cheated_runs, 1):
            print(f"{i}. Run ID: {run['run_id']}")
            print(f"   Player: {run['playername']} (ID: {run['player_id']})")
            print(f"   Map: {run['map_name']} (ID: {run['mapid']})")
            print(f"   FPS: {run['fps']}")
            print(f"   Old time: {run['old_time_formatted']}")
            print(f"   Adjustment: +{run['adjustment_seconds']:.2f}s ({run['adjustment_ticks']} ticks)")

            # Get following checkpoints
            following_cps = self.get_following_checkpoints(to_cp_id, run['mapid'])
            print(f"   Checkpoints to update: {len(following_cps)}")

            if not dry_run:
                success = self.fix_cheated_run(
                    run['run_id'],
                    to_cp_id,
                    run['mapid'],
                    run['adjustment_ticks'],
                    following_cps
                )
                if success:
                    new_time_played = run['old_time_played'] + run['adjustment_ticks']
                    new_time_formatted = self.ticks_to_time_format(new_time_played)
                    print(f"   New time: {new_time_formatted}")
                    print(f"   ✓ Successfully updated")

                    # Store data for CSV
                    csv_data.append({
                        **run,
                        'from_cp_id': from_cp_id,
                        'to_cp_id': to_cp_id,
                        'new_time_played': new_time_played,
                        'new_time_formatted': new_time_formatted
                    })
                else:
                    print(f"   ✗ Failed to update")
            else:
                new_time_played = run['old_time_played'] + run['adjustment_ticks']
                new_time_formatted = self.ticks_to_time_format(new_time_played)
                print(f"   New time (would be): {new_time_formatted}")
                print(f"   (DRY RUN - no changes made)")

                # Store data for CSV preview in dry run
                csv_data.append({
                    **run,
                    'from_cp_id': from_cp_id,
                    'to_cp_id': to_cp_id,
                    'new_time_played': new_time_played,
                    'new_time_formatted': new_time_formatted
                })

            print()

        # Export to CSV
        if csv_data:
            if dry_run:
                # Ask if user wants to export preview CSV
                print("\n" + "=" * 80)
                response = input("\nExport preview CSV? (yes/no): ").strip().lower()
                if response == 'yes':
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"cheated_runs_preview_{timestamp}.csv"
                    self.save_to_csv(csv_filename, csv_data)
                    self.print_summary(csv_data)
            else:
                # Always export CSV after live run
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_filename = f"cheated_runs_fixed_{timestamp}.csv"
                self.save_to_csv(csv_filename, csv_data)
                self.print_summary(csv_data)


def main():
    """Main entry point for the script."""
    print("=" * 80)
    print("Cheated Run Fixer")
    print("=" * 80)

    # Uncomment and configure this if you need SSH tunnel:
    DB_CONFIG = {
        'host': '<CHANGE>',
        'database': '<CHANGE>',
        'user': '<CHANGE>',
        'password': '<CHANGE>',
        'ssh_config': {
            'ssh_host': '<CHANGE>',  # SSH server hostname/IP
            'ssh_port': '<CHANGE>',  # SSH port
            'ssh_user': '<CHANGE>',  # SSH username
            'ssh_key_file': '<CHANGE>',  # Path to SSH private key
            'local_bind_port': '<CHANGE>',  # Optional: local port (auto-assigned if not specified)
            'remote_bind_address': '<CHANGE>',  # MySQL host on remote (usually localhost)
            'remote_bind_port': '<CHANGE>'  # MySQL port on remote server
        }
    }

    # Check for revert mode
    if len(sys.argv) >= 2 and sys.argv[1] == '--revert':
        if len(sys.argv) != 3:
            print("Usage for revert: python fix_cheated_runs.py --revert <csv_file>")
            sys.exit(1)

        csv_file = sys.argv[2]
        fixer = CheatRunFixer(**DB_CONFIG)

        if not fixer.connect():
            sys.exit(1)

        try:
            response = input(f"\nAre you sure you want to revert changes from '{csv_file}'? (yes/no): ").strip().lower()
            if response == 'yes':
                fixer.revert_from_csv(csv_file)
            else:
                print("Revert cancelled.")
        finally:
            fixer.close()

        sys.exit(0)

    # Get input parameters
    if len(sys.argv) == 4:
        try:
            from_cp_id = int(sys.argv[1])
            to_cp_id = int(sys.argv[2])
            ref_time = float(sys.argv[3])
        except ValueError:
            print("Error: Invalid input parameters")
            print("Usage: python fix_cheated_runs.py <from_cp_id> <to_cp_id> <ref_time_seconds>")
            print("   or: python fix_cheated_runs.py --revert <csv_file>")
            sys.exit(1)
    else:
        # Interactive mode
        print("\nEnter parameters:")
        try:
            from_cp_id = int(input("From checkpoint ID: "))
            to_cp_id = int(input("To checkpoint ID: "))
            ref_time = float(input("Reference time (seconds): "))
        except ValueError:
            print("Error: Invalid input")
            sys.exit(1)

    # Initialize fixer
    fixer = CheatRunFixer(**DB_CONFIG)

    if not fixer.connect():
        sys.exit(1)

    try:
        # First run in dry-run mode
        print("\n" + "=" * 80)
        print("STEP 1: Dry Run (Analysis Only)")
        print("=" * 80)
        err = fixer.fix_cheated_runs(from_cp_id, to_cp_id, ref_time, dry_run=True)
        if err:
            print("\nNo changes applied. Exiting.")
            return


        # Ask for confirmation
        print("\n" + "=" * 80)
        response = input("\nDo you want to apply these changes? (yes/no): ").strip().lower()

        if response == 'yes':
            print("\n" + "=" * 80)
            print("STEP 2: Applying Changes")
            print("=" * 80)
            err = fixer.fix_cheated_runs(from_cp_id, to_cp_id, ref_time, dry_run=False)
            if err:
                print("\nNo changes applied. Exiting.")
                return
            print("\n✓ All changes applied successfully!")
        else:
            print("\nChanges not applied. Exiting.")

    finally:
        fixer.close()


if __name__ == "__main__":
    main()
