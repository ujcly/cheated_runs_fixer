# Cheated Run Fixer

A Python script to detect and fix cheated runs in the JH CoD2 MySQL database by adjusting `time_played` values in the `checkpoint_statistics` table.

## Installation

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `mysql-connector-python` - MySQL database driver
- `sshtunnel` - SSH tunnel support for remote database connections
- `pytest` - Testing framework (optional)
- `pytest-cov` - Code coverage for tests (optional)

### 2. Configure Database Credentials

Edit `fix_cheated_runs.py` and update the database configuration.

**SSH Tunnel with Private Key**

If your database requires SSH tunnel access:

```python
DB_CONFIG = {
    'host': 'localhost',  # Keep as localhost
    'database': 'db_name',
    'user': 'your_mysql_username',
    'password': 'your_mysql_password',
    'ssh_config': {
        'ssh_host': 'your-server.com',        # SSH server hostname/IP
        'ssh_port': 22,                       # SSH server port
        'ssh_user': 'your_ssh_username',      # SSH username
        'ssh_key_file': '/path/to/your/private_key',  # Path to SSH private key
        # 'ssh_key_password': 'key_password',  # Optional: only if key is encrypted
        # 'local_bind_port': 'local_port',             # Optional: local port (auto-assigned if not specified)
        'remote_bind_address': 'localhost',   # MySQL host on remote server
        'remote_bind_port': 'remote_bind_port'  # MySQL port on remote server
    }
}
```

**SSH Configuration Parameters:**
- `ssh_host` + `ssh_port`: The remote SSH server (like HeidiSQL's "SSH host + port")
- `local_bind_port`: The local port for the tunnel (like HeidiSQL's "Local port") - optional, auto-assigned if not specified
- `remote_bind_address` + `remote_bind_port`: Where MySQL is running on the remote server (usually `localhost:3306`)

## Usage

### Fix Cheated Runs

**Interactive Mode:**
```bash
python fix_cheated_runs.py
```
The script will prompt you for:
- From checkpoint ID
- To checkpoint ID
- Reference time (in seconds)

**Command Line Mode:**
```bash
python fix_cheated_runs.py <from_cp_id> <to_cp_id> <ref_time_seconds>
```

Example:
```bash
python fix_cheated_runs.py 100 105 15.5
```

### Revert Changes

If you need to undo changes, use the CSV file generated during the fix:

```bash
python fix_cheated_runs.py --revert cheated_runs_fixed_YYYYMMDD_HHMMSS.csv
```

## How It Works

1. **Dry Run (Step 1)**: The script first analyzes the data and shows what would be changed
   - Optionally export a preview CSV (`cheated_runs_preview_YYYYMMDD_HHMMSS.csv`) to review the data
2. **Confirmation**: You'll be asked to confirm before applying changes
3. **Live Run (Step 2)**: If confirmed, changes are applied and automatically exported to CSV
4. **Export**: Updated run details are saved to a timestamped CSV file (`cheated_runs_fixed_YYYYMMDD_HHMMSS.csv`)

### Safety Checks

The script performs comprehensive safety checks:

1. ✓ **Input Validation**: Validates checkpoint IDs and reference time
2. ✓ **Checkpoint Validation**: Verifies checkpoints exist and are on the same map
3. ✓ **Database Connection**: Checks connection and UPDATE privileges
4. ✓ **Transaction Safety**: Uses explicit transactions with rollback on errors
5. ✓ **Post-update Verification**: Confirms changes were applied correctly by comparing expected vs actual values

### Output

**Terminal Output:**
- Detailed information about each affected run
- Summary of affected players grouped by FPS category
- Example: `claay(id:46, fps:125, 250, runs:3)`

**CSV Export:**
Contains:
- run_id, player_id, player_name
- mapid, map_name, fps
- from_cp_id, to_cp_id (checkpoint IDs used for the fix - needed for revert)
- old_time_played, old_time_formatted (MM:SS.SS)
- new_time_played, new_time_formatted (MM:SS.SS)
- adjustment_seconds

Results are sorted by FPS (ascending) and new_time_played.

## Testing

Run the unit tests to verify everything works correctly:

### Using pytest (recommended):
```bash
python -m pytest test_fix_cheated_runs.py -v
```

### With code coverage:
```bash
python -m pytest test_fix_cheated_runs.py -v --cov=fix_cheated_runs --cov-report=html
```

### Using unittest:
```bash
python -m unittest test_fix_cheated_runs.py -v
```

## Example Workflow

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the script
python fix_cheated_runs.py 100 105 15.5

# 3. Review the dry run output
# The script will show:
# - All affected runs
# - Player information
# - Time adjustments needed

# 4. (Optional) Export preview CSV
# Type 'yes' if you want to review the data in CSV format
# File will be named: cheated_runs_preview_YYYYMMDD_HHMMSS.csv

# 5. Confirm to apply changes
# Type 'yes' when prompted to apply changes

# 6. Check the exported CSV file
# File will be named: cheated_runs_fixed_YYYYMMDD_HHMMSS.csv

# 7. If needed, revert changes
python fix_cheated_runs.py --revert cheated_runs_fixed_20260108_143022.csv
```

## Database Schema

The script works with these tables:
- `checkpoint_statistics` - Contains time_played values (updated by this script)
- `checkpoints` - Checkpoint definitions
- `checkpoint_connections` - Checkpoint relationships
- `player_runs` - Run metadata
- `mapids` - Map information

## Requirements

- Python 3.6 or higher
- MySQL 8.0.41 (compatible with MySQL 5.7+)
- Database user with UPDATE privileges on `checkpoint_statistics` table
- For SSH tunnel connections: SSH private key file and access to remote server

## Troubleshooting

### Connection Issues
- Verify database credentials in `fix_cheated_runs.py`
- Check that MySQL server is running
- Ensure firewall allows connection to MySQL port (default 3306)

### Permission Issues
- Ensure database user has UPDATE privileges:
  ```sql
  GRANT UPDATE ON JumpersHeaven_cod2.checkpoint_statistics TO 'your_username'@'localhost';
  FLUSH PRIVILEGES;
  ```

### Validation Errors
- Check that checkpoint IDs exist in the database
- Verify checkpoints are on the same map
- Ensure ref_time is between 0.05 and 3600 seconds

## License

This script is provided as-is for database maintenance purposes.
