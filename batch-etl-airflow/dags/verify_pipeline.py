import sys
import os
import subprocess
import tempfile
import shutil

# The verification logic to run inside the virtualenv
VERIFY_SCRIPT = """
import logging
import os
from pyiceberg.catalog import load_catalog
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify():
    logger.info("Loading catalog 'gourmetgram'...")
    # Ensure env vars are passed through or set
    if 'PYICEBERG_CATALOG__GOURMETGRAM__URI' not in os.environ:
         logger.warning("PYICEBERG environment variables might be missing!")

    try:
        catalog = load_catalog("gourmetgram")
    except ValueError as e:
        logger.error(f"Error loading catalog: {e}")
        return

    table_name = "moderation.training_data"
    try:
        logger.info(f"Loading table '{table_name}'...")
        table = catalog.load_table(table_name)
        logger.info(f"Table {table_name} found.")
        
        # Scan and convert to pandas
        scan = table.scan()
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas()
        
        print("\\nSchema:")
        print(table.schema())
        
        print(f"\\nTotal rows: {len(df)}")
        print("\\nSample Data (first 5 rows):")
        print(df.head())
        
    except Exception as e:
        logger.error(f"Error loading validation data: {e}")

if __name__ == "__main__":
    verify()
"""

def main():
    # Dependencies required for verification (same as load_iceberg task)
    requirements = [
        "pyiceberg[s3fs,sql-postgres]==0.8.0",
        "pandas<2.2",
        "pyarrow",
        "sqlalchemy>=2.0",
        "psycopg2-binary"
    ]

    print("Setting up temporary virtualenv for verification...")
    with tempfile.TemporaryDirectory() as venv_dir:
        # 1. Create virtualenv
        subprocess.run([sys.executable, "-m", "virtualenv", venv_dir], check=True)
        
        venv_python = os.path.join(venv_dir, "bin", "python")
        venv_pip = os.path.join(venv_dir, "bin", "pip")
        
        # 2. Install dependencies
        print("Installing dependencies...")
        subprocess.run([venv_pip, "install"] + requirements, check=True)
        
        # 3. Write verification script to file
        verify_script_path = os.path.join(venv_dir, "verify_script.py")
        with open(verify_script_path, "w") as f:
            f.write(VERIFY_SCRIPT)
            
        # 4. Run verification script
        print("\\nRunning verification...")
        env = os.environ.copy()
        subprocess.run([venv_python, verify_script_path], env=env, check=True)

if __name__ == "__main__":
    main()
