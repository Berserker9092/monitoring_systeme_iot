import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src" / "producers"))
sys.path.insert(0, str(ROOT / "src" / "processors"))
