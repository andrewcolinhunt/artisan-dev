"""Tiny installed application used to verify native source preparation."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from qualification_helper import passthrough


def transform(value: int) -> int:
    """Read a packaged resource and call the separately packaged dependency."""
    factor = json.loads((Path(__file__).parent / "_assets/factor.json").read_text())[
        "factor"
    ]
    return passthrough(value) * int(factor)


def main() -> None:
    """Emit the scientific result and literal activation evidence."""
    result = {
        "value": transform(int(sys.argv[1])),
        "activation": os.environ.get("PROBE_ACTIVATION"),
    }
    print(json.dumps(result))
