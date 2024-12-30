from enum import Enum
from typing import Literal

class RefreshStatus(str, Enum):
    IDLE = 'idle'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'