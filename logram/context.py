from contextvars import ContextVar
from typing import Optional

# IDs pour suivre où on en est dans l'arbre d'exécution
current_run_id: ContextVar[Optional[str]] = ContextVar("current_run_id", default=None)
current_input_id: ContextVar[Optional[str]] = ContextVar("current_input_id", default=None)
current_step_id: ContextVar[Optional[str]] = ContextVar("current_step_id", default=None)

# Cascade flag: once True in a context, all nested @trace steps run LIVE.
_is_forced_by_flow: ContextVar[bool] = ContextVar("_is_forced_by_flow", default=False)