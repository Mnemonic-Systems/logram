from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

def _run_git(args: list[str], cwd: str | None = None) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        return (proc.stdout or "").strip()
    except Exception:
        return ""

def get_semantic_version() -> str:
    """
    Return a resilient semantic code version:
    - clean repo: <commit_short>
    - dirty repo: <commit_short>-dirty-<md5_6_of_diff_and_untracked>
    
    Excludes the .logram directory to avoid version drift caused by logs/cache.
    """
    try:
        repo_root_str = _run_git(["rev-parse", "--show-toplevel"])
        if not repo_root_str:
            return "unknown_version"
        
        repo_root = Path(repo_root_str)
        commit = _run_git(["rev-parse", "--short", "HEAD"], cwd=repo_root_str)
        if not commit:
            return "unknown_version"

        # 1. Obtenir le diff en EXCLUANT explicitement .logram
        # La syntaxe ':!.logram' dit à git d'ignorer ce chemin
        diff_text = _run_git(["diff", "HEAD", "--", ".", ":!.logram"], cwd=repo_root_str)
        combined = diff_text.encode("utf-8", errors="replace")

        # 2. Obtenir le status pour les fichiers untracked
        status = _run_git(["status", "--porcelain"], cwd=repo_root_str)
        
        has_real_changes = len(diff_text.strip()) > 0

        # 3. Filtrer les fichiers untracked
        for line in status.splitlines():
            if not line.startswith("?? "):
                # Si c'est un fichier modifié (M) déjà capturé par git diff, 
                # on marque juste qu'il y a des changements
                if not line.startswith("??"):
                    has_real_changes = True
                continue
                
            rel_path = line[3:].strip()
            # --- CRUCIAL : IGNORER .logram ---
            if rel_path.startswith(".logram") or "/.logram" in rel_path:
                continue
            
            has_real_changes = True
            abs_path = repo_root / rel_path
            try:
                content = abs_path.read_bytes()
            except Exception:
                content = b""
            
            combined += b"\n--UNTRACKED--\n"
            combined += rel_path.encode("utf-8", errors="replace")
            combined += b"\n"
            combined += content

        if not has_real_changes:
            return commit

        diff_hash = hashlib.md5(combined).hexdigest()[:6]
        return f"{commit}-dirty-{diff_hash}"
        
    except Exception:
        return "unknown_version"