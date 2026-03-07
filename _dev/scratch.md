  - Should logs_root be configurable? Or always derived from delta_root.parent? Given that failure_logs_root is already hardcoded this way, staying
  derived seems fine.

ANSWER: derived is fine


  - Migration for failure_logs/? Existing pipelines have failure_logs/ at the top level. Options: (a) just move it, old logs stay where they are; (b)
  check both locations when reading. I'd lean toward (a) — failure logs are ephemeral debugging aids, not precious data.

  ANSWER just move it.

  - Tool output duplication — The sandbox tool_output.log already exists (temporarily). Writing to logs/tools/ means two writes. Alternatively, change
  the sandbox log_path to point into logs/tools/ directly and symlink or skip the sandbox copy. But sandboxes are in $TMPDIR which might be on a
  different filesystem (fast local SSD vs. shared NFS), so the sandbox copy serves a performance purpose during execution.

QUESTION: are these logs currently captured in the execution record in the delta lake?
I don't think we want to write them here by default

  - Log rotation / cleanup — Should logs/ accumulate forever? For SLURM clusters with thousands of jobs, logs/slurm/ could grow large. A max_log_age or
  clean_logs() utility might be needed eventually, but YAGNI for now.

YAGNI

QUESTION are slurm logs currently capture in the delta lake? since they are worker level not execution unit level it is a bit weird. they would probably be step level.