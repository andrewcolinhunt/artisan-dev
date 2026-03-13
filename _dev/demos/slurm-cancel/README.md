# Demo: SLURM Array Job Cancellation

Tests how the pipeline responds when you Ctrl+C during a SLURM array job.

## Run

```bash
~/.pixi/bin/pixi run python _dev/demos/slurm-cancel/demo_slurm_cancel.py
```

## What it does

- **Step 0** generates 8 datasets locally (~1s)
- **Step 1** submits all 8 as a single SLURM array job, each task sleeping 60s

## Testing cancellation

While Step 1 is running:

```bash
# In another terminal, check the array job
squeue -u $USER

# Press Ctrl+C in the demo terminal
# Observe the escalating signal handler behavior:
#   1st Ctrl+C: "cancelling" — sets cancel event
#   2nd Ctrl+C: "restoring default handlers" warning
#   3rd Ctrl+C: force-kills the process

# Optionally, manually cancel the SLURM job to see fast cleanup:
scancel <job_id>
```

## What to observe

- The array job appears as `12345_[0-7]` in `squeue`
- First Ctrl+C sets the cancel event but the current step's flow
  continues collecting results (parallel collection)
- SLURM tasks keep running until manually scancelled (auto-scancel
  is not yet implemented)
- `BrokenProcessPool` handling produces clean error messages
- Second Ctrl+C restores default signal handlers
- Third Ctrl+C force-kills immediately
