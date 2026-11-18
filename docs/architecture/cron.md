# Cron triggers

Scheduled runs (task 03.08, FR-WF-14). A trigger starts runs of a workflow on a cron schedule:
- **at most one run per fire time**, across crashes, restarts and two schedulers;
- **no fire lost**, unless the missed-fire policy says to skip it.

Code: `internal/cron` (`Parse`, `NextFire`, `Loop`), `service.TriggerService` (the REST CRUD and `StartFire`), and the REST routes `/api/v1/triggers` (`internal/api/rest/triggers.go`). The table is `cron_triggers` (`00E` §2).

## Schedules

- **Format:** a trigger's `schedule` is a standard 5-field cron expression — minute, hour, day of month, month, day of week — in its IANA `timezone` (default `UTC`).
- **Rejected:** seconds fields, descriptors (`@hourly`, `@every 30s`), `TZ=` prefixes, `Local`, and schedules that never fire. Fires are therefore at least a minute apart.
- **Parser:** robfig/cron v3, used as the parser only.

| Example | Fires |
|---|---|
| `*/5 * * * *` | Every 5 minutes |
| `0 9 * * 1-5`, `America/New_York` | 09:00 New York time on weekdays (14:00Z in winter, 13:00Z in summer) |

### Daylight saving time

The schedule is evaluated on wall-clock time, and each wall-clock time fires once (`TestNextFire`).

- **Spring forward.** A time that doesn't exist fires once, at the next valid time: the end of the gap. On 2027-03-14 in New York, 02:00 EST jumps to 03:00 EDT, so `30 2 * * *` fires at 03:00 EDT that day and at 02:30 again the next.
- **Fall back.** A time that occurs twice fires once, on its first occurrence. On 2026-11-01 in New York, 01:00–01:59 happens twice. `30 1 * * *` fires at 01:30 EDT and not at 01:30 EST, and `*/30 * * * *` goes from 01:30 EDT straight to 02:00 EST.

## Firing

`cron.Loop` runs on the scheduler leader every 5 s.

```mermaid
sequenceDiagram
  participant L as cron.Loop (leader)
  participant S as TriggerService / RunService
  participant P as Postgres
  L->>P: tx: AssertEpoch; lock due triggers (FOR UPDATE SKIP LOCKED, ≤ 50)
  L->>S: StartFire(key = cron:trg_1:1794909600)
  S->>P: own tx: INSERT run (unique idempotency_key), or find it (replay)
  Note over L: crash here → the next tick starts the same fire again → replay, no 2nd run
  L->>P: audit cron.fire (system:cron); UPDATE trigger SET next_fire_at = … WHERE next_fire_at = old
  L->>P: commit
```

For each due trigger, with `fire = next_fire_at`:

| How late | What happens |
|---|---|
| At most `FF_CRON_MISSED_GRACE` (60 s) | Start the fire's run, then advance to the schedule's next time after `fire` |
| Later, `missed_policy: catch_up_one`, and at most 24 h old | Start one run for the oldest missed fire (`fire`), then advance to the next time after now |
| Later, `missed_policy: skip` (the default), or a catch-up older than 24 h | Start nothing; audit `cron.skip`; advance to the next time after now |

A missed fire's run gets the original fire time's key, so a catch-up can't collide with a later on-time fire.

### Why a fire never makes two runs

The identity of a fire is `(trigger_id, fire time)`. Its run is started with `Idempotency-Key` `cron:<trigger_id>:<fire time in Unix seconds>`, and `RunService.Start` is idempotent on that key (02.04).

| Situation | Why there's still one run |
|---|---|
| Two loops | `SKIP LOCKED` means two loops never hold the same trigger. A loop that would anyway (after a lock timeout or a failover) gets the existing run back: `TestCronPG_TwoLoopsOneRunPerFire` |
| A crash between starting the run and advancing the trigger | The trigger is still due, and the next tick replays the run instead of creating another: `TestCronPG_CrashBetweenStartAndAdvance`, failpoint `cron/after-start-before-advance` |
| The latest workflow version changed between the two attempts | The key now conflicts, and `StartFire` returns the existing run as replayed |
| A double advance | The advance is conditional on `next_fire_at` still being the handled fire |

The audit row of each fire (`cron.fire` or `cron.skip`, actor `system:cron`, with the fire time, key and run id) is written in the same transaction as the advance. Triggers are not runs, so nothing goes to `workflow_events`; the run itself carries `trigger_id`.

### Failures

| Failure | What happens |
|---|---|
| The workflow, or the pinned version, no longer exists; or the input became invalid | **Permanent:** the fire is audited with outcome `error`, counted `failed`, and the trigger advances. It isn't retried forever |
| Anything else (the database is down) | **Transient:** the trigger stays due and the loop backs off; the next tick retries the same fire, and the idempotency key keeps it single |

Metric: `flowforge_cron_fires_total{outcome}`, where `outcome` is `fired`, `replayed`, `skipped` or `failed`.

## API

| Endpoint | Role |
|---|---|
| `POST /api/v1/triggers` | admin |
| `GET /api/v1/triggers`, `GET /api/v1/triggers/{trigger_id}` | viewer |
| `PATCH /api/v1/triggers/{trigger_id}` | admin |
| `DELETE /api/v1/triggers/{trigger_id}` | admin |

- **Creating:** requires `name`, `workflow_name` and `schedule`. Optional fields are `workflow_version` (omitted means the latest version at each fire), `timezone`, `input` (a JSON object), `missed_policy` and `enabled`.
- **Validation** returns `400 INVALID_ARGUMENT` naming the field. The checks are: the workflow and version exist, the schedule parses, the timezone is known, and the input is an object. A duplicate name is 409.
- **Patching** changes `enabled`, `schedule`, `timezone`, `input` or `missed_policy`:
  - enabling, or changing the schedule or timezone, recomputes `next_fire_at` from now;
  - disabling clears it.
- **Deleting** a trigger leaves its runs' `trigger_id` as it was.

## Limits

- **One fire per trigger per tick:** with a 5 s tick, schedules up to one fire a minute keep up.
- **Up to 50 due triggers per tick;** more wait for the next tick.
- **No index on `next_fire_at`:** the due-trigger query scans `cron_triggers`, which is fine for the tens or hundreds of triggers it holds.
- **Catch-up is capped at 24 h,** because Idempotency-Keys are cleared after 24 h (03.07) and an older fire could then start a duplicate run.
- **A patch racing a fire** can make the loop handle the old `next_fire_at` once more; the fire's key turns that into a replay.
