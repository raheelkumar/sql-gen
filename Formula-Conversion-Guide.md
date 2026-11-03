# Formula Conversion Guide

### Turning plain-English formulas into the exact format the system understands

---

## 1. What this guide is for

When we add a new calculated line item, we first write the formula in plain English in
`driver_config_raw.csv` — things like "last month's connections plus this month's net adds." Before
the system can actually run that calculation, the formula needs to be rewritten into a very specific,
narrow format in `config_driver.csv`. The computer program that reads this file is strict: it only
understands a handful of patterns, written in an exact way. If a formula isn't written exactly right,
the system will reject it rather than guess what you meant.

The good news: there are only **a few patterns** you'll ever need. This guide shows each one, with
real examples, so anyone on the team can do this translation without needing to know how to code.

---

## 2. The three building blocks

Every formula in `config_driver.csv` is built from just three things:

1. **A line item in square brackets** — `[Consumer - FWA Gross Adds]`
   This always means "the value of that row, for this month." The name must be spelled **exactly** as
   it appears elsewhere in the file — copy and paste it rather than retyping it, to avoid typos.

2. **Everyday math** — `+`, `-`, `*`, `/`, and parentheses `( )`
   This works exactly like a calculator. Two useful safety nets are built in automatically:
   - If a line item has no value for a given month, it's simply treated as **0** (not an error).
   - Dividing by zero gives **0** instead of breaking the calculation.

3. **Functions** — a small, fixed set of named "recipes" for the few calculations that plain math
   can't express cleanly, like averages or "last month's value." These are covered in the cheat sheet
   below.

That's the entire language. If a formula isn't one of these three things, the system won't accept it.

---

## 3. Translation cheat sheet

This is the core of the guide. Find the phrase that sounds like what the raw formula says, and use
the matching format. Every example below is a real formula taken from our own config files.

| If the plain-English formula says... | Write it like this | Real example from our files |
|---|---|---|
| "A minus B" (e.g. gross adds minus disconnects) | `[A] - [B]` | `[Consumer - FWA Gross Adds] - [Consumer - FWA Disconnects]` |
| "the average of this month's X and last month's X" | `ROLLING_AVG([X], 2)` | `ROLLING_AVG([Consumer - FWA Connections], 2)` |
| "last month's balance, plus this month's net adds, plus this month's adjustment" | `ROLL_FORWARD(connections=[X], net_adds=[Y], adj=[Z])` | `ROLL_FORWARD(connections=[Consumer - FWA Connections], net_adds=[Consumer - FWA Net Adds], adj=[Calculated Consumer - FWA Connections Adj])` |
| "A divided by B" (used for rates and percentages, e.g. churn %) | `SAFE_DIVIDE([A], [B])` | `SAFE_DIVIDE([Consumer - FWA Disconnects], [Consumer - FWA Average Connections])` |
| "this month's X vs. X from N months ago" | `LAG([X], N)` | `[Consumer - Phone Connections] - LAG([Consumer - Phone Connections], 1)` |
| "the total of X over the last N months" | `ROLLING_SUM([X], N)` | Same shape as `ROLLING_AVG`, just adds instead of averaging, e.g. `ROLLING_SUM([X], 3)` for a 3-month total |

**Note on plain division:** you don't strictly have to use `SAFE_DIVIDE` — writing `[A] / [B]` also
works and is automatically protected against divide-by-zero. We still use `SAFE_DIVIDE` for rates and
percentages because it makes the formula's intent clearer to read later.

---

## 4. A worked example, start to finish

Here's one real line item, "Consumer - FWA Connections," translated step by step.

**What the raw file says, in plain English:**
> Prior month (Consumer - FWA Connections) + Current Month (Consumer - FWA Net Adds) + Current month
> (Calculated Consumer - FWA Connections Adj)

**What it means in business terms:** this month's connection count = last month's connection count,
plus however many net new connections came in this month, plus any one-off adjustment for this month.

**Which pattern from the cheat sheet matches?** This is the "last month's balance, plus this month's
net adds, plus this month's adjustment" pattern — so it becomes a `ROLL_FORWARD`.

**The finished formula:**
```
ROLL_FORWARD(connections=[Consumer - FWA Connections], net_adds=[Consumer - FWA Net Adds], adj=[Calculated Consumer - FWA Connections Adj])
```

Breaking down each piece:
- `connections=[Consumer - FWA Connections]` — the line item's own name. (This is the one time a
  formula is allowed to reference its own row — it tells the system "this is the balance being rolled
  forward.")
- `net_adds=[Consumer - FWA Net Adds]` — the line item that holds this month's net new connections.
- `adj=[Calculated Consumer - FWA Connections Adj]` — the line item that holds this month's one-off
  adjustment.

---

## 5. Simple rules to keep in mind

- **Spell line item names exactly.** Copy-paste the name from another row rather than retyping it. A
  misspelled name will be rejected (the system will usually suggest the closest matching name it
  knows about).
- **`ROLL_FORWARD` must be the whole formula.** You can't add anything else to it or combine it with
  other math — it always stands alone.
- **Don't nest averages, totals, or "N months ago" lookups inside each other.** For example, you can't
  write "the average of (X from 2 months ago)" directly. If you need something like that, create a
  separate line item for the inner part first, then build the outer calculation from that new line
  item.
- **A formula can't reference its own line item** — except for `ROLL_FORWARD`'s `connections=`, which
  is required to reference itself (see the worked example above).
- **If the system rejects your formula, fix the formula text — don't assume the system is wrong.** The
  checker is intentionally strict so mistakes get caught before they reach a report, rather than
  silently producing a wrong number.

---

## 6. Quick reference

| Function | What it means, in plain English | Exact format |
|---|---|---|
| **SAFE_DIVIDE** | A divided by B; gives 0 instead of an error if B is 0 | `SAFE_DIVIDE([A], [B])` |
| **LAG** | The value of X from N months ago | `LAG([X], N)` |
| **ROLLING_AVG** | The average of X over this month and the N-1 months before it | `ROLLING_AVG([X], N)` |
| **ROLLING_SUM** | The total of X over this month and the N-1 months before it | `ROLLING_SUM([X], N)` |
| **ROLL_FORWARD** | This month's balance = last month's balance + this month's additions + this month's adjustment | `ROLL_FORWARD(connections=[X], net_adds=[Y], adj=[Z])` |

For plain arithmetic ("A minus B," "A plus B times C," etc.), just write it the normal way using
`[Line Item]`, `+`, `-`, `*`, `/`, and `( )` — no function needed.
