# Sorted merge join

Sorted merge join is an experimental execution strategy for partitioned
equi-joins. It sorts both inputs by the join keys, merges the ordered streams,
and replays matching groups. Enable it with the `prefer_merge_join` session
property, described in [](/admin/properties-optimizer). The default is `OFF`.

## Execution and memory

Rows with equal keys are sent to compatible tasks. Each task uses one merge
driver, regardless of `task_concurrency`. This provides distributed execution,
but does not parallelize a single hot key across drivers or workers.

The bounded external sorter is enabled for sorts introduced by merge join
planning. Ordinary `ORDER BY` operators retain their existing spill-run merge.

With `spill_enabled=true` and configured spill paths, the build buffer and sorts
can use local disk. Large matching groups use replayable spill pages, bounded
probe chunks, and a disk bitmap when an outer join needs more match state than
fits in memory. An on-disk page-offset index permits exact replay without
loading an index proportional to the build size into memory.

Task reservations include the output builder, retained replay pages, probe
chunks, match state, and spill reservations. A reservation is not a cap on the
entire JVM heap. Input pages owned by upstream operators, serializer working
buffers, transient allocations, Java object overhead, and pages already
transferred downstream also consume memory. A single large row or page can
still exceed the available allowance. Concurrent joins and sorts share the
query allowance.

Many-to-many groups can require large output and repeated residual-filter
evaluation. Spill bounds retained state; it does not eliminate that work or
guarantee faster execution than hash join. See [](/admin/spill) for spill space,
compression, and encryption settings.

When spill encryption is enabled, it also protects the outer join match bitmap.
The bitmap uses independently encrypted blocks with a temporary key and a new
initialization vector for every write, including updates to an existing block.
Zero blocks are encrypted in bounded initialization steps before matching starts.
Spill reservations include the encrypted block overhead. File sizes and the
page-offset index remain storage metadata.

Late aggregation pushdown retains the selected physical join contract. Hash
partial aggregations are not pushed below a merge join or through an ordered
exchange, since their output does not preserve the required input ordering.

## Iceberg sortedness

The Iceberg connector can report verified identity prefixes of the table sort
order with `iceberg.report-sortedness`, or the catalog session property
`report_sortedness`. Every relevant file must identify the current table sort
order. Missing, unknown, or mixed file sort orders disable the hint.

The hint describes file ordering, rather than the order of all files or splits
assigned to a task. Merge-join sorts validate individual pages and construct
an ordered task stream. Multiple sorted files do not allow the engine to remove
all ordering work. Reporting sortedness is independently configurable and is
enabled by default, including when merge join selection is `OFF`.

## Observability

`EXPLAIN` identifies selected joins with `Algorithm: MERGE` and shows the
canonical sorts and ordered exchanges. Final operator information includes
`probeRows`, `buildRows`, `outputRows`, `keyGroups`, and `maxBuildGroupRows`.
Counts from different tasks are summed, except `maxBuildGroupRows`, which uses
the maximum. Spill read counters measure serialized bytes read by the spiller,
rather than physical reads from the storage device.

## Selection

`FORCE` and `PREFER` select every eligible join. They retain hash join for
ineligible plans. `FORCE` does not override execution or type restrictions.

`COST_BASED` compares a local estimate for hash join and merge join after join
order and distribution have been chosen. It requires known input statistics,
spill support, a resource or ordering trigger, and a cost advantage. The
estimate includes sorting, exchanges, build spill, and group replay. It uses
average group sizes and cannot predict arbitrary hot-key frequencies from
distinct-value counts. It is an experimental resource heuristic, rather than
joint enumeration of join order, distribution, and algorithm. Cost values are
relative estimates, not elapsed-time predictions or memory admission limits.

In particular, Iceberg estimates variable-width data sizes from compressed
file sizes. Highly compressible strings can occupy substantially more memory
than these estimates suggest, even after `ANALYZE`. `COST_BASED` can retain hash
join and exceed the memory limit on such input while `FORCE` succeeds. Use
`FORCE` for a validated memory-sensitive workload when these estimates are
unreliable; automatic selection does not guarantee that a query fits in memory.

The general join cost calculators used for `EXPLAIN` totals do not reproduce
this local strategy-selection model. Identify the algorithm from the plan
marker and use runtime operator statistics to assess its resource use.

## Supported queries

The strategy supports `INNER`, `LEFT`, `RIGHT`, and `FULL` joins with at least
one equality criterion. Additional residual predicates are supported. Null
keys do not match, and outer joins retain unmatched rows.

Keys must have orderable scalar types. `ARRAY`, `ROW`, `DOUBLE`, and `REAL`
keys are excluded. Non-equality-only joins and dedicated semi-join operators
are not supported. A query rewritten to an eligible ordinary join can use the
strategy.

The distribution must be `PARTITIONED`, distributed sorting must be enabled,
and `retry_policy` must be `NONE`. Broadcast joins, multiple merge drivers per
task, and fault-tolerant execution with task retries are not supported.
