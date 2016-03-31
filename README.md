## Limitations
 * Max number of transactions: `usize::max_value()`
 * Only 1 write transaction at a time

## Clean Up
 * Change constructor for Pool to consume a vec
 * Replace &'static str errors with real Error types

## Ideas
 * replace single free index with free lists
