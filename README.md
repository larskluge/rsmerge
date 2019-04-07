# rsmerge

Writes Redis Stream entries into a temporary hash/set structure to enable merging of small streams.


## Build

```
shards install
shards build --release
bin/rsmerge # prints help
```


## Usage

```
# Collect data to merge into a temp hash/set
rsmerge redis://admin:password1@redis.io:6379/0/stream redis://localhost/merging
rsmerge redis://data-collector/5/some-stream redis://localhost/merging

# Write into final destination stream, sorted in chronological order
rsmerge redis://localhost/merging redis://admin:password1@redis.io:6379/new-stream
```
