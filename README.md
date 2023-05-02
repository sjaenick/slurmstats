# slurmstats

A small utility to collect and aggregate job array statistics for
SLURM-based HPC clusters. By default, sums up CPU usage and wallclock
time and provides their medians.

## Dependencies

Perl

## Usage

```
Usage: ./slurmstats.pl [-a] [-h | -k] -j arrayId
    -a  Display average time instead of median
    -h  Group jobs by host
    -k  Group jobs by kernel version
```

## License

MIT
