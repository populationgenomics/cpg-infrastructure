# Groups cleanup

To get Pulumi out of an out-of-sync state, it can be helpful to remove particular
members from Google Groups. Run `main.py` with a regular expression to remove all
matching members from all groups, e.g.

```sh
./main.py "all-datasets.*"
```

This will e.g. remove `all-datasets-test@populationgenomics.org.au` and similar
members from all groups.
