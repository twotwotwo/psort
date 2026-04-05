# psort: parallel sorting in Go

This library, built around stdlib sort, provides parallel sorting and
(optional) abbreviated-key sorts for strings and byte slices.  It's a
potential upgrade path from `twotwotwo/sorts` taking advantage of generics
and improvements in Go's stdlib sorts in the time since the older package
was published.

`psort` partitions your data into a few times more pieces than you have
CPUs, then sorts those pieces in parallel.  The parallel mode only triggers
for slices over 8192 items and helps most on bigger datasets (and with more
CPUs, of course).  For strings and byte slices it uses abbreviated-key
sorting, which helps if the first eight bytes are enough to distinguish a
lot of your values.

There's also `SortKey` so you can pass in a key function to e.g. sort
structs by a field, and sorts for byte slices (`SortBytes` and
`SortKeyBytes`) that use the abbreviated-key trick.

On an eight-core Zen 3 laptop CPU with Go 1.24, sorting 10M random 20-byte
strings took 3.5s with the stdlib, 0.95s with twotwotwo/sorts, and 0.29s
with psort. Sorting 10M ints took 0.72s with the stdlib, 0.24s with
twotwotwo/sorts, and 0.16s with psort.

In ideal conditions, abbreviated-key sorting more than doubles sorting speed.
The current implementation uses 8-10 bytes of temp space per string sorted,
with a sync.Pool that can help if you're doing many sorts.
It won't help when prefixes are mostly the same, like URLs all starting
"https://"; it detects extreme cases of that and disables abbreviated
keys. You can explicitly disable abbreviated keys by using `SortFunc` (as
in `SortFunc(myStrings, strings.Compare)`) or, for convenience,
`SortInPlace(myStrings)`.

And!  Though I put real effort into trying different approaches and getting
the code simple and well-tested, know the code is LLM-generated.

I'm interested in [hearing](https://bsky.app/profile/twotwotwo.bsky.social)
if you use this or if there are specific things you want.

## License

It would be cool if this were uncopyrightable, but practically speaking, no
one is going to rely on that.

So, you can use this under the terms of the BSD license.
