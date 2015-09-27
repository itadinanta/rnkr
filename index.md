---
title: rnkr
---

## Rnkr

### TL;DR gimme the good stuff, NOW!

If you're dying to see for yourself what this is about, follow these links

- [Getting started](quickstart)
- [Project on GitHub](https://github.com/itadinanta/rnkr)

### Synopsis

Rnkr is a *general purpose leaderboard microservice* to be integrated as a back-end component behind
a custom server aggregator (ie your Node.js API)

### Project status

It's early days. Rankr is not yet suitable for production as-is but can be used as a starting
point for a custom deployment if you're happy to contribute. Please do contact me either
via [GitHub Issues](https://github.com/itadinanta/rnkr/issues) or start a private messaging thread by
[any of these means](http://www.itadinanta.net/about) for any inquiries.

### Rationale and goals

I believe that the best way of learning something new is to dive in head first. 
This is an open-source project which I've started mainly to learn the Scala language and toolbox. 

I see programming languages as sophisticated tools to solve problems, I have choosen to solve this one
because

- I am familiar with the problem: I write server code for console games day in, day out
and players love being ranked against each other.
- I believe the problem is not fully solved: I have adopted multiple solutions in the past but I am never
fully satisfied with the solution, but project budgets tends to be tight for more R&D.
- There is potential to build a community for it

I started with a data structure which implements an ordered list of N rows of the form

	(rank, (score, timestamp), value, [attachment])

Where:
- `rank` are unique and sequentially ordered numbers 1 to N
- list is kept sorted by `score` according to a given `Ordering` (ascending/descending)
- `(score, timestamp)` are also a sorted key
- `value` is an unique key
- `attachment` is an arbitrary, optional value
- seeking by either `rank`, `score`, `(score, timestamp)` or `value` is `O(logN)` or `O(1)`
- traversal of the structure with `prev()` and `next() which are `O(1)`
- insertion, update and deletion are `O(logN)` or `O(1)`
	
With the following implementation goals:
- efficient in-memory storage
- efficient persistence
- "reasonable" scalability (support large leaderboards and a large number of leaderboards)
- "reasonable" availability
- loss of nodes with no loss of data
- pluggable backends
- ease of deployment with state-of-the art containerization (eg CoreOS/Docker)

As definition of "large" may vary, I suggest that a leaderboard containing 1-10 million entries can 
be considered large, and that 100 thousands to a million leaderboards is a large number.

According to my experience, games tend to have a few large leaderboards and a large number of small
leaderboards, with a pareto-like distribution.

Given the design goals and constraints, I decided to 

- implement the structure as a *mutable, counted B+tree* in Scala.
- use *Akka actors* and *queues* to arbitrate read/write operations to the data structure
- adopt an *event log/snapshot* system for persistence, treating each leaderboard as a distinct entity
so load can easily be distributed across nodes
- design with *row oriented partitioned storage* in mind, starting with a Cassandra implementation
- design with *shared-nothing* principles in mind
- explore the concept of *hot/cold* leaderboards. "Cold" leaderboards are similar to "frozen" audio tracks
in a DAW: they allow faster and more time/space efficient sequential reads at the cost of transitioning
both ways (hot <-> cold).

You are welcome to submit feature requests, user stories or questions in the form of
[GitHub Issues](https://github.com/itadinanta/rnkr/issues) and submit pull requests.

### Licence

At the time of writing, the project is subject to the
[GPL 2.0 licence](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html). As I am bound by
certain contract obligations with my employer, I reserve the rights to change the licensing terms for
future releases. Please contact me in private .

Design and documentation are subject to [CC BY-SA 4.0](http://creativecommons.org/licenses/by-sa/4.0/)
