Various things that we might want to find again:

* [A nice breakdown on storage (some years ago) from Google](http://static.googleusercontent.com/media/research.google.com/en//university/relations/facultysummit2010/storage_architecture_and_challenges.pdf)

* [Article on Colossus](http://highscalability.com/blog/2010/9/11/googles-colossus-makes-search-real-time-by-dumping-mapreduce.html)

* [Another article on Colossus](http://www.theregister.co.uk/2009/08/12/google_file_system_part_deux/) TL;DR: Blocksize of 1MB is reasonable. Aiming for 8K is overkill.

* [GFS: Evolution on Fast-forward](http://queue.acm.org/detail.cfm?id=1594206)

* [Roaring Bitmaps](http://arxiv.org/abs/1402.6407) have a nice Go implementation and can be serialized into etcd.

* [Mentions of Colossus](https://github.com/cockroachdb/cockroach/issues/243#issuecomment-91575792) at Cockroach

* [QFS Paper](https://drive.google.com/file/d/0Bz6uqmjs5anRVU9ROThpd2hVd2s/view?usp=sharing)

* [QFS Github](https://github.com/quantcast/qfs/wiki)

Erasure encoding

* [Erasure encoding on HDFS](https://drive.google.com/file/d/0B9_MJMolBE71cWMxeU1zc25WQjg/view?usp=sharing)
