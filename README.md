# mustela
Mustela

The `direct_writing` branch marks moment in history, when initial experiments with 
direct pages writing is finished. Lessons are learned, and another experimental 
implementation will continue in main branch.

Code in this branch works by copy-on-write pages on first modification, using the same tree for both
modified and not modified pages. This leads to non-optimal layout with compaction for pages
that are modified several times, and very complicated code for updating cursors.

This approach allows the system to slowly write updated pages to disk in background, leading to very fast commit,
with the drawback that some pages will be written several times.  

But main show-stopper of this approach is desire to detect DB against corruption, storing hash of every page in parent.

# testing

One time:

- `brew install python3`
- `python3 -m venv venv`
- `source ./venv/bin/activate`
- `pip install -r requirements.txt`

Each time:

- (with `source ./venv/bin/activate` before)
- `py.test -vv`

# testing todo

- create readers in tests
- create bookmarks in tests
