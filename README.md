# mustela
Mustela

Direct writing pages (see `direct_writing` branch) proved to be non-optimal design decision.

Instead, from now on, read-only trees in file will be used together with a in-memory patch.

During commit, in-memory patch will be merged with latest tree, forming a new tree. This will allow optimal 
page splitting, and calculating each modified page hash or other aggregate data to be included in its parent.

Cursors will be just seeked to new positions by their corresponding key.

If in-memory patch will grow to occupy too much memory, a temporary commit will be performed, but without
storing tree root into file. This will allow larger than RAM transactions, transparent to users.

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
