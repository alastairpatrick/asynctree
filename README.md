# asynctree

Asynchronous, immutable, persistent multi-way search tree of key-value pairs.

JavaScript B+-Tree that may be used in node.js or browser environments.

Tree nodes need not be stored in memory. Rather, nodes are read asynchronously as needed from a custom
backing store. For example, the nodes may be read asynchronously from files or via XMLHttpRequest.
Each node contains many entries, making trees relatively shallow.

A tree is immutable and fully persistent, meaning after changes are made, both the modified
and original tree are available. Further changes may be made to original tree, creating new
modifed trees. Each tree is identified by a pointer to its root node.

Writes to the backing store may be deferred until commit time, preventing uncommited changes
from being visible to other transactions.

A pointer identifies a node wrt a backing store. It could be a number or string, such as a
filename or URL. Interpretation of pointers is a concern of the backing store, which is
provided as configuration when constructing the tree.

Keys and values are considered opaque, except for key comparisons. A comparison function can be confiured,
which determines key ordering. The default comparison uses JavaScript's < and > operators
and is effective for e.g. strings or numbers (but not both at the same time).

Supported operations:
* Insert new key-value pair
* Update existing key-value pair
* Delete key
* Iterate all key-value pairs or limit to keys in a particular range
