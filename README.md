# asynctree

Asynchronous, immutable, persistent multi-way search tree of key-value pairs.

JavaScript B+-Tree that may be used in node.js or browser environments.

Tree nodes need not be stored in memory. Rather, nodes are read asynchronously as needed from various
customizable backing stores. For example, nodes may be read asynchronously from files or via XMLHttpRequest.
Each node contains several entries, perhaps 100s or 1000s, making trees relatively shallow.

A tree is immutable and fully persistent, meaning after making changes, both the modified
and original tree remain available. Further changes may be made to the original tree, creating new
modifed trees.

Writes to the backing store may be deferred until commit, preventing uncommited changes
from being visible to other transactions.

Each tree is identified by a pointer to its root node. A pointer identifies a node wrt a backing store.
It could be a number or string, such as a filename or URL. It is up to the backing store to interpret 
pointers; trees view them as opaque.

Keys and values are also considered opaque, except with regard to key comparison. A comparison function
determines key ordering. The default comparison uses JavaScript's < and > operators and is effective for
e.g. strings or numbers.

Supported operations:
* Insert new key-value pair
* Update existing key-value pair
* Delete key
* Iterate all key-value pairs or limit to keys in a particular range
* Mark and sweep garbage collection of unreachable nodes
