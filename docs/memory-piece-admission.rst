Full-piece admission for the memory disk backend
===============================================

The memory disk backend admits a piece to RAM only when the shared pool and
the storage can reserve the whole piece, and it keeps that reservation until
the piece's RAM is freed. Received bytes alone never decide admission.

Admission
---------

Before accepting the first RAM block of a generation, the backend must reserve
its whole piece under the pool mutex. The reservation includes the allocation
of its short final block rounded up to the 16 KiB block size. A piece spanning
files reserves the whole torrent piece, rather than the intersection with the
selected file. Pad blocks are charged too.

Both the shared pool and the optional storage limit must admit the complete
reservation. Unused space smaller than the piece is insufficient. A new piece
that does not fit must start in the file under MEMORY, or be declined under
MEMORY_ONLY. Persist and FILE claims retain their precedence. A started partial
piece must finish in its original place even after the limit is lowered.

Lifetime
--------

A reservation must cover current, transferring and retired generations until
their RAM can be freed. Removing an owner or storage, starting a transfer, or
answering a write does not by itself release a reservation. Pinned readers and
hash jobs retain it. Partial transfer and a completed transfer must retain the
reservation while their dropped blocks are pinned. A failed full transfer
returns the piece to memory with its reservation; a partial one, failed or not,
returns it once its blocks are freed. Clear, forget and successful final
release must return it exactly once. Replacing an unpinned generation may reuse
its reservation only after its old blocks have actually been freed under the
same mutex.

Observation and clients
-----------------------

``memory_storage_pool::reserved_bytes() const`` returns the shared
reservation, including removed storages with pinned generations. It is separate
from ``held_bytes()`` and ``retired_bytes()``, which continue to report physical
received blocks with their existing meanings. The observation must take the
same mutex as admission and must not wait for the network thread.

A client must not read the reservation as permission to hold more RAM: it is
the backend's own accounting of admitted pieces, not a budget for requests.

Verification
------------

Native tests must fill several partial pieces that each began with one block,
verify that new pieces exceeding the full slots went to the file, and compare
all read bytes and hashes. Separate cases cover foreign storages, retained
readers, transfers, shrinking limits, MEMORY_ONLY, unaligned files and a short
last block. Production mutations must make their named tests fail; compilation
failure is not a successful mutation.
