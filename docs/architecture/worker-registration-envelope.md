# Worker registration HTTP envelope

The Worker registers through `POST /v1/workers/register`. Its canonical Native capability inventory is Base64 encoded in the JSON request. The inventory codec accepts up to 4 MiB of encoded data. The generic Coordinator HTTP limit is 1 MiB, so applying that generic limit to registration rejects a valid inventory before the Worker can join.

Registration has an inclusive 5,242,906-byte transport bound: 1 MiB for its other JSON fields, 4 MiB for the encoded inventory, and 26 bytes for the inventory JSON property and separator. The Worker and Coordinator both check the non-inventory fields against the same 1 MiB bound using their actual Jackson serialization. The Coordinator applies the larger transport bound only to the registration `POST`; heartbeat and other routes retain the generic HTTP limit. Declared and streamed body lengths are both enforced.

The transport limit permits a valid canonical inventory; it does not authorize the model or a query. The inventory codec still validates the canonical v2 descriptor and individual capability contracts, and the registration resource still requires internal service identity before accepting a Worker.
