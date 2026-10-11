# Coordinator HTTP error responses

Coordinator REST error mapping preserves the status of Jakarta REST exceptions
that carry a response. Method, media, negotiation and missing route
rejections retain 405, 415, 406 and 404, with a typed `USER_ERROR` JSON body.
Protocol headers such as Allow, WWW-Authenticate and Retry-After survive;
replaced entity length and encoding headers are removed. Unknown exceptions
remain logged server errors with status 500.

This follows the Jakarta REST exception response contract:
https://jakarta.ee/specifications/restful-ws/4.0/jakarta-restful-ws-spec-4.0.html#exceptions
