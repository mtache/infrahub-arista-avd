Retry artifact generation only when Infrahub explicitly rejects it before handler execution, with bounded backoff and Retry-After; do not replay unmarked errors or lost responses.
