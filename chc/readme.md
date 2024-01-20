# CHC Implementation
We implemented the CHC system in Java 8. The system provides cross-flow and per-flow database access/update control.

## Further Improvements
1. The current implementation does not support consistency preservation for the case where a state is updated by multiple users.
2. Offloading the database request handling process.
3. Improving state access process. (每个thread分管一部分states) 
