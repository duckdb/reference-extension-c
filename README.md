# DuckDB Reference extension
This is an (experimental) DuckDB extension that serves two main purposes:
- Providing a reference implementation for the various features that can be extended in DuckDB
- Extensive test coverage of C Extension API

## Validating the C Extension API stability
The DuckDB C Extension API works through a large struct of function pointers that can only grow but never be modified.
Every time new functions are stabilized in the struct, the C Extension API version is bumped to the corresponding DuckDB
version. To guarantee the extension API is not accidentally broken, we aim to use this repository to achieve full test
coverage of the entire C Extension API. However, in the initial phase of developing this extension, this will be done in more of a best
effort way since there are more than 350 functions in the C API already.
