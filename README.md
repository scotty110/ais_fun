# AIS
Can DuckDB speedup opperations
(https://duckdb.org/#quickinstall)

## GEO
https://github.com/handstuyennn/geo

### Extension Help
https://github.com/duckdb/extension-template

### Make for version of Duckdb
1. remove `pull` from masker makefile
2. download version of duckdb that conda installed from git (ex. wget https://github.com/duckdb/duckdb/archive/refs/tags/v0.8.1.zip )
3. rename duckdb-*.zip to duckdb
4. Edit CmakeLists to compile with correct version of duckdb: duckdb/CmakeLists.txt ->  set(DUCKDB_VERSION "v0.8.1")
5. Build
6. `cp build/release/extension/geo/geo.duckdb_extension ~/.duckdb/extensions/v0.8.1/linux_amd64/`

## Running
Activate duckdb enviroment: `conda activate duckdb`
For an example use: `python duckdb_geo.py`


### Conda Stuff
`conda remove --all --name duckdb`
