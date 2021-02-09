# db2fs ![test](https://github.com/abmamo/db2fs/workflows/test/badge.svg?branch=main)
download database tables to files (currently supports Postgres & MySQL)

## quickstart
create virtualenv
```
    python3 -m venv env && source env/bin/activate && pip3 install --upgrade pip
```
install db2fs
```
    pip3 install db2fs @ https://github.com/abmamo/db2fs/archive/v0.0.1.tar.gz
```
### Table 2 File
```
    ### extract database table into a local file ###
    # import database extractor
    from db2fs import DatabaseExtractor
    # database connection info
    connection_info = {
        "engine": "<database type (pg or mysql)>",
        "host": "<database host>",
        "dbname": "<database name>",
        "user": "<database user>",
        "password": "<database password>",
        "port": "<database port>"
    }
    # init extractor with db info
    db_extractor = FileExtractor(
        connection_info=connection_info,
    )
    # download table
    db_extractor.table2csv(table_name="<name of tbl in db>")
```
### Database 2 Files
```
    ### extract all database tables into a local file ###
    # import database extractor
    from db2fs import DatabaseExtractor
    # database connection info
    connection_info = {
        "engine": "<database type (pg or mysql)>",
        "host": "<database host>",
        "dbname": "<database name>",
        "user": "<database user>",
        "password": "<database password>",
        "port": "<database port>"
    }
    # init extractor with db info
    db_extractor = FileExtractor(
        connection_info=connection_info,
    )
    # download table
    db_extractor.db2csv()
```