REGISTER "src/it/resources/sqlite-jdbc-3.8.11.1.jar";
A = LOAD 'data' USING JdbcStorage('jdbc:sqlite:sqldb.sqlite3') AS (col1: int, col2:chararray);
STORE A INTO '$outfile';
