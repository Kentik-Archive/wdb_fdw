wdb_fdw
=======

WhiteDB Foreign Data Wrapper for PostgreSQL

##To Build:
    make
    (sudo) make install

##Usage:

    CREATE SERVER <server name> FOREIGN DATA WRAPPER wdb_fdw OPTIONS
    (address '1000', size '1000000');
    (the above options are the defaults)

    CREATE USER MAPPING FOR PUBLIC SERVER <server name>;

    CREATE FOREIGN TABLE <table name> (key TEXT, value TEXT) SERVER <server name>;

Now you can Select, Update, Delete and Insert!
