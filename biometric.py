import sqlite3 as sql
import csv
import numpy as np

# Setup LOGGER
import logging as LOGGER
LOGGER.basicConfig(
    format="%(asctime)s %(levelname)s %(funcName)s:%(lineno)s - %(message)s",
    level=LOGGER.DEBUG)


summary_columns = (
    ("range", "REAL", "MAX(d)-MIN(d)"),
    ("min", "REAL", "MIN(d)"),
    ("max", "REAL", "MAX(d)"),
    ("avg", "REAL", "AVG(d)"),
    ("variance", "REAL", "AVG(d*d)-AVG(d)*AVG(d)"))


def create_table(dbfile, columns, table):
    """
    Create table with given columns

    Parameters:
    * dbfile - database to use
    * columns - tuple of ("name", "type") for db
    * table - name of table to create
    """
    LOGGER.debug("Inserting table \"{}\" into database \"{}\""
                 .format(table, dbfile))
    LOGGER.debug("Columns:" + ", ".join(["|".join(c) for c in columns]))
    with sql.connect(dbfile) as con:
        cur = con.cursor()

        # Drop old table if it existst
        cur.execute("DROP TABLE IF EXISTS {0};".format(table))

        # Create table
        colswtypes = ",".join(["{} {}".format(c[0], c[1]) for c in columns])
        cur.execute("CREATE TABLE {0} ({1});".format(table, colswtypes))

        # Delete header row
        cur.execute("DELETE FROM {0} WHERE {1} = {1};".format(
                    table, columns[0][0]))


def insert_csv_table(dbfile, csvfile, columns, table):
    """
    Insert csv data into table.

    Will drop prexisting table!

    Parameters:
    * csvfile - csvfile with table to read
    * dbfile, columns, table - see create_table
    """
    LOGGER.debug("Inserting csv \"{}\" into database \"{}\""
                 .format(csvfile, dbfile))

    create_table(dbfile, columns, table)
    with sql.connect(dbfile) as con, open(csvfile, "r") as infile:
        cur = con.cursor()

        # Read data from csv
        LOGGER.debug("Reading from csv file \"{}\"".format(csvfile))
        csvdata = csv.reader(infile)
        floatdata = None
        for row in csvdata:
            if floatdata is None:
                floatdata = []
                continue
            floatdata += [[float(i) for i in row]]

        # Write data to new table
        LOGGER.debug("Write data into table {} in dbfile {}"
                     .format(table, dbfile))
        cols = ",".join(["{}".format(c[0]) for c in columns])
        vals = ",".join(["?" for c in columns])
        cur.executemany(
            "INSERT INTO {0} ({1}) VALUES ({2})".format(
                table, cols, vals), floatdata)


def add_index(dbfile, table, key):
    """
    Create an index of key in table.

    Will drop prexisting index!

    Parameters:
    * dbfile - database to use
    * table - table to index in db
    * key - key to index in table
    """
    indexname = "idx_{}_{}".format(table, key)
    LOGGER.debug(
        "Adding index \"{}\" to table \"{}\" on key \"{}\" in db \"{}\""
        .format(indexname, table, key, dbfile))
    with sql.connect(dbfile) as con:
        cur = con.cursor()
        cur.execute("DROP INDEX IF EXISTS {};".format(indexname))
        cur.execute("CREATE INDEX {} ON {}({});".format(indexname, table, key))


def create_tables_from_csv(dbfile, datadir="data/"):
    """
    Create raw table from csv.

    Assumes the following files in current directory
    * questions.csv
    * train.csv
    * test.csv

    Parameters:
    * dbfile - database to use
    * datadir - directory with csv files (must end with /)
    """
    LOGGER.debug("Creating tables in database {}".format(dbfile))

    # Test questions
    questions_cols = (("QuestionId", "INTEGER"), (
        "SequenceId", "INTEGER"), ("QuizDevice", "INTEGER"))
    insert_csv_table(
        dbfile, datadir + "questions.csv", questions_cols, "questions")
    add_index(dbfile, "questions", "SequenceId")
    add_index(dbfile, "questions", "QuizDevice")

    # Test data
    test_cols = (("T", "INTEGER"), ("X", "REAL"), ("Y", "REAL"),
                 ("Z", "REAL"), ("SequenceId", "INTEGER"))
    insert_csv_table(dbfile, datadir + "test.csv", test_cols, "test")
    add_index(dbfile, "test", "SequenceId")

    # Training data
    train_cols = (("T", "INTEGER"), ("X", "REAL"), ("Y", "REAL"),
                  ("Z", "REAL"), ("Device", "INTEGER"))
    insert_csv_table(dbfile, datadir + "train.csv", train_cols, "train")
    add_index(dbfile, "train", "Device")


def create_summary_table(dbfile, source_table, table, columns, key):
    """
    Creates a summary table with a certain key

    Parameters:
    * dbfile - database to use
    * table - table to loop over
    * key - column in table to group by
    * columns - columns to loop over
    """
    LOGGER.debug(
        "Creating summary table \"{}\" by key \"{}\" in database \"{}\""
        .format(table, key, dbfile))
    col_keys = ", ".join(["{}".format(c[0]) for c in columns])
    col_defs = ", ".join(["{} AS {}".format(c[2], c[0]) for c in columns])
    cols = [c for c in columns] + [(key, 'INTEGER')]
    create_table(dbfile, cols, table)
    with sql.connect(dbfile) as con:
        cur = con.cursor()
        cmd = """INSERT INTO {4}({0}, {1}) SELECT {2}, {1} FROM
            (SELECT t.X*t.X+t.Y*t.Y+t.Z*t.Z AS d, t.{1} as {1} FROM {3} t)
            GROUP BY {1};""".format(
            col_keys, key, col_defs, source_table, table)
        cur.execute(cmd)


def create_summary_tables(dbfile):
    """
    Create training summary tables in dbfile

    Parameters:
    * dbfile - database to use
    """
    LOGGER.debug(
        "Creating training summary tables in dbfile {}".format(dbfile))
    create_summary_table(dbfile, "train", "train_summary",
                         summary_columns, "Device")
    create_summary_table(dbfile, "test", "test_summary",
                         summary_columns, "SequenceId")


def make_norm_select(dbfile, keys):
    """
    Make the select command normalise columns

    Parameters:
    * dbfile - database to use
    * keys - keys to select
    """
    key_str = ",".join([k for k in keys])
    LOGGER.debug("Making the select command normalise columns" +
                 " for dbfile {} with keys {}"
                 .format(dbfile, key_str))
    with sql.connect(dbfile) as con:
        cur = con.cursor()
        cur.execute("SELECT {} FROM train_summary".format(key_str))
        dat = np.array(cur.fetchall())
        mmin = np.min(dat, axis=0)
        mmax = np.max(dat, axis=0)
        return ["({0}-{1})/{2} AS t_{0}".format(
                x[0], x[1], x[2]) for x in zip(keys, mmin, mmax - mmin)[:-1]]


def create_norm_table(dbfile, source_table, table, columns, key):
    """
    Create a normalised table
    Parameters:
    * dbfile - database to use
    * source_table - table to normalise*
    * table - destination table
    * columns - columns to normalise
    """
    LOGGER.debug(
        "Creating normalised table \"{}\" ".format(table) +
        "from source table \"{}\" in database \"{}\"".
        format(source_table, dbfile))
    select_cmd = ", ".join([c[2] for c in columns])
    new_columns = [("t_" + i[0], i[1], i[2]) for i in columns]
    new_columns += [(key, 'INTEGER')]
    col_tkeys = ", ".join(["t_" + c[0] for c in columns])
    col_keys = ", ".join([c[0] for c in columns])

    create_table(dbfile, new_columns, table)
    with sql.connect(dbfile) as con:
        cur = con.cursor()
        cmd = "INSERT INTO {0}({1}, {2}) SELECT {3}, {2} FROM {4}".format(
            table, col_tkeys, key, select_cmd, source_table, col_keys)
        LOGGER.debug(cmd)
        cur.execute(cmd)


def make_normed_summaries(dbfile):
    """
    Make the transformed tables

    Parameters:
    * dbfile - database to use
    """
    LOGGER.debug("Making the transformed tables in in dbfile \"{}\""
                 .format(dbfile))
    keys = [c[0] for c in summary_columns]
    norm = make_norm_select(dbfile, keys)
    norm_columns = [(i[0][0], i[0][1], i[1])
                    for i in zip(summary_columns, norm)]

    create_norm_table(dbfile, "train_summary",
                      "train_summary_norm", norm_columns, "Device")
    create_norm_table(dbfile, "test_summary",
                      "test_summary_norm", norm_columns, "SequenceId")


datadir = "data/"
# dbfile = datadir + "biometric_data.sqlite"
dbfile = datadir + "biometric_data_2.sqlite"
# Don't rerun any of these if already done it once...
# create_tables_from_csv(dbfile, datadir)
# create_summary_tables(dbfile)
make_normed_summaries(dbfile)

LOGGER.debug("Done work with dbfile \"{}\"".format(dbfile))
