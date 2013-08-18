import sqlite3 as sql
import csv
import numpy as np


summary_columns = (
    ("range", "REAL"), ("min", "REAL"), ("max", "REAL"),
    ("avg", "REAL"), ("variance", "REAL"), ("Device", "INTEGER"))


def create_table(dbfile, columns, table):
    """
    Create table with given columns

    Parameters:
    * dbfile - database to write to
    * columns - tuple of ("name", "type") for db
    * table - name of table to create
    """
    print("Inserting table \"{}\" into database \"{}\"".format(table, dbfile))
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
    create_table(dbfile, columns, table)
    with sql.connect(dbfile) as con, open(csvfile, "r") as infile:
        cur = con.cursor()

        # Read data from csv
        print("Reading from csv file \"{}\"".format(csvfile))
        csvdata = csv.reader(infile)
        floatdata = None
        for row in csvdata:
            if floatdata is None:
                floatdata = []
                continue
            floatdata += [[float(i) for i in row]]

        # Write data to new table
        print("Write data into table {} in dbfile {}".format(table, dbfile))
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
    * dbfile - database file
    * table - table to index in db
    * key - key to index in table
    """
    indexname = "idx_{}_{}".format(table, key)
    print("Adding index \"{}\" to table \"{}\" on key \"{}\" in db \"{}\"".
          format(indexname, table, key, dbfile))
    with sql.connect(dbfile) as con:
        cur = con.cursor()
        cur.execute("DROP INDEX IF EXISTS {};".format(indexname))
        cur.execute("CREATE INDEX {} ON {}({});".format(indexname, table, key))


def create_table_from_csv(dbfile, datadir="data/"):
    """
    Create raw table from csv.

    Assumes the following files in current directory
    * questions.csv
    * train.csv
    * test.csv

    Parameters:
    * dbfile - database to write to
    * datadir - directory with csv files (must end with /)
    """

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


def create_summary_table(dbfile, table, key):
    """
    Creates a summary table with a certain key

    Parameters:
    * dbfile - database to write to
    * table - table to loop over
    * key - column in table to group by
    """
    create_table(dbfile, summary_columns, table)
    with sql.connect(dbfile) as con:
        cur = con.cursor()
        cur.execute("""INSERT INTO
            train_summary(range,min,max,avg,variance,{0}) SELECT
            MAX(d)-MIN(d) AS range,
            MIN(d) AS min,
            MAX(d) AS max,
            AVG(d) as avg,
            AVG(d*d)-AVG(d)*AVG(d) AS variance,
            {0}
            FROM (
                SELECT t.X*t.X+t.Y*t.Y+t.Z*t.Z AS d, t.0 as {0}
                FROM {1} t) GROUP BY {0};""".format(key, table))


def create_summarys_tables(dbfile):
    """
    Create training summary table in dbfile

    Parameters:
    * dbfile - database to write to
    """
    create_summary_table(dbfile, "train_summary", "Device")
    create_summary_table(dbfile, "test_summary", "SequenceId")


def make_transformed_table(dbfile):
    """
    Make the transformed table

    Parameters:
    * dbfile - database to write to
    """
    keys = [c[0] for c in summary_columns][:-1]
    create_table(dbfile, summary_columns, "train_summary_transformed")
    with sql.connect(dbfile) as con:
        cur = con.cursor()

        # Make the transform command
        cur.execute("SELECT {} FROM train_summary".format(
                    ",".join([k for k in keys])))
        dat = np.array(cur.fetchall())
        mmin = np.min(dat, axis=0)
        mmax = np.max(dat, axis=0)
        transform_command = ", \n".join(
            ["({0}-{1})/{2} AS t_{0}".format(
                x[0], x[1], x[2]) for x in zip(keys, mmin, mmax - mmin)[:-1]])

        # Get transformed training data
        cur.execute(
            "SELECT {}, Device FROM train_summary".format(transform_command))
        print cur.fetchall()
        # Get transformed testing data
        cur.execute(
            "SELECT {}, SequenceId FROM test_summary".format(transform_command))
        print cur.fetchall()

datadir = "data/"
dbfile = datadir + "biometric_data.sqlite"
# create_table_from_csv(dbfile, datadir)  # Don't rerun if db is ok
# create_training_summary_table(dbfile)   # Also not this one
# create_testing_summary_table(dbfile)   # ... yes not this either...

make_transformed_table(dbfile)
