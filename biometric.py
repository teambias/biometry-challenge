import sqlite3 as sql
import csv
import numpy as np


def insert_csv_table(dbfile, csvfile, columns, table):
    """
    Insert csv data into table.

    Will drop prexisting table!

    Parameters:
    * dbfile - database to write to
    * csvfile - csvfile with table to read
    * columns - tuple of ("name", "type") for db
    * table - name of table to create
    """
    print("Inserting table \"{}\" into database \"{}\" from csv file \"{}\"".
          format(table, dbfile, csvfile))
    with sql.connect(dbfile) as con, open(csvfile, "r") as infile:
        cur = con.cursor()

        # Drop old table if it existst
        cur.execute("DROP TABLE IF EXISTS {0};".format(table))

        # Create table
        colswtypes = ",".join(["{} {}".format(c[0], c[1]) for c in columns])
        cur.execute("CREATE TABLE {0} ({1});".format(table, colswtypes))

        # Delete header row
        cur.execute("DELETE FROM {0} WHERE {1} = {1};".format(
                    table, columns[0][0]))

        # Read data from csv
        csvdata = csv.reader(infile)
        floatdata = None
        for row in csvdata:
            if floatdata is None:
                floatdata = []
                continue
            floatdata += [[float(i) for i in row]]

        # Write data to new table
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


def create_training_summary_table(dbfile):
    """
    Create training summary table in dbfile

    Parameters:
    * dbfile - database to write to
    """
    create_cmd = """CREATE TABLE train_summary (
        range REAL,
        min REAL,
        max REAL,
        avg REAL,
        variance REAL,
        Device INTEGER);"""
    insert_cmd = """INSERT INTO
        train_summary(range,min,max,avg,variance,Device) SELECT
        MAX(d)-MIN(d) AS range,
        MIN(d) AS min,
        MAX(d) AS max,
        AVG(d) as avg,
        AVG(d*d)-AVG(d)*AVG(d) AS variance,
        Device
        FROM (
            SELECT t.X*t.X+t.Y*t.Y+t.Z*t.Z AS d, t.Device as Device
            FROM train t) GROUP BY Device;"""
    con = sql.connect(dbfile)
    with con:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS train_summary;")
        cur.execute(create_cmd)
        cur.execute(insert_cmd)


def sqlite3get(dbfile, cmd):
    """
    Run a sqlite command on db and return the result

    Parameters:
    * dbfile - database file
    * cmd - sqlite command

    Returns:
    * resluting tuples OR None if failed
    """
    con = sql.connect(dbfile)
    with con:
        cur = con.cursor()
        cur.execute(cmd)
        dat = cur.fetchall()
        return dat


def extract_csv_summaries(
        in_dbfile="biometric_data.sqlite",
        out_csvfile="biometric_data_summary.csv"):
    """
    Calcualte summary stats for each device and write output as csv.

    Note: slow, call once, then just use the csv
    """
    cmd = """SELECT
        MAX(d)-MIN(d) AS range,
        MIN(d) AS min,
        MAX(d) AS max,
        AVG(d) as avg,
        AVG(d*d)-AVG(d)*AVG(d) AS variance,
        Device
        FROM (
              SELECT t.X*t.X+t.Y*t.Y+t.Z*t.Z AS d, t.Device as Device
              FROM train t) GROUP BY Device"""
    dat = sqlite3get(in_dbfile, cmd)
    csv_writer = csv.writer(open(out_csvfile, "w"))
    csv_writer.writerows(dat)


def make_transformed_variables_command(
        in_csvfile="biometric_data_summary.csv"):
    """
    Make SQL command fragment to transform sql variables so that they are
    normalized to training data.
    """
    dat = csv.reader(open(in_csvfile, "r"))
    keys = ("range", "min", "max", "avg", "variance")
    biodata = []
    for row in dat:
        biodata += [[float(i) for i in row]]
    biodata = np.array(biodata)
    mmin = np.min(biodata, axis=0)
    mmax = np.max(biodata, axis=0)
    cmd = ", \n".join(["({0}-{1})/{2} AS t_{0}".format(
                      x[0], x[1], x[2]) for x in zip(keys, mmin, mmax - mmin)])
    return cmd

datadir = "data/"
dbfile = "biometric_data.sqlite"
# create_table_from_csv(dbfile, datadir)  # Don't rerun if db is ok
# create_training_summary_table(dbfile)   # Also not this one
