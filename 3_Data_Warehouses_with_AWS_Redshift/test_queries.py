if __name__ == "__main__":
    import configparser
    import psycopg2
    import os
    import sys

    os.chdir(sys.path[0])

    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    d_tables_expectedRows = {
        "staging_events": 8056,
        "staging_songs": 14896,
        "users": 104,
        "artist": 10025,
        "song": 14896,
        "songplay": 333,
        "time": 333,
    }

    for table, expectedRows in d_tables_expectedRows.items():
        # Check that the number of rows in the table is the same as the expected number of rows
        print(table)
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        conn.commit()
        rows = cur.fetchone()[0]
        print(rows)
        assert rows == expectedRows

        # Print the first 5 rows
        cur.execute(f"SELECT * FROM {table} LIMIT 5;")
        conn.commit()
        for r in cur.fetchall():
            print(r)
        print()
        print("*** ***")
    conn.close()
