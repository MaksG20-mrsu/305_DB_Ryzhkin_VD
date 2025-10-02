import csv
import os

DATASET_PATH = 'dataset'
OUTPUT_SQL_FILE = 'db_init.sql'


def generate_sql_script():

    with open(OUTPUT_SQL_FILE, 'w', encoding='utf-8') as f:

        f.write("DROP TABLE IF EXISTS movies;\n")
        f.write("DROP TABLE IF EXISTS ratings;\n")
        f.write("DROP TABLE IF EXISTS tags;\n")
        f.write("DROP TABLE IF EXISTS users;\n")
        f.write("\n")

        f.write("""
CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    year INTEGER,
    genres TEXT
);
""")
        f.write("""
CREATE TABLE ratings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER,
    rating REAL,
    timestamp INTEGER
);
""")
        f.write("""
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER,
    tag TEXT,
    timestamp INTEGER
);
""")
        f.write("""
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    gender TEXT,
    register_date TEXT,
    occupation TEXT
);
""")
        f.write("\n")

        print("Processing movies.csv...")
        if os.path.exists(os.path.join(DATASET_PATH, 'movies.csv')):
            with open(os.path.join(DATASET_PATH, 'movies.csv'), 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)
                for row in reader:
                    movie_id, title, genres = row
                    title = title.replace("'", "''")
                    year = title[-5:-1] if title[-5:-1].isdigit() else "NULL"
                    title = title[:-7]
                    f.write(
                        f"INSERT INTO movies (id, title, year, genres) VALUES ({movie_id}, '{title}', {year}, '{genres}');\n")

        print("Processing ratings.csv...")
        if os.path.exists(os.path.join(DATASET_PATH, 'ratings.csv')):
            with open(os.path.join(DATASET_PATH, 'ratings.csv'), 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)
                for row in reader:
                    user_id, movie_id, rating, timestamp = row
                    f.write(
                        f"INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES ({user_id}, {movie_id}, {rating}, {timestamp});\n")

        print("Processing tags.csv...")
        if os.path.exists(os.path.join(DATASET_PATH, 'tags.csv')):
            with open(os.path.join(DATASET_PATH, 'tags.csv'), 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)
                for row in reader:
                    user_id, movie_id, tag, timestamp = row
                    tag = tag.replace("'", "''")
                    f.write(
                        f"INSERT INTO tags (user_id, movie_id, tag, timestamp) VALUES ({user_id}, {movie_id}, '{tag}', {timestamp});\n")

        print("Processing users.dat...")
        if os.path.exists(os.path.join(DATASET_PATH, 'users.txt')):
            with open(os.path.join(DATASET_PATH, 'users.txt'), 'r', encoding='utf-8') as user_file:
                for line in user_file:
                    parts = line.strip().split('|')
                    if len(parts) == 6:
                        user_id, name, email, gender, register_date, occupation = parts
                        name = name.replace("'", "''")
                        f.write(
                            f"INSERT INTO users (id, name, email, gender, register_date, occupation) "
                            f"VALUES ({user_id}, '{name}', '{email}', '{gender}', '{register_date}', '{occupation}');\n"
                        )

    print(f"SQL script '{OUTPUT_SQL_FILE}' has been generated successfully.")


if __name__ == "__main__":
    generate_sql_script()