import sys
import csv
import json
import operator
import boto3
from csv import reader
import snowflake.connector

import settings

# Defining global params
translated_csv = "translated_keywords.csv"
region = 'eu-west-1'
sm = boto3.client('secretsmanager', region)

translate = boto3.client(
    'translate',
    aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
    region_name='eu-west-1'
)


comprehend = boto3.client(
    service_name='comprehend',
    aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
    region_name='eu-west-1')


allowed_languages = {
    "GB": ["en"],
    "US": ["en", "es", "fr"],
    "CA": ["fr", "en"],
    "NL": ["nl", "en"],
    "NZ": ["en"],
    "ZA": ["en"],
    "AT": ["de", "en"],
    "DE": ["de", "en"],
    "IN": ["en"],
    "IE": ["en"],
    "CH": ["de", "fr", "it", "en"],
    "CO": ["es", "en"],
    "FI": ["fi", "en"],
    "JP": ["ja", "en"],
    "ES": ["es", "en"],
    "AR": ["es", "en"],
    "CL": ["es", "en"],
    "EC": ["es", "en"],
    "PE": ["es", "en"],
    "MX": ["es", "en"],
    "BR": ["pt", "en"],
    "PT": ["pt", "en"],
    "IT": ["it", "en"],
    "DK": ["da", "en"],
    "NO": ["no", "en"],
    "SE": ["sv", "en"],
}


def connect_to_db():

    """ Creating the function that will allow us connect to Snowflake """

    connection = snowflake.connector.connect(
        user=settings.SNOWFLAKE_USER,
        role=settings.SNOWFLAKE_ROLE,
        account=settings.SNOWFLAKE_ACCOUNT,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        authenticator=settings.SNOWFLAKE_AUTH,
        database=settings.SNOWFLAKE_DATABASE,
    )
    return connection.cursor()


def load_existing_keywords():

    """ Creating the function that will allow us load in memory the current Snowflake table  """

    sql_extract_translated_keywords = """SELECT DISTINCT
          KEYWORD,
          COUNTRY_CODE,
          DETECTED_SOURCE_LANGUAGE,
          VOLUME

        FROM prd_dwh.sandbox.translated_keywords_master"""

    result = connect_to_db().execute(sql_extract_translated_keywords).fetchall()
    translated_keywords = list(map(operator.itemgetter(0), result))
    return translated_keywords


def search_keywords(csv_reader, translated_keywords):
    # Defining the output file, the output's file headers and the writer we'll use to create the file
    with open(translated_csv, "w", newline='\n') as output_file:
        headers = ["KEYWORD", "DETECTED_SOURCE_LANGUAGE", "COUNTRY_CODE", "TRANSLATED_KEYWORD", "VOLUME"]
        writer = csv.writer(output_file, delimiter=',')
        writer.writerow(headers)
        # Searching for keywords in input file
        count = 1
        for row in csv_reader:
            keyword = row[1]
            country_code = row[14]
            volume = row[5]
            print(f"Keyword: {keyword}")
            print(f"Country Code: {country_code}")
            print(f"Row Count: {count}")

            if keyword not in translated_keywords:
                # Api call to detect language
                comprehend_result = json.loads(
                    json.dumps(comprehend.detect_dominant_language(Text=keyword), sort_keys=True, indent=4))
                detected_language = comprehend_result["Languages"][0]["LanguageCode"]
                print(f"Detected Language: {detected_language}")

                if detected_language not in allowed_languages[country_code]:
                    detected_language = "en"
                    print("Forcing detected language to EN")

                result = translate.translate_text(
                    Text=keyword,
                    SourceLanguageCode=detected_language,
                    TargetLanguageCode="EN"
                )

                print(f'Detected language: {detected_language}')
                print(f'Translated text: {result["TranslatedText"]}')

                if keyword == result["TranslatedText"]:
                    detected_language = "en"
                    print(row)
                    print(f"Source Language: {detected_language}")
                    print(f"Translated Keyword: {keyword}")

                # Using the writer, we are writing the new 3 columns to the file
                new_row = []
                new_row.insert(0, keyword)
                new_row.insert(1, detected_language)
                new_row.insert(2, country_code)
                new_row.insert(3, result["TranslatedText"])
                new_row.insert(4, volume)
                writer.writerow(new_row)
                values = (keyword, detected_language, country_code, result["TranslatedText"], volume)
                translated_keywords.append(values)

                count += 1


def insert_into_db():
    """ Creating the function that will allow us to write into Snowflake"""

    with open(translated_csv, 'r', newline='\n') as input_file:
        csv_reader = reader(input_file)
        next(csv_reader, None)
        db = connect_to_db()

        keywords_list = []
        for row in csv_reader:
            keyword = row[0]
            detected_language = row[1]
            country_code = row[2]
            translated_keyword = row[3]
            volume = row[4]
            print(f"Reading Keyword: {keyword}")
            print(f"Reading Country Code: {country_code}")

            values = (keyword, detected_language, country_code, translated_keyword, volume)
            keywords_list.append(values)

        print(f"Keywords list: {keywords_list}")

        for i in range(0, len(keywords_list), 16000):
            # Writing back into snowflake table:
            keywords_chunk = keywords_list[i:i + 16000]
            sql_insert_keywords = ("insert into prd_dwh.sandbox.translated_keywords_master"
                                   "(KEYWORD, DETECTED_SOURCE_LANGUAGE, COUNTRY_CODE, TRANSLATED_KEYWORD, VOLUME)"
                                   " values (%s, %s, %s, %s, %s)")

            db.executemany(sql_insert_keywords, keywords_chunk)


def main():
    """
        Taking a  keyword found in input_file, use the translator object to translate the keyword
        which will be written in a new file
    """
    args = sys.argv[1:]
    if len(args) < 1:
        print("Missing file name.")
        sys.exit(1)

    # Declaring the input file path
    # encoding='cp1252'
    translated_keywords = load_existing_keywords()
    if sys.argv[1] == "rerun":
        insert_into_db()
    else:
        file_name = sys.argv[1]
        with open(file_name, 'r', newline='\n') as input_file:
            csv_reader = reader(input_file)
            next(csv_reader, None)
            search_keywords(csv_reader, translated_keywords)
            insert_into_db()


if __name__ == '__main__':
    main()
