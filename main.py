import json
import operator
import boto3
from csv import reader
import snowflake.connector

import settings


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


# Detect language test
sentence = "drück"
comprehend_result = json.loads(json.dumps(comprehend.detect_dominant_language(Text=sentence), sort_keys=True, indent=4))
detected_lang = comprehend_result["Languages"][0]["LanguageCode"]
print(detected_lang)

# Translate API test
print(translate.translate_text(Text=sentence, SourceLanguageCode=detected_lang, TargetLanguageCode="EN"))


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


def main():
    """
        Taking a  keyword found in input_file, use the translator object to translate the keyword
        which will be written in a new file
    """

    sql_extract_translated_keywords = """SELECT DISTINCT 
      KEYWORD,
      COUNTRY_CODE,
      DETECTED_SOURCE_LANGUAGE,
      VOLUME

    FROM prd_dwh.ods.translated_keywords_master"""

    with snowflake.connector.connect(
            user=settings.SNOWFLAKE_USER,
            role=settings.SNOWFLAKE_ROLE,
            password=settings.SNOWFLAKE_PASSWORD,
            account=settings.SNOWFLAKE_ACCOUNT,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
    ) as conn:
        cs = conn.cursor()
        result = cs.execute(sql_extract_translated_keywords).fetchall()
        translated_keywords = list(map(operator.itemgetter(0), result))
        print(f"translated keywords: {translated_keywords}")
        print(f"Type: {type(translated_keywords)}")

        # Declaring the input file path
        # encoding='cp1252'
        with open("august_stiched.csv", 'r', newline='\n') as input_file:
            csv_reader = reader(input_file)
            next(csv_reader, None)
            count = 1

            # Writing new keywords back to snowflake database for all values not already translated
            headers = [
                "KEYWORD", "COUNTRY_CODE", "DETECTED_SOURCE_LANGUAGE", "TRANSLATED_KEYWORD", "VOLUME", "DATE_TIME_ADDED"
            ]

            # Searching for keywords in input file
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

                    print(f'Translated Keyword: {keyword}')
                    print(f'Detected language: {detected_language}')
                    print(f'Translated text: {result["TranslatedText"]}')

                    if keyword == result["TranslatedText"]:
                        detected_language = "en"
                        print(row)
                        print(f"Source Language: {detected_language}")
                        print(f"Translated Keyword: {keyword}")

                    # Writing back into snowflake table:
                    sql_insert_keywords = ("insert into prd_dwh.ods.translated_keywords_master"
                        "(KEYWORD, COUNTRY_CODE, DETECTED_SOURCE_LANGUAGE, TRANSLATED_KEYWORD, VOLUME)"
                        " values (%s, %s, %s, %s, %s)")

                    values = (keyword, country_code, detected_language, result["TranslatedText"], volume)
                    cs.execute(sql_insert_keywords, values)
                    translated_keywords.append(values)
                    print(f"Inserted into db: {values}")

                    count += 1


if __name__ == '__main__':
    main()
